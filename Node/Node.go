package main

import (
    "context"
    "flag"
    "fmt"
    "log"
    "net"
    "os"
    "sync"
    "time"
	"strings"

    pb "github.com/SWLC0099/DISYS-ASSIGNMENT-5/gRPC"
    "google.golang.org/grpc"
)

const (
    HeartbeatInterval = 500 * time.Millisecond
    HeartbeatTimeout  = 1500 * time.Millisecond
)

type LogEntry struct {
    Seq    int64
    Bidder string
    Amount int64
}

type AuctionNode struct {
    pb.UnimplementedAuctionServer

    mu sync.Mutex

    id       string
    addr     string
    peers    map[string]string // id -> addr
    clientConns map[string]pb.AuctionClient

    // state
    log       []LogEntry
    seq       int64
    highestBid int64
    highestBidder string
    closed    bool
    auctionStart time.Time
    auctionDuration time.Duration

    // leader/primary
    leaderID string
    isPrimary bool

    // heartbeat tracking
    lastHeartbeat map[string]time.Time

    persistFile string
}

func NewNode(id, addr string, peers map[string]string, duration time.Duration, persistFile string) *AuctionNode {
    n := &AuctionNode{
        id: id, addr: addr, peers: peers,
        clientConns: make(map[string]pb.AuctionClient),
        lastHeartbeat: make(map[string]time.Time),
        auctionDuration: duration,
        persistFile: persistFile,
    }
    n.auctionStart = time.Now()
    // initial leader: highest id among configured nodes
    highest := id
    for pid := range peers {
        if pid > highest {
            highest = pid
        }
    }
    n.leaderID = highest
    n.isPrimary = (n.leaderID == n.id)
    n.loadPersistedState()
    return n
}

func (n *AuctionNode) loadPersistedState() {
    if n.persistFile == "" {
        return
    }
    f, err := os.Open(n.persistFile)
    if err != nil {
        return
    }
    defer f.Close()
    var seq, hb int64
    var hbdr string
    if _, err := fmt.Fscanf(f, "%d %d %s\n", &seq, &hb, &hbdr); err == nil {
        n.seq = seq
        n.highestBid = hb
        n.highestBidder = hbdr
    }
}

func (n *AuctionNode) persistState() {
    if n.persistFile == "" {
        return
    }
    f, err := os.Create(n.persistFile)
    if err != nil { return }
    defer f.Close()
    fmt.Fprintf(f, "%d %d %s\n", n.seq, n.highestBid, n.highestBidder)
}

// helper: get client for peer
func (n *AuctionNode) getClient(id string) (pb.AuctionClient, error) {
    if c, ok := n.clientConns[id]; ok { return c, nil }
    addr, ok := n.peers[id]
    if !ok { return nil, fmt.Errorf("unknown peer %s", id) }
    conn, err := grpc.Dial(addr, grpc.WithInsecure())
    if err != nil { return nil, err }
    client := pb.NewAuctionClient(conn)
    n.clientConns[id] = client
    return client, nil
}

func (n *AuctionNode) Bid(ctx context.Context, req *pb.BidRequest) (*pb.BidResponse, error) {
    n.mu.Lock()
    leader := n.leaderID
    isPrimary := n.isPrimary
    n.mu.Unlock()

    // If not primary, forward to leader
    if !isPrimary {
        if leader == "" {
            return &pb.BidResponse{Outcome: pb.BidResponse_EXCEPTION, Message: "no leader"}, nil
        }
        client, err := n.getClient(leader)
        if err != nil {
            return &pb.BidResponse{Outcome: pb.BidResponse_EXCEPTION, Message: "cannot contact leader"}, nil
        }
        // forward and return whatever leader responds
        return client.Bid(ctx, req)
    }

    // Primary handles auction-closed check and replication
    n.mu.Lock()
    if n.isAuctionClosedLocked() {
        n.mu.Unlock()
        return &pb.BidResponse{Outcome: pb.BidResponse_FAIL, Message: "auction closed"}, nil
    }
    if req.Amount <= n.highestBid {
        n.mu.Unlock()
        return &pb.BidResponse{Outcome: pb.BidResponse_FAIL, Message: "bid too low"}, nil
    }
    n.seq++
    entry := LogEntry{Seq: n.seq, Bidder: n.id, Amount: req.Amount}
    n.mu.Unlock()

    // synchronous replication to peers: require majority ack
    totalNodes := 1 + len(n.peers)
    needed := totalNodes/2 + 1

    acks := 1 // primary itself
    var wg sync.WaitGroup
    muAck := sync.Mutex{}
    ctxRep, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
    defer cancel()

    for pid := range n.peers {
        wg.Add(1)
        go func(peerID string) {
            defer wg.Done()
            c, err := n.getClient(peerID)
            if err != nil { return }
            repReq := &pb.ReplicateRequest{
                Amount: entry.Amount,
                Bidder: entry.Bidder,
                Seq:    entry.Seq,
            }
            resp, err := c.Replicate(ctxRep, repReq)
            if err == nil && resp.Ok {
                muAck.Lock()
                acks++
                muAck.Unlock()
            }
        }(pid)
    }

    // wait either until wg done or we have majority
    doneCh := make(chan struct{})
    go func() {
        wg.Wait()
        close(doneCh)
    }()

    select {
    case <-doneCh:
    case <-time.After(1200 * time.Millisecond):
    }

    muAck.Lock()
    got := acks
    muAck.Unlock()

    if got < needed {
        return &pb.BidResponse{Outcome: pb.BidResponse_EXCEPTION, Message: "could not replicate to majority"}, nil
    }

    n.mu.Lock()
    n.log = append(n.log, entry)
    n.highestBid = entry.Amount
    n.highestBidder = entry.Bidder
    n.persistState()
    n.mu.Unlock()

    return &pb.BidResponse{Outcome: pb.BidResponse_SUCCESS, Message: "bid accepted"}, nil
}

// Replicate: server-to-server replication endpoint
func (n *AuctionNode) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
    n.mu.Lock()
    defer n.mu.Unlock()
    // only append if seq greater than local seq
    if req.Seq <= n.seq {
        return &pb.ReplicateResponse{Ok: true}, nil
    }
    n.seq = req.Seq
    n.log = append(n.log, LogEntry{Seq: req.Seq, Bidder: req.Bidder, Amount: req.Amount})
    if req.Amount > n.highestBid {
        n.highestBid = req.Amount
        n.highestBidder = req.Bidder
    }
    n.persistState()
    return &pb.ReplicateResponse{Ok: true}, nil
}

func (n *AuctionNode) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
    n.mu.Lock()
    n.lastHeartbeat[req.SenderId] = time.Now()
    // update leader if provided
    if req.LeaderId != "" {
        // only update if different
        if n.leaderID != req.LeaderId {
            log.Printf("[%s] Heartbeat: observed leader change suggestion -> %s (from %s)", n.id, req.LeaderId, req.SenderId)
        }
        n.leaderID = req.LeaderId
        n.isPrimary = (n.leaderID == n.id)
    }
    n.mu.Unlock()
    log.Printf("[%s] Heartbeat: received from %s (leader=%s)", n.id, req.SenderId, req.LeaderId)
    return &pb.HeartbeatResponse{Ok: true}, nil
}


func (n *AuctionNode) Result(ctx context.Context, req *pb.ResultRequest) (*pb.ResultResponse, error) {
    n.mu.Lock()
    leader := n.leaderID
    isPrimary := n.isPrimary
    n.mu.Unlock()

    // If not primary, forward to leader
    if !isPrimary {
        if leader == "" {
            // Leader is unknown
            return &pb.ResultResponse{Closed: false, HighestBid: 0, Winner: "unknown"}, nil
        }
        client, err := n.getClient(leader)
        if err != nil {
            // Leader uncontactable return "exception" 
            return &pb.ResultResponse{Closed: false, HighestBid: 0, Winner: "unknown"}, nil
        }
        return client.Result(ctx, req)
    }

    // Primary returns state
    n.mu.Lock()
    defer n.mu.Unlock()
    closed := n.isAuctionClosedLocked()
    return &pb.ResultResponse{
        Closed:     closed,
        HighestBid: n.highestBid,
        Winner:     n.highestBidder,
    }, nil
}



func (n *AuctionNode) GetState(context.Context, *pb.GetStateRequest) (*pb.GetStateResponse, error) {
    n.mu.Lock()
    defer n.mu.Unlock()
    return &pb.GetStateResponse{HighestBid: n.highestBid, Closed: n.isAuctionClosedLocked(), Seq: n.seq}, nil
}

func (n *AuctionNode) isAuctionClosedLocked() bool {
    if n.closed {
        return true
    }
    if time.Since(n.auctionStart) > n.auctionDuration {
        n.closed = true
        return true
    }
    return false
}

func (n *AuctionNode) startHeartbeatLoop(interval, timeout time.Duration) {
    go func() {
        ticker := time.NewTicker(interval)
        defer ticker.Stop()
        for range ticker.C {
            // send heartbeat to all peers
            for pid := range n.peers {
                client, err := n.getClient(pid)
                if err != nil {
                    // cannot create client -> peer unreachable
                    log.Printf("[%s] heartbeat: cannot connect to peer %s: %v", n.id, pid, err)
                    continue
                }
                ctx, cancel := context.WithTimeout(context.Background(), timeout/4)
                resp, err := client.Heartbeat(ctx, &pb.HeartbeatRequest{LeaderId: n.leaderID, SenderId: n.id})
                cancel()
                if err != nil || resp == nil || !resp.Ok {
                    log.Printf("[%s] heartbeat: failed to %s (err=%v, resp=%v)", n.id, pid, err, resp)
                    continue
                }
                // successful heartbeat to peer -> peer alive 
                n.mu.Lock()
                n.lastHeartbeat[pid] = time.Now()
                n.mu.Unlock()
                log.Printf("[%s] heartbeat: sent ok to %s", n.id, pid)
            }

            // detect timeouts/elect
            n.mu.Lock()
            now := time.Now()
            alive := []string{n.id}
            for pid := range n.peers {
                if t, ok := n.lastHeartbeat[pid]; ok && now.Sub(t) <= timeout {
                    alive = append(alive, pid)
                }
            }

            // print debug of alive 
            log.Printf("[%s] heartbeat: alive=%v leader=%s", n.id, alive, n.leaderID)

            // If current leader is not alive, elect highest id among alive
            leader := n.leaderID
            leaderAlive := false
            if leader == n.id {
                leaderAlive = true
            } else {
                if t, ok := n.lastHeartbeat[leader]; ok && now.Sub(t) <= timeout {
                    leaderAlive = true
                }
            }

            if !leaderAlive {
                highest := n.id
                for _, a := range alive {
                    if a > highest { highest = a }
                }
                if highest != n.leaderID {
                    n.leaderID = highest
                    n.isPrimary = (n.leaderID == n.id)
                    log.Printf("[%s] elected new leader: %s (alive=%v)", n.id, n.leaderID, alive)
                }
            }
            n.mu.Unlock()
        }
    }()
}


// start gRPC server
func (n *AuctionNode) StartServer() error {
    lis, err := net.Listen("tcp", n.addr)
    if err != nil { return err }
    s := grpc.NewServer()
    pb.RegisterAuctionServer(s, n)
    n.startHeartbeatLoop(HeartbeatInterval, HeartbeatTimeout)
    log.Printf("[%s] starting node on %s (leader=%s primary=%v)", n.id, n.addr, n.leaderID, n.isPrimary)
    return s.Serve(lis)
}

func main() {
	hbInterval := flag.Int("hb_interval_ms", 500, "heartbeat interval (ms)")
	hbTimeout := flag.Int("hb_timeout_ms", 1500, "heartbeat timeout (ms)")
    id := flag.String("id", "", "node id")
    addr := flag.String("addr", "", "listen addr")
    peersArg := flag.String("peers", "", "comma separated list id:addr,...")
    dur := flag.Int("duration", 100, "auction duration seconds")
    persist := flag.String("persist", "node_state.txt", "persist file")
    flag.Parse()
    if *id == "" || *addr == "" {
        log.Fatal("id and addr required")
    }
	peers := make(map[string]string)
	if *peersArg != "" {
		pairs := split(*peersArg, ",")
		for _, p := range pairs {
			if p == "" { continue }
			i := strings.Index(p, ":")
			if i <= 0 || i == len(p)-1 {
				continue
			}
			pid := p[:i]
			paddr := p[i+1:]
			peers[pid] = paddr
		}
	}

	node := NewNode(*id, *addr, peers, time.Duration(*dur)*time.Second, *persist)
	go func() {
		// Run heartbeat with configured timings 
		node.startHeartbeatLoop(time.Duration(*hbInterval)*time.Millisecond, time.Duration(*hbTimeout)*time.Millisecond)
	}()
	if err := node.StartServer(); err != nil {
		log.Fatal(err)
	}
}

func split(s, sep string) []string {
    var out []string
    cur := ""
    for i := 0; i < len(s); i++ {
        if string(s[i]) == sep {
            out = append(out, cur)
            cur = ""
        } else {
            cur += string(s[i])
        }
    }
    if cur != "" { out = append(out, cur) }
    return out
}
