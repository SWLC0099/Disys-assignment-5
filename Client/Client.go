package main

import (
    "context"
    "flag"
    "fmt"
    "log"
    "time"

    pb "github.com/SWLC0099/DISYS-ASSIGNMENT-5/gRPC"
    "google.golang.org/grpc"
)

func main() {
    server := flag.String("server", "localhost:50051", "server addr")
    op := flag.String("op", "result", "op: bid or result")
    amount := flag.Int64("amount", 0, "bid amount")
    flag.Parse()

    conn, err := grpc.Dial(*server, grpc.WithInsecure())
    if err != nil { log.Fatalf("dial: %v", err) }
    client := pb.NewAuctionClient(conn)
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()

    if *op == "bid" {
        resp, err := client.Bid(ctx, &pb.BidRequest{Amount: *amount})
        if err != nil {
            log.Fatalf("Bid error: %v", err)
        }
        fmt.Printf("Bid outcome: %v message=%s\n", resp.Outcome, resp.Message)
    } else {
        resp, err := client.Result(ctx, &pb.ResultRequest{})
        if err != nil { log.Fatalf("Result error: %v", err) }
        fmt.Printf("Result closed=%v highest=%d winner=%s\n", resp.Closed, resp.HighestBid, resp.Winner)
    }
}
