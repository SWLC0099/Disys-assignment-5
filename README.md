# How to run
Start three nodes in three separate terminals
```
go run Node/Node.go -id=A -addr=localhost:50051 -peers=B:localhost:50052,C:localhost:50053 -persist=""
```
```
go run Node/Node.go -id=B -addr=localhost:50052 -peers=A:localhost:50051,C:localhost:50053 -persist=""
```
```
go run Node/Node.go -id=C -addr=localhost:50053 -peers=A:localhost:50051,B:localhost:50052 -persist=""
```
In a fourth terminal, bid:
```
go run Client/Client.go -server=localhost:50052 -op=bid -amount=100
```
Then query the result:
```
go run Client/Client.go -server=localhost:50052 -op=result
```

After this kill the leader ctrl + C in the terminal that is running the leader node
Then bid and query the result again:
```
go run Client/Client.go -server=localhost:50051 -op=bid -amount=200
go run Client/Client.go -server=localhost:50052 -op=result
```