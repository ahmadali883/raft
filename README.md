# raft
raft implementation

## Overview
This project implements the Raft consensus algorithm in Go. It demonstrates leader election, log replication, and a simple key-value store.

## Usage
1. Clone the repository.
2. Run each node (for example, node1, node2, node3) from main.go or client.go.
3. Observe the leader election in console logs.
4. Use Put, Append, or Get calls to interact with the cluster.

## Architecture
- node.go, rpc.go, and store.go cooperate to handle voting, leader election, log replication, and state machine updates.
- Each node runs independently and communicates with peers via AppendEntries and RequestVote RPC calls.
