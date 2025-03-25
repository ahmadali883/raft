package main

import (
	"A2/raft"
	"time"
)

func main() {
	// Create 3 nodes
	node1 := raft.NewRaftNode(1, nil)
	node2 := raft.NewRaftNode(2, nil)
	node3 := raft.NewRaftNode(3, nil)

	// Set peers
	node1.SetPeers([]*raft.RaftNode{node2, node3})
	node2.SetPeers([]*raft.RaftNode{node1, node3})
	node3.SetPeers([]*raft.RaftNode{node1, node2})

	// Simulate client requests
	time.Sleep(1 * time.Second)
	if node1.GetState() == raft.Leader {
		node1.Put("name", "Alice")
		val := node1.Get("name")
		println("Value for 'name':", val)
	}
}
