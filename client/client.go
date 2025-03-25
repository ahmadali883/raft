package main

import (
	"A2/raft"
	"fmt"
	"time"
)

func main() {
	node1 := raft.NewRaftNode(1, nil)
	node2 := raft.NewRaftNode(2, nil)
	node3 := raft.NewRaftNode(3, nil)

	node1.SetPeers([]*raft.RaftNode{node2, node3})
	node2.SetPeers([]*raft.RaftNode{node1, node3})
	node3.SetPeers([]*raft.RaftNode{node1, node2})

	time.Sleep(1 * time.Second)

	leader := node1
	if node1.GetState() != raft.Leader {
		if node2.GetState() == raft.Leader {
			leader = node2
		} else {
			leader = node3
		}
	}

	leader.Put("city", "Berlin")
	fmt.Println("City:", leader.Get("city"))
}
