package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type State string

const (
	Follower  State = "follower"
	Candidate State = "candidate"
	Leader    State = "leader"
)

type LogEntry struct {
	Term    int
	Command string // Format: "PUT:key:value" or "APPEND:key:value"
}

type RaftNode struct {
	mu            sync.Mutex
	state         State
	currentTerm   int
	votedFor      int
	log           []LogEntry
	commitIndex   int
	lastApplied   int
	nextIndex     map[int]int
	matchIndex    map[int]int
	kvStore       map[string]string
	peers         []*RaftNode
	id            int
	electionTimer *time.Timer
}

func NewRaftNode(id int, peers []*RaftNode) *RaftNode {
	rn := &RaftNode{
		id:          id,
		peers:       peers,
		state:       Follower,
		currentTerm: 0,
		votedFor:    -1,
		kvStore:     make(map[string]string),
		nextIndex:   make(map[int]int),
		matchIndex:  make(map[int]int),
		log:         []LogEntry{{Term: 0, Command: ""}}, // Initialize with dummy entry
	}
	rn.resetElectionTimer()
	go rn.applyCommittedEntriesLoop() // Start background thread to apply entries
	return rn
}

func (rn *RaftNode) resetElectionTimer() {
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}
	timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	fmt.Printf("Node %d resetting election timer to %v\n", rn.id, timeout)
	rn.electionTimer = time.AfterFunc(timeout, rn.startElection)
}

func (rn *RaftNode) startElection() {
	rn.mu.Lock()
	rn.state = Candidate
	rn.currentTerm++
	rn.votedFor = rn.id
	votes := 1
	lastLogIndex := len(rn.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rn.log[lastLogIndex].Term
	}
	args := RequestVoteArgs{
		Term:         rn.currentTerm,
		CandidateId:  rn.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rn.mu.Unlock()

	fmt.Printf("Node %d starting election for term %d\n", rn.id, rn.currentTerm)

	for _, peer := range rn.peers {
		go func(p *RaftNode) {
			var reply RequestVoteReply
			p.RequestVote(args, &reply)
			if reply.VoteGranted {
				rn.mu.Lock()
				votes++
				if votes > len(rn.peers)/2 && rn.state == Candidate {
					rn.becomeLeader()
				}
				rn.mu.Unlock()
			}
		}(peer)
	}
}

func (rn *RaftNode) becomeLeader() {
	fmt.Printf("Node %d becoming leader for term %d\n", rn.id, rn.currentTerm)
	rn.state = Leader
	for i := range rn.peers {
		rn.nextIndex[i] = len(rn.log)
		rn.matchIndex[i] = 0
	}
	go rn.sendHeartbeats()
}

// GetState returns the current state of the RaftNode
func (rn *RaftNode) GetState() State {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.state
}

// SetPeers sets the peers for the RaftNode
func (rn *RaftNode) SetPeers(peers []*RaftNode) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.peers = peers
}

func (rn *RaftNode) applyCommittedEntriesLoop() {
	for {
		time.Sleep(50 * time.Millisecond)
		rn.mu.Lock()
		fmt.Printf("Node %d applying committed entries from %d to %d\n", rn.id, rn.lastApplied+1, rn.commitIndex)
		for rn.lastApplied < rn.commitIndex {
			rn.lastApplied++
			if rn.lastApplied < len(rn.log) {
				entry := rn.log[rn.lastApplied]
				rn.applyCommand(entry.Command)
			}
		}
		rn.mu.Unlock()
	}
}
