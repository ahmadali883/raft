package raft

import (
	"fmt"
	"time"
)

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rn *RaftNode) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	fmt.Printf("Node %d received RequestVote from Node %d for term %d\n", rn.id, args.CandidateId, args.Term)

	if args.Term < rn.currentTerm {
		reply.VoteGranted = false
		reply.Term = rn.currentTerm
		return nil
	}

	if args.Term > rn.currentTerm {
		rn.currentTerm = args.Term
		rn.state = Follower
		rn.votedFor = -1
	}

	lastLogTerm := 0
	if len(rn.log) > 0 {
		lastLogTerm = rn.log[len(rn.log)-1].Term
	}

	if (rn.votedFor == -1 || rn.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= len(rn.log)-1)) {
		rn.votedFor = args.CandidateId
		reply.VoteGranted = true
		rn.resetElectionTimer()
	}

	reply.Term = rn.currentTerm
	return nil
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rn *RaftNode) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	fmt.Printf("Node %d received AppendEntries from Node %d (term %d), entries len=%d\n",
		rn.id, args.LeaderId, args.Term, len(args.Entries))

	reply.Term = rn.currentTerm
	reply.Success = false

	if args.Term < rn.currentTerm {
		return nil
	}

	if args.Term > rn.currentTerm {
		rn.currentTerm = args.Term
		rn.state = Follower
		rn.votedFor = -1
	}

	rn.resetElectionTimer()

	// Check if prevLogIndex is valid
	if args.PrevLogIndex >= len(rn.log) {
		return nil
	}

	// Check if term matches at prevLogIndex
	if args.PrevLogIndex >= 0 && rn.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return nil
	}

	// Append entries
	if len(args.Entries) > 0 {
		rn.log = append(rn.log[:args.PrevLogIndex+1], args.Entries...)
	}

	// Update commit index
	if args.LeaderCommit > rn.commitIndex {
		rn.commitIndex = min(args.LeaderCommit, len(rn.log)-1)
	}

	reply.Success = true
	return nil
}

func (rn *RaftNode) sendHeartbeats() {
	for rn.state == Leader {
		rn.mu.Lock()
		currentTerm := rn.currentTerm
		leaderId := rn.id
		prevLogIndex := len(rn.log) - 1
		if prevLogIndex < 0 {
			prevLogIndex = 0
		}
		prevLogTerm := 0
		if len(rn.log) > 0 {
			prevLogTerm = rn.log[prevLogIndex].Term
		}
		commitIndex := rn.commitIndex
		peers := rn.peers
		rn.mu.Unlock()

		fmt.Printf("Leader %d sending heartbeat with commitIndex=%d\n", leaderId, commitIndex)

		for _, peer := range peers {
			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     leaderId,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      []LogEntry{},
				LeaderCommit: commitIndex,
			}
			go func(p *RaftNode) {
				var reply AppendEntriesReply
				p.AppendEntries(args, &reply)
				if reply.Term > currentTerm {
					rn.mu.Lock()
					if reply.Term > rn.currentTerm {
						rn.currentTerm = reply.Term
						rn.state = Follower
						rn.votedFor = -1
					}
					rn.mu.Unlock()
				}
			}(peer)
		}
		time.Sleep(150 * time.Millisecond)
	}
}
