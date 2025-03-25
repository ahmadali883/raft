package raft

import (
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

func (rn *RaftNode) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply.Term = rn.currentTerm
	reply.VoteGranted = false
	
	rn.logInfo("Received vote request from node %d for term %d (my term: %d, votedFor: %d)", 
		args.CandidateId, args.Term, rn.currentTerm, rn.votedFor)

	if args.Term < rn.currentTerm {
		rn.logInfo("Rejecting vote request from node %d - lower term (%d < %d)", 
			args.CandidateId, args.Term, rn.currentTerm)
		return
	}

	if args.Term > rn.currentTerm {
		rn.logInfo("Node %d has higher term (%d > %d), becoming follower", 
			args.CandidateId, args.Term, rn.currentTerm)
		rn.becomeFollower(args.Term)
	}

	// If we've already voted for someone else in this term
	if rn.votedFor != -1 && rn.votedFor != args.CandidateId {
		rn.logInfo("Rejecting vote request from node %d - already voted for node %d in term %d", 
			args.CandidateId, rn.votedFor, rn.currentTerm)
		return
	}

	// Check if candidate's log is at least as up-to-date as ours
	lastLogIndex := len(rn.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rn.log[lastLogIndex].Term
	}

	logOk := false
	if args.LastLogTerm > lastLogTerm {
		logOk = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		logOk = true
	}
	
	if !logOk {
		rn.logInfo("Rejecting vote request from node %d - log not up-to-date (their term: %d, index: %d | my term: %d, index: %d)", 
			args.CandidateId, args.LastLogTerm, args.LastLogIndex, lastLogTerm, lastLogIndex)
		return
	}
	
	// Grant vote
	reply.VoteGranted = true
	rn.votedFor = args.CandidateId
	rn.resetElectionTimer()
	
	rn.logInfo("Granted vote to node %d for term %d", args.CandidateId, args.Term)
	
	// Persist state after voting
	state := map[string]interface{}{
		"currentTerm": rn.currentTerm,
		"votedFor":    rn.votedFor,
		"log":         rn.log,
		"kvStore":     rn.kvStore,
	}
	if err := rn.persistStateToFile(state); err != nil {
		rn.logInfo("Error persisting state after vote: %v", err)
	}
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
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rn *RaftNode) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply.Term = rn.currentTerm
	reply.Success = false
	
	isHeartbeat := len(args.Entries) == 0
	if isHeartbeat {
		rn.logInfo("Received heartbeat from leader %d (term %d)", args.LeaderId, args.Term)
	} else {
		rn.logInfo("Received %d log entries from leader %d (term %d)", 
			len(args.Entries), args.LeaderId, args.Term)
	}

	if args.Term < rn.currentTerm {
		rn.logInfo("Rejecting AppendEntries from %d - lower term (%d < %d)", 
			args.LeaderId, args.Term, rn.currentTerm)
		return
	}

	wasLeader := rn.state == Leader
	
	// Valid AppendEntries from current leader, reset election timer
	rn.resetElectionTimer()
	rn.lastHeartbeat = time.Now()

	if args.Term > rn.currentTerm {
		rn.logInfo("Node %d has higher term (%d > %d), becoming follower", 
			args.LeaderId, args.Term, rn.currentTerm)
		rn.becomeFollower(args.Term)
	} else if rn.state == Candidate && args.Term == rn.currentTerm {
		// If we're a candidate but we receive a heartbeat with our term,
		// it means another node won the election, so we should become a follower
		rn.logInfo("Received AppendEntries from elected leader %d with same term, becoming follower", 
			args.LeaderId)
		rn.becomeFollower(args.Term)
	} else if wasLeader && args.Term == rn.currentTerm {
		// Two leaders with same term - should not happen in correct Raft
		rn.logInfo("CRITICAL: Two leaders with same term detected! Becoming follower")
		rn.becomeFollower(args.Term)
	}

	// Check log consistency
	if args.PrevLogIndex >= len(rn.log) {
		rn.logInfo("Rejecting AppendEntries: PrevLogIndex %d >= log length %d", 
			args.PrevLogIndex, len(rn.log))
		reply.ConflictIndex = len(rn.log)
		reply.ConflictTerm = -1
		return
	}

	if args.PrevLogIndex >= 0 && (args.PrevLogIndex >= len(rn.log) || rn.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		// Log inconsistency
		if args.PrevLogIndex >= len(rn.log) {
			rn.logInfo("Rejecting AppendEntries: PrevLogIndex %d is beyond end of log (length %d)", 
				args.PrevLogIndex, len(rn.log))
			reply.ConflictIndex = len(rn.log)
			reply.ConflictTerm = -1
		} else {
			conflictTerm := rn.log[args.PrevLogIndex].Term
			conflictIndex := args.PrevLogIndex
			
			// Find the first index with that term
			for i := args.PrevLogIndex - 1; i >= 0; i-- {
				if rn.log[i].Term != conflictTerm {
					conflictIndex = i + 1
					break
				}
			}
			
			reply.ConflictTerm = conflictTerm
			reply.ConflictIndex = conflictIndex
			
			rn.logInfo("Rejecting AppendEntries: Term mismatch at PrevLogIndex %d (expected %d, got %d, conflictIndex: %d)", 
				args.PrevLogIndex, args.PrevLogTerm, conflictTerm, conflictIndex)
		}
		return
	}

	// Log is consistent with leader up to PrevLogIndex
	// Now handle new entries
	if len(args.Entries) > 0 {
		rn.logInfo("Processing %d new log entries", len(args.Entries))
		
		// Append new entries, removing any conflicting old entries
		newEntries := make([]LogEntry, 0, len(args.Entries))
		for i, entry := range args.Entries {
			logIndex := args.PrevLogIndex + 1 + i
			
			if logIndex < len(rn.log) {
				// Check for conflicts
				if rn.log[logIndex].Term != entry.Term {
					// Conflict found, truncate log here and append new entries
					rn.logInfo("Conflict at index %d (my term: %d, leader term: %d), truncating log", 
						logIndex, rn.log[logIndex].Term, entry.Term)
					rn.log = rn.log[:logIndex]
					newEntries = args.Entries[i:]
					break
				}
				// Otherwise entry already exists and matches
			} else {
				// Past the end of our log, append all remaining entries
				newEntries = args.Entries[i:]
				break
			}
		}
		
		// Append any new entries
		if len(newEntries) > 0 {
			rn.logInfo("Appending %d new entries to log", len(newEntries))
			rn.log = append(rn.log, newEntries...)
		}
	}

	// Update commit index if leader told us to
	if args.LeaderCommit > rn.commitIndex {
		oldCommitIndex := rn.commitIndex
		rn.commitIndex = min(args.LeaderCommit, len(rn.log)-1)
		if rn.commitIndex > oldCommitIndex {
			rn.logInfo("Updated commit index: %d -> %d", oldCommitIndex, rn.commitIndex)
			// Apply newly committed entries
			rn.applyCommittedEntries()
		}
	}

	reply.Success = true
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
