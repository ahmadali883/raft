package raft

import (
	"fmt"
	"strings"
)

func (rn *RaftNode) applyCommittedEntries() {
	for rn.lastApplied < rn.commitIndex {
		rn.lastApplied++
		entry := rn.log[rn.lastApplied]
		rn.applyCommand(entry.Command)
	}
}

func (rn *RaftNode) applyCommand(command string) {
	if command == "" {
		return // Skip empty commands
	}

	fmt.Printf("Node %d applying command: %s\n", rn.id, command)

	parts := strings.Split(command, ":")
	if len(parts) < 3 {
		return // Invalid command format
	}

	switch parts[0] {
	case "PUT":
		rn.kvStore[parts[1]] = parts[2]
	case "APPEND":
		rn.kvStore[parts[1]] += parts[2]
	}
}

func (rn *RaftNode) Put(key, value string) bool {
	fmt.Printf("Node %d received PUT request: %s -> %s\n", rn.id, key, value)
	return rn.appendLog("PUT:" + key + ":" + value)
}

func (rn *RaftNode) Append(key, value string) bool {
	fmt.Printf("Node %d received APPEND request: %s += %s\n", rn.id, key, value)
	return rn.appendLog("APPEND:" + key + ":" + value)
}

func (rn *RaftNode) Get(key string) string {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	val := rn.kvStore[key]
	fmt.Printf("Node %d handling GET request: %s -> %s\n", rn.id, key, val)
	return val
}

func (rn *RaftNode) appendLog(command string) bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if rn.state != Leader {
		return false
	}
	entry := LogEntry{
		Term:    rn.currentTerm,
		Command: command,
	}

	prevLogIndex := len(rn.log) - 1
	if prevLogIndex < 0 {
		prevLogIndex = 0
	}
	prevLogTerm := 0
	if len(rn.log) > 0 {
		prevLogTerm = rn.log[prevLogIndex].Term
	}

	newIndex := len(rn.log)
	rn.log = append(rn.log, entry)

	fmt.Printf("Node %d appending log entry (term %d): %s\n", rn.id, rn.currentTerm, command)

	// Immediately apply to own state machine
	rn.applyCommand(entry.Command)

	successCount := 1 // Count self
	for _, peer := range rn.peers {
		go func(p *RaftNode, idx int) {
			var reply AppendEntriesReply
			p.AppendEntries(AppendEntriesArgs{
				Term:         rn.currentTerm,
				LeaderId:     rn.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      []LogEntry{entry},
				LeaderCommit: rn.commitIndex,
			}, &reply)

			if reply.Success {
				rn.mu.Lock()
				defer rn.mu.Unlock()
				successCount++
				if successCount > (len(rn.peers)+1)/2 && rn.commitIndex < idx {
					rn.commitIndex = idx
				}
			}
		}(peer, newIndex)
	}
	return true
}
