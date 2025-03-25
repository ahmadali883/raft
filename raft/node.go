package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
	"encoding/json"
	"os"
	"path/filepath"
	"log"
	"strings"
	"sort"
)

type State string

const (
	Follower  State = "follower"
	Candidate State = "candidate"
	Leader    State = "leader"
)

type LogEntry struct {
	Index     int
	Term      int
	Command   string
	Status    string    // "uncommitted", "committed", "applied"
	Timestamp time.Time
}

type RaftNode struct {
	mu              sync.RWMutex
	state           State
	currentTerm     int
	votedFor        int
	log             []LogEntry
	commitIndex     int
	lastApplied     int
	nextIndex       map[int]int
	matchIndex      map[int]int
	kvStore         map[string]string
	peers           []*RaftNode
	id              int
	electionTimer   *time.Timer
	heartbeatTimer  *time.Timer
	snapshotIndex   int
	lastIncludedTerm int
	persistentDir   string
	lastHeartbeat   time.Time
	failureDetector *time.Timer
	stopCh          chan struct{}
	logger          *log.Logger
	logFile         *os.File
}

func NewRaftNode(id int, peers []*RaftNode, persistentDir string) *RaftNode {
	// Create persistent directory if it doesn't exist
	os.MkdirAll(persistentDir, 0755)
	
	// Create log file
	logFileName := filepath.Join(persistentDir, fmt.Sprintf("node%d.log", id))
	logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("Error opening log file for node %d: %v\n", id, err)
		return nil
	}

	logger := log.New(logFile, fmt.Sprintf("[Node %d] ", id), log.LstdFlags)

	rn := &RaftNode{
		id:              id,
		peers:           peers,
		state:           Follower,
		currentTerm:     0,
		votedFor:        -1,
		kvStore:         make(map[string]string),
		nextIndex:       make(map[int]int),
		matchIndex:      make(map[int]int),
		log:            []LogEntry{{Index: 0, Term: 0, Command: "", Status: "committed", Timestamp: time.Now()}},
		persistentDir:   persistentDir,
		snapshotIndex:   0,
		lastIncludedTerm: 0,
		stopCh:          make(chan struct{}),
		logger:          logger,
		logFile:         logFile,
	}
	
	// Recover state if available
	rn.recoverState()
	
	// Start background goroutines
	go rn.run()
	
	return rn
}

func (rn *RaftNode) logInfo(format string, args ...interface{}) {
	if rn.logger != nil {
		rn.logger.Printf(format, args...)
	}
	
	// Also log important events to terminal for visibility
	msg := fmt.Sprintf(format, args...)
	if isImportantEvent(msg) {
		fmt.Printf("[Node %d] %s\n", rn.id, msg)
	}
}

// isImportantEvent determines if a log message should be shown in terminal
func isImportantEvent(msg string) bool {
	importantPatterns := []string{
		"becoming leader",
		"becoming follower",
		"starting election",
		"received vote",
		"requesting vote",
		"granted vote",
		"denied vote",
		"election timeout",
		"applied log entry",
		"state changed",
		"timer reset",
		"error",
		"Error",
		"failed",
	}
	
	for _, pattern := range importantPatterns {
		if strings.Contains(msg, pattern) {
			return true
		}
	}
	return false
}

func (rn *RaftNode) run() {
	rn.logInfo("Node started")
	
	// Initialize random seed based on node ID for election timer diversity
	rand.Seed(time.Now().UnixNano() + int64(rn.id))
	
	// Initialize the last heartbeat time
	rn.lastHeartbeat = time.Now()
	
	// Start background tasks
	go rn.applyCommittedEntriesLoop()
	go rn.persistStateLoop()
	
	// Start election timer
	rn.resetElectionTimer()
	
	// Start failure detector
	rn.startFailureDetector()
	
	rn.logInfo("Node initialized successfully, waiting as follower for leader election")
	
	// Wait for stop signal
	<-rn.stopCh
	rn.logInfo("Node stopped")
}

func (rn *RaftNode) resetElectionTimer() {
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}
	timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	rn.logInfo("Resetting election timer to %v", timeout)
	rn.electionTimer = time.AfterFunc(timeout, func() {
		rn.mu.RLock()
		if rn.state != Leader {
			rn.mu.RUnlock()
			rn.logInfo("Election timeout triggered, starting election")
			rn.startElection()
		} else {
			rn.mu.RUnlock()
		}
	})
}

func (rn *RaftNode) startElection() {
	rn.mu.Lock()
	rn.state = Candidate
	rn.currentTerm++
	rn.votedFor = rn.id
	currentTerm := rn.currentTerm
	votes := 1
	
	rn.logInfo("Starting election for term %d (voted for self)", currentTerm)
	
	lastLogIndex := len(rn.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rn.log[lastLogIndex].Term
	}
	args := RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  rn.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	// Save current state so we can check it when votes come back
	peers := make([]*RaftNode, len(rn.peers))
	copy(peers, rn.peers)
	rn.mu.Unlock()

	for _, peer := range peers {
		go func(p *RaftNode) {
			rn.logInfo("Requesting vote from peer %d for term %d", p.id, currentTerm)
			var reply RequestVoteReply
			p.RequestVote(args, &reply)
			
			rn.mu.Lock()
			defer rn.mu.Unlock()
			
			// Only count votes for the current election term we started
			if currentTerm != rn.currentTerm {
				rn.logInfo("Ignoring vote from peer %d - terms don't match (current: %d, vote: %d)", 
					p.id, rn.currentTerm, currentTerm)
				return
			}
			
			// Only count votes if we're still a candidate
			if rn.state != Candidate {
				rn.logInfo("Ignoring vote from peer %d - no longer a candidate (state: %s)", 
					p.id, rn.state)
				return
			}
			
			if reply.VoteGranted {
				votes++
				rn.logInfo("Received vote from peer %d for term %d, total votes: %d/%d needed", 
					p.id, currentTerm, votes, (len(rn.peers)/2)+1)
					
				if votes > len(rn.peers)/2 {
					rn.logInfo("Won election with %d votes (threshold: %d)", 
						votes, (len(rn.peers)/2)+1)
					rn.becomeLeader()
				}
			} else {
				rn.logInfo("Vote denied by peer %d for term %d (peer term: %d)", 
					p.id, currentTerm, reply.Term)
					
				if reply.Term > rn.currentTerm {
					rn.logInfo("Peer %d has higher term (%d > %d), becoming follower", 
						p.id, reply.Term, rn.currentTerm)
					rn.becomeFollower(reply.Term)
				}
			}
		}(peer)
	}
}

func (rn *RaftNode) becomeLeader() {
	rn.state = Leader
	rn.logInfo("Becoming leader for term %d", rn.currentTerm)
	
	// Initialize leader state - nextIndex and matchIndex
	rn.nextIndex = make(map[int]int)
	rn.matchIndex = make(map[int]int)
	
	for _, peer := range rn.peers {
		rn.nextIndex[peer.id] = len(rn.log)
		rn.matchIndex[peer.id] = -1
	}
	
	// Persist state after becoming leader
	state := map[string]interface{}{
		"currentTerm": rn.currentTerm,
		"votedFor":    rn.votedFor,
		"log":         rn.log,
		"kvStore":     rn.kvStore,
	}
	if err := rn.persistStateToFile(state); err != nil {
		rn.logInfo("Error persisting state after becoming leader: %v", err)
	}
	
	// Cancel election timer if running
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}
	
	// Start sending heartbeats immediately
	go func() {
		// Send immediate heartbeat
		rn.sendHeartbeat()
		
		// Set up a ticker for periodic heartbeats
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		
		for {
			select {
			case <-ticker.C:
				rn.mu.RLock()
				isLeader := rn.state == Leader
				rn.mu.RUnlock()
				
				if !isLeader {
					rn.logInfo("No longer leader, stopping heartbeat ticker")
					return
				}
				rn.sendHeartbeat()
			case <-rn.stopCh:
				rn.logInfo("Node stopping, ending heartbeat goroutine")
				return
			}
		}
	}()
}

func (rn *RaftNode) becomeFollower(term int) {
	rn.state = Follower
	rn.currentTerm = term
	rn.votedFor = -1
	rn.resetElectionTimer()
	rn.logInfo("Becoming follower for term %d", term)

	// Persist state after becoming follower
	state := map[string]interface{}{
		"currentTerm": rn.currentTerm,
		"votedFor":    rn.votedFor,
		"log":         rn.log,
		"kvStore":     rn.kvStore,
	}
	if err := rn.persistStateToFile(state); err != nil {
		rn.logInfo("Error persisting state after becoming follower: %v", err)
	}
}

// GetState returns the current state of the RaftNode
func (rn *RaftNode) GetState() State {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.state
}

// SetPeers sets the peers for the RaftNode
func (rn *RaftNode) SetPeers(peers []*RaftNode) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.peers = peers
	rn.logInfo("Set peers: %v", getPeerIds(peers))
}

func getPeerIds(peers []*RaftNode) []int {
	ids := make([]int, len(peers))
	for i, p := range peers {
		ids[i] = p.id
	}
	return ids
}

func (rn *RaftNode) applyCommittedEntriesLoop() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-rn.stopCh:
			return
		case <-ticker.C:
			rn.mu.RLock()
			if rn.lastApplied >= rn.commitIndex {
				rn.mu.RUnlock()
				continue
			}
			rn.mu.RUnlock()

			rn.mu.Lock()
			if rn.lastApplied < rn.commitIndex {
				rn.lastApplied++
				if rn.lastApplied < len(rn.log) {
					entry := rn.log[rn.lastApplied]
					rn.mu.Unlock()
					rn.applyCommand(entry.Command)
					rn.mu.Lock()
					entry.Status = "applied"
					rn.logInfo("Applied log entry %d: %s", entry.Index, entry.Command)
				}
			}
			rn.mu.Unlock()
		}
	}
}

func (rn *RaftNode) persistStateLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-rn.stopCh:
			return
		case <-ticker.C:
			rn.mu.RLock()
			state := map[string]interface{}{
				"currentTerm": rn.currentTerm,
				"votedFor":    rn.votedFor,
				"log":         rn.log,
				"kvStore":     rn.kvStore,
			}
			rn.mu.RUnlock()

			if err := rn.persistStateToFile(state); err != nil {
				rn.logInfo("Error persisting state: %v", err)
			}
		}
	}
}

func (rn *RaftNode) persistStateToFile(state map[string]interface{}) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	tempFile := filepath.Join(rn.persistentDir, fmt.Sprintf("node%d.tmp", rn.id))
	finalFile := filepath.Join(rn.persistentDir, fmt.Sprintf("node%d.state", rn.id))

	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return err
	}

	return os.Rename(tempFile, finalFile)
}

func (rn *RaftNode) recoverState() error {
	stateFile := filepath.Join(rn.persistentDir, fmt.Sprintf("node%d.state", rn.id))
	data, err := os.ReadFile(stateFile)
	if err != nil {
		if os.IsNotExist(err) {
			rn.logInfo("No previous state found, starting fresh")
			return nil
		}
		return err
	}
	
	var state map[string]interface{}
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	
	rn.mu.Lock()
	defer rn.mu.Unlock()
	
	// Handle currentTerm
	if ct, ok := state["currentTerm"]; ok {
		switch v := ct.(type) {
		case float64:
			rn.currentTerm = int(v)
		case int:
			rn.currentTerm = v
		}
	}
	
	// Handle votedFor
	if vf, ok := state["votedFor"]; ok {
		switch v := vf.(type) {
		case float64:
			rn.votedFor = int(v)
		case int:
			rn.votedFor = v
		}
	}
	
	// Handle log entries
	if logEntries, ok := state["log"].([]interface{}); ok {
		rn.log = make([]LogEntry, len(logEntries))
		for i, entry := range logEntries {
			entryMap, ok := entry.(map[string]interface{})
			if !ok {
				continue
			}
			
			var logEntry LogEntry
			
			// Handle Index
			if idx, ok := entryMap["Index"]; ok {
				switch v := idx.(type) {
				case float64:
					logEntry.Index = int(v)
				case int:
					logEntry.Index = v
				}
			}
			
			// Handle Term
			if term, ok := entryMap["Term"]; ok {
				switch v := term.(type) {
				case float64:
					logEntry.Term = int(v)
				case int:
					logEntry.Term = v
				}
			}
			
			// Handle Command
			if cmd, ok := entryMap["Command"].(string); ok {
				logEntry.Command = cmd
			}
			
			// Handle Status
			if status, ok := entryMap["Status"].(string); ok {
				logEntry.Status = status
			}
			
			// Handle Timestamp
			if ts, ok := entryMap["Timestamp"]; ok {
				switch v := ts.(type) {
				case float64:
					logEntry.Timestamp = time.Unix(0, int64(v))
				case int64:
					logEntry.Timestamp = time.Unix(0, v)
				case string:
					if t, err := time.Parse(time.RFC3339, v); err == nil {
						logEntry.Timestamp = t
					} else {
						logEntry.Timestamp = time.Now()
					}
				default:
					logEntry.Timestamp = time.Now()
				}
			} else {
				logEntry.Timestamp = time.Now()
			}
			
			rn.log[i] = logEntry
		}
	}
	
	// Handle kvStore
	if kvStore, ok := state["kvStore"].(map[string]interface{}); ok {
		for k, v := range kvStore {
			if strVal, ok := v.(string); ok {
				rn.kvStore[k] = strVal
			}
		}
	}
	
	rn.logInfo("Recovered state: term=%d, votedFor=%d, logLength=%d", 
		rn.currentTerm, rn.votedFor, len(rn.log))
	return nil
}

func (rn *RaftNode) startFailureDetector() {
	// This checks if we haven't received a heartbeat in a while
	checkInterval := 100 * time.Millisecond
	
	go func() {
		ticker := time.NewTicker(checkInterval)
		defer ticker.Stop()
		
		for {
			select {
			case <-rn.stopCh:
				return
			case <-ticker.C:
				rn.mu.RLock()
				timeSinceHeartbeat := time.Since(rn.lastHeartbeat)
				state := rn.state
				rn.mu.RUnlock()
				
				// If we haven't received a heartbeat and we're not the leader
				if timeSinceHeartbeat > 300*time.Millisecond && state != Leader {
					rn.logInfo("No heartbeat received for %v, starting election", timeSinceHeartbeat)
					rn.startElection()
				}
			}
		}
	}()
}

func (rn *RaftNode) Stop() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	
	rn.logInfo("Stopping node")
	
	// Only close the channel if not already closed
	select {
	case <-rn.stopCh:
		// Channel already closed, do nothing
	default:
		close(rn.stopCh)
	}
	
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}
	if rn.heartbeatTimer != nil {
		rn.heartbeatTimer.Stop()
	}
	if rn.failureDetector != nil {
		rn.failureDetector.Stop()
	}
	if rn.logFile != nil {
		rn.logFile.Close()
	}
}

// Add methods for client operations
func (rn *RaftNode) Put(key, value string) bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.state != Leader {
		rn.logInfo("Put operation failed - not leader")
		return false
	}

	command := fmt.Sprintf("PUT %s %s", key, value)
	entry := LogEntry{
		Index:     len(rn.log),
		Term:      rn.currentTerm,
		Command:   command,
		Status:    "uncommitted",
		Timestamp: time.Now(),
	}
	rn.log = append(rn.log, entry)
	
	// Apply the command locally first on the leader
	rn.kvStore[key] = value
	rn.logInfo("Added log entry and applied locally: %s", command)
	
	// Trigger an immediate heartbeat to replicate faster
	go rn.sendHeartbeat()
	
	return true
}

// Append adds the value to the existing value for the key
func (rn *RaftNode) Append(key, value string) bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.state != Leader {
		rn.logInfo("Append operation failed - not leader")
		return false
	}

	command := fmt.Sprintf("APPEND %s %s", key, value)
	entry := LogEntry{
		Index:     len(rn.log),
		Term:      rn.currentTerm,
		Command:   command,
		Status:    "uncommitted",
		Timestamp: time.Now(),
	}
	rn.log = append(rn.log, entry)
	
	// Apply the command locally first on the leader
	currentValue, exists := rn.kvStore[key]
	if exists {
		rn.kvStore[key] = currentValue + value
	} else {
		rn.kvStore[key] = value
	}
	
	rn.logInfo("Added log entry and applied locally: %s, new value: %s", command, rn.kvStore[key])
	
	// Trigger an immediate heartbeat to replicate faster
	go rn.sendHeartbeat()
	
	return true
}

func (rn *RaftNode) Get(key string) string {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	
	value, exists := rn.kvStore[key]
	if !exists {
		return ""
	}
	return value
}

func (rn *RaftNode) applyCommand(command string) {
	if len(command) == 0 {
		return
	}
	
	// Find the first two spaces to separate command type and key, everything after is the value
	parts := strings.SplitN(command, " ", 3)
	if len(parts) < 3 {
		rn.logInfo("Invalid command format: %s", command)
		return
	}
	
	cmdType := parts[0]
	key := parts[1]
	value := parts[2]
	
	if cmdType == "PUT" {
		rn.kvStore[key] = value
		rn.logInfo("Applied PUT command: %s = %s", key, value)
	} else if cmdType == "APPEND" {
		// For APPEND, concatenate the new value to the existing value
		currentValue, exists := rn.kvStore[key]
		if exists {
			rn.kvStore[key] = currentValue + value
		} else {
			// If key doesn't exist, create it with the value
			rn.kvStore[key] = value
		}
		rn.logInfo("Applied APPEND command: %s, new value: %s", key, rn.kvStore[key])
	}
}

func (rn *RaftNode) applyCommittedEntries() {
	if rn.commitIndex <= rn.lastApplied {
		return
	}
	
	rn.logInfo("Applying committed entries from index %d to %d", rn.lastApplied+1, rn.commitIndex)
	
	// Apply all newly committed entries
	for i := rn.lastApplied + 1; i <= rn.commitIndex; i++ {
		if i < len(rn.log) {
			entry := rn.log[i]
			rn.logInfo("Applying log entry %d: %s", i, entry.Command)
			
			// Apply the command to the state machine
			rn.applyCommand(entry.Command)
			
			// Update entry status
			rn.log[i].Status = "committed"
			
			// Update lastApplied
			rn.lastApplied = i
		} else {
			rn.logInfo("Error: Trying to apply entry at index %d but log length is %d", i, len(rn.log))
			break
		}
	}
	
	// Persist state after applying entries
	state := map[string]interface{}{
		"currentTerm": rn.currentTerm,
		"votedFor":    rn.votedFor,
		"log":         rn.log,
		"kvStore":     rn.kvStore,
	}
	if err := rn.persistStateToFile(state); err != nil {
		rn.logInfo("Error persisting state after applying entries: %v", err)
	}
}

func (rn *RaftNode) sendHeartbeat() {
	rn.mu.RLock()
	if rn.state != Leader {
		rn.logInfo("Not sending heartbeats - no longer leader")
		rn.mu.RUnlock()
		return
	}
	
	currentTerm := rn.currentTerm
	leaderID := rn.id
	commitIndex := rn.commitIndex
	peers := make([]*RaftNode, len(rn.peers))
	copy(peers, rn.peers)
	
	// Create individual next/match index maps for each peer
	nextIndices := make(map[int]int)
	prevLogIndices := make(map[int]int)
	prevLogTerms := make(map[int]int)
	
	for _, peer := range peers {
		nextIdx := rn.nextIndex[peer.id]
		nextIndices[peer.id] = nextIdx
		
		// Calculate previous log index and term for each peer
		prevLogIndex := nextIdx - 1
		prevLogIndices[peer.id] = prevLogIndex
		
		// Get the term of the previous log entry
		prevLogTerm := 0
		if prevLogIndex >= 0 && prevLogIndex < len(rn.log) {
			prevLogTerm = rn.log[prevLogIndex].Term
		}
		prevLogTerms[peer.id] = prevLogTerm
	}
	
	// Get any log entries that need to be sent
	entriesMap := make(map[int][]LogEntry)
	for _, peer := range peers {
		peerNextIdx := nextIndices[peer.id]
		if peerNextIdx < len(rn.log) {
			// This peer is missing some entries
			entriesMap[peer.id] = make([]LogEntry, len(rn.log)-peerNextIdx)
			copy(entriesMap[peer.id], rn.log[peerNextIdx:])
		} else {
			// This peer is up-to-date, send empty entries (heartbeat)
			entriesMap[peer.id] = []LogEntry{}
		}
	}
	
	rn.mu.RUnlock()
	
	// Send AppendEntries RPCs to each peer
	for _, peer := range peers {
		go func(p *RaftNode) {
			// Get the entries for this peer
			entries := entriesMap[p.id]
			prevLogIndex := prevLogIndices[p.id]
			prevLogTerm := prevLogTerms[p.id]
			
			// Create the RPC request
			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     leaderID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: commitIndex,
			}
			
			// If sending entries (not just a heartbeat)
			if len(entries) > 0 {
				rn.logInfo("Sending %d log entries to node %d (nextIndex=%d)", 
					len(entries), p.id, nextIndices[p.id])
			} else {
				rn.logInfo("Sending heartbeat to node %d", p.id)
			}
			
			// Send the request
			var reply AppendEntriesReply
			p.AppendEntries(args, &reply)
			
			// Process the response
			rn.mu.Lock()
			defer rn.mu.Unlock()
			
			// Check if we're still the leader and in the same term
			if rn.state != Leader || rn.currentTerm != currentTerm {
				rn.logInfo("Ignoring AppendEntries response from %d - no longer leader or term changed", p.id)
				return
			}
			
			if reply.Term > rn.currentTerm {
				rn.logInfo("Node %d has higher term (%d > %d), becoming follower", 
					p.id, reply.Term, rn.currentTerm)
				rn.becomeFollower(reply.Term)
				return
			}
			
			if reply.Success {
				// Update nextIndex and matchIndex for successful AppendEntries
				newNextIndex := prevLogIndex + 1 + len(entries)
				newMatchIndex := newNextIndex - 1
				
				// Only update if this would increase the indices
				if newNextIndex > rn.nextIndex[p.id] {
					rn.nextIndex[p.id] = newNextIndex
				}
				if newMatchIndex > rn.matchIndex[p.id] {
					oldMatch := rn.matchIndex[p.id]
					rn.matchIndex[p.id] = newMatchIndex
					if len(entries) > 0 {
						rn.logInfo("Updated matchIndex for node %d: %d -> %d", 
							p.id, oldMatch, newMatchIndex)
					}
				}
				
				// Check if we can commit more entries
				rn.updateCommitIndex()
			} else {
				// If the follower rejected our AppendEntries, use the conflict information
				// to update nextIndex more efficiently
				oldNext := rn.nextIndex[p.id]
				
				if reply.ConflictTerm == -1 {
					// Follower's log is too short
					rn.nextIndex[p.id] = reply.ConflictIndex
				} else {
					// Find the last entry in our log with conflictTerm
					conflictTermIndex := -1
					for i := len(rn.log) - 1; i >= 0; i-- {
						if rn.log[i].Term == reply.ConflictTerm {
							conflictTermIndex = i
							break
						}
					}
					
					if conflictTermIndex != -1 {
						// We have an entry with conflictTerm, set nextIndex to the one after it
						rn.nextIndex[p.id] = conflictTermIndex + 1
					} else {
						// We don't have any entries with conflictTerm, set nextIndex to conflictIndex
						rn.nextIndex[p.id] = reply.ConflictIndex
					}
				}
				
				rn.logInfo("AppendEntries failed for node %d, adjusting nextIndex %d -> %d", 
					p.id, oldNext, rn.nextIndex[p.id])
			}
		}(peer)
	}
}

// ForceLeader forces the node to become a leader for demonstration purposes
func (rn *RaftNode) ForceLeader() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	
	// Force to be a leader by incrementing term and setting state
	rn.currentTerm++
	rn.state = Leader
	rn.votedFor = rn.id
	
	// Initialize leader state
	for _, peer := range rn.peers {
		rn.nextIndex[peer.id] = len(rn.log)
		rn.matchIndex[peer.id] = 0
	}
	
	// Log the forced promotion
	rn.logInfo("FORCED to become leader for term %d (demonstration mode)", rn.currentTerm)
	
	// Start sending heartbeats
	go rn.sendHeartbeat()
}

func (rn *RaftNode) updateCommitIndex() {
	// If not leader, return
	if rn.state != Leader {
		return
	}
	
	// Find the median matchIndex
	matches := make([]int, 0, len(rn.peers))
	for _, peer := range rn.peers {
		matches = append(matches, rn.matchIndex[peer.id])
	}
	
	// Sort matchIndices
	sort.Ints(matches)
	
	// Find the N such that N > commitIndex and a majority of matchIndex[i] ≥ N
	// and log[N].term == currentTerm
	n := matches[len(matches)/2] // Median
	
	if n > rn.commitIndex {
		// Make sure the entry at index N is from the current term
		if n < len(rn.log) && rn.log[n].Term == rn.currentTerm {
			oldCommitIndex := rn.commitIndex
			rn.commitIndex = n
			rn.logInfo("Updated commitIndex: %d -> %d", oldCommitIndex, n)
			
			// Apply newly committed entries immediately
			rn.applyCommittedEntries()
		}
	}
}

