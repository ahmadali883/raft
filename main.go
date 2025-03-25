package main

import (
	"A2/raft"
	"time"
	"os"
	"path/filepath"
	"fmt"
	"os/signal"
	"syscall"
	"log"
	"io"
	"context"
)

func main() {
	fmt.Println("╔══════════════════════════════════════════════╗")
	fmt.Println("║       RAFT CONSENSUS ALGORITHM DEMO          ║")
	fmt.Println("╚══════════════════════════════════════════════╝")
	
	// Set up signal handling early
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	
	// Set up a context that will be canceled when Ctrl+C is pressed
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Start a goroutine to handle Ctrl+C
	go func() {
		<-sigCh
		fmt.Println("\n\n🛑 Received shutdown signal, stopping cluster...")
		cancel() // Cancel the context to signal shutdown
	}()
	
	// Create base directory for node data in current directory
	baseDir := "raft-data"
	
	// Clean up existing data for a fresh start
	fmt.Println("🧹 Cleaning up existing data...")
	os.RemoveAll(baseDir)
	
	// Create directory structure
	fmt.Println("📁 Creating directory structure...")
	os.MkdirAll(baseDir, 0755)

	// Set up cluster log file
	fmt.Println("📝 Setting up logging...")
	clusterLogFile, err := os.OpenFile(filepath.Join(baseDir, "cluster.log"), 
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("❌ Error opening cluster log file: %v\n", err)
		return
	}
	defer clusterLogFile.Close()

	// Create multiwriter to write to both file and terminal
	multiWriter := io.MultiWriter(clusterLogFile, os.Stdout)
	clusterLogger := log.New(multiWriter, "[Cluster] ", log.LstdFlags)
	
	// Keep track of stopped nodes
	stoppedNodes := make(map[int]bool)

	// Create node directories for 5 nodes
	numNodes := 5
	for i := 1; i <= numNodes; i++ {
		nodeDir := filepath.Join(baseDir, fmt.Sprintf("node%d", i))
		os.MkdirAll(nodeDir, 0755)
		
		// Create a symlink from node logs to terminal for easy viewing
		nodeLogFile := filepath.Join(nodeDir, fmt.Sprintf("node%d.log", i))
		if file, err := os.Create(nodeLogFile); err == nil {
			file.Close()
		}
	}

	// Create 5 nodes with their own persistent directories
	fmt.Println("\n🚀 Creating Raft nodes...")
	clusterLogger.Println("Creating Raft nodes...")
	
	nodes := make([]*raft.RaftNode, numNodes)
	
	for i := 1; i <= numNodes; i++ {
		nodes[i-1] = raft.NewRaftNode(i, nil, filepath.Join(baseDir, fmt.Sprintf("node%d", i)))
		if nodes[i-1] == nil {
			fmt.Printf("❌ Failed to create node %d\n", i)
			return
		}
	}
	
	fmt.Println("✅ All nodes created successfully")

	// Set peers for all nodes
	fmt.Println("\n🔄 Setting up peer relationships...")
	clusterLogger.Println("Setting up peer relationships...")
	
	for i, node := range nodes {
		// Create a list of peers (all nodes except the current one)
		peers := make([]*raft.RaftNode, 0, numNodes-1)
		for j, peer := range nodes {
			if i != j { // Skip self
				peers = append(peers, peer)
			}
		}
		node.SetPeers(peers)
	}
	
	fmt.Println("✅ Peer relationships established")

	fmt.Println("\n🛫 Starting Raft cluster with 5 nodes...")
	clusterLogger.Println("Starting Raft cluster with 5 nodes...")

	// Simulate client requests with timeout context
	fmt.Println("\n⏳ Waiting for leader election (max 10 seconds)...")
	
	electionCtx, electionCancel := context.WithTimeout(ctx, 10*time.Second)
	defer electionCancel()
	
	// Progress dots in a separate goroutine
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		
		for {
			select {
			case <-ticker.C:
				fmt.Print(".")
			case <-electionCtx.Done():
				return
			}
		}
	}()
	
	// Wait for leader election with a timeout
	var leader *raft.RaftNode = nil
	var leaderIdx int = -1
	
	// Poll for a leader with a timeout
	startTime := time.Now()
	electionTimeout := 10 * time.Second
	pollInterval := 250 * time.Millisecond
	
	for time.Since(startTime) < electionTimeout {
		select {
		case <-ctx.Done():
			// Ctrl+C was pressed, clean up and exit
			shutdownCluster(nodes, clusterLogger, baseDir, stoppedNodes)
			return
		default:
			// Check if any node has become leader
			for i, node := range nodes {
				if node.GetState() == raft.Leader {
					leader = node
					leaderIdx = i + 1
					goto LeaderFound // Break out of nested loops
				}
			}
			time.Sleep(pollInterval)
		}
	}
	
LeaderFound:
	fmt.Println("\n✅ Leader election period complete")
	
	// Check which node is the leader
	fmt.Println("\n🔍 Checking node states...")
	
	fmt.Println("┌─────────┬────────────┐")
	fmt.Println("│  Node   │    State   │")
	fmt.Println("├─────────┼────────────┤")
	
	for i, node := range nodes {
		state := node.GetState()
		fmt.Printf("│  Node %d  │  %-9s │\n", i+1, state)
		clusterLogger.Printf("Node %d state: %s", i+1, state)
	}
	fmt.Println("└─────────┴────────────┘")
	
	if leader != nil {
		fmt.Printf("\n👑 Node %d is the leader\n", leaderIdx)
		clusterLogger.Printf("Node %d is the leader", leaderIdx)
	} else {
		fmt.Println("\n⚠️ No leader detected after timeout!")
		clusterLogger.Printf("No leader detected after timeout")
		
		// Try to force a leader for demonstration purposes
		fmt.Println("🛠️ Forcing node 1 to become leader for demonstration...")
		nodes[0].ForceLeader()
		leader = nodes[0]
		leaderIdx = 1
	}
	
	// If we have a leader, perform operations
	if leader != nil {
		fmt.Printf("\n🔄 Performing operations on leader (Node %d)...\n", leaderIdx)
		clusterLogger.Printf("Leader found, performing operations...")
		
		// Write some key-value pairs
		fmt.Println("\n📝 Writing key-value pairs:")
		fmt.Println("  • Setting name = Alice")
		leader.Put("name", "Alice")
		
		fmt.Println("  • Setting age = 30")
		leader.Put("age", "30")
		
		fmt.Println("  • Setting city = New York")
		leader.Put("city", "New York")
		
		// Test the Append operation
		fmt.Println("\n📝 Testing APPEND operation:")
		fmt.Println("  • Creating greeting = Hello")
		leader.Put("greeting", "Hello")
		fmt.Printf("  • Current greeting: %s\n", leader.Get("greeting"))
		
		fmt.Println("  • Appending to greeting: , World!")
		leader.Append("greeting", ", World!")
		fmt.Printf("  • Updated greeting: %s\n", leader.Get("greeting"))
		
		// Read them back
		fmt.Println("\n📖 Reading values from leader:")
		fmt.Printf("  • name: %s\n", leader.Get("name"))
		fmt.Printf("  • age: %s\n", leader.Get("age"))
		fmt.Printf("  • city: %s\n", leader.Get("city"))
		fmt.Printf("  • greeting: %s\n", leader.Get("greeting"))
		
		clusterLogger.Printf("Key-value pairs stored:")
		clusterLogger.Printf("  name: %s", leader.Get("name"))
		clusterLogger.Printf("  age: %s", leader.Get("age"))
		clusterLogger.Printf("  city: %s", leader.Get("city"))
		clusterLogger.Printf("  greeting: %s", leader.Get("greeting"))
		
		// Wait a bit to let replication happen
		fmt.Println("\n⏳ Waiting for replication (1 second)...")
		select {
		case <-time.After(1 * time.Second):
			fmt.Println("✅ Replication period complete")
		case <-ctx.Done():
			shutdownCluster(nodes, clusterLogger, baseDir, stoppedNodes)
			return
		}
		
		// Try reading from other nodes
		fmt.Println("\n🔍 Verifying data on follower nodes:")
		for i, node := range nodes {
			if node != leader {
				fmt.Printf("\n📖 Node %d (follower) data:\n", i+1)
				name := node.Get("name")
				age := node.Get("age")
				city := node.Get("city")
				greeting := node.Get("greeting")
				
				fmt.Printf("  • name: %s %s\n", name, getStatusEmoji(name == "Alice"))
				fmt.Printf("  • age: %s %s\n", age, getStatusEmoji(age == "30"))
				fmt.Printf("  • city: %s %s\n", city, getStatusEmoji(city == "New York"))
				fmt.Printf("  • greeting: %s %s\n", greeting, getStatusEmoji(greeting == "Hello, World!"))
			}
		}
		
		// Simulate leader failure after 7 seconds
		fmt.Println("\n⏳ Running for 7 seconds before simulating leader failure...")
		
		// Set up a timer to kill the leader
		leaderFailureTimer := time.NewTimer(7 * time.Second)
		
		go func() {
			select {
			case <-leaderFailureTimer.C:
				if leader != nil {
					// Store which node was the leader
					oldLeaderIdx := leaderIdx
					
					fmt.Printf("\n💥 Simulating failure of leader (Node %d)...\n", oldLeaderIdx)
					clusterLogger.Printf("Simulating failure of Node %d (leader)", oldLeaderIdx)
					
					// Stop the leader node
					leader.Stop()
					stoppedNodes[oldLeaderIdx] = true
					
					fmt.Println("🔄 Leader node stopped. Waiting for re-election...")
					clusterLogger.Printf("Leader node stopped. Waiting for re-election...")
					
					// Wait for a new leader to be elected
					newLeaderElected := false
					startTime := time.Now()
					for time.Since(startTime) < 10*time.Second && !newLeaderElected {
						for i, node := range nodes {
							if i+1 != oldLeaderIdx && node.GetState() == raft.Leader {
								fmt.Printf("\n👑 Node %d is the new leader\n", i+1)
								clusterLogger.Printf("Node %d is the new leader", i+1)
								leader = node
								leaderIdx = i + 1
								newLeaderElected = true
								
								// Wait a bit to let the leader stabilize
								time.Sleep(1 * time.Second)
								
								// Add more data with the new leader
								fmt.Println("\n📝 Writing more data with new leader:")
								
								// Try the operations with retry logic
								tryOperation := func(operation func() bool, description string) {
									maxRetries := 5
									success := false
									
									for retry := 0; retry < maxRetries && !success; retry++ {
										if retry > 0 {
											fmt.Printf("  • Retrying %s (attempt %d)...\n", description, retry+1)
											time.Sleep(500 * time.Millisecond)
										}
										
										success = operation()
										if success {
											break
										}
									}
									
									if !success {
										fmt.Printf("  • Failed to %s after %d attempts\n", description, maxRetries)
									}
								}
								
								// Try to add location
								tryOperation(func() bool {
									return leader.Put("location", "Office")
								}, "set location")
								
								// Try to append to name
								tryOperation(func() bool {
									return leader.Append("name", " Smith")
								}, "append to name")
								
								// Read back values
								fmt.Println("\n📖 Reading values from new leader:")
								fmt.Printf("  • name: %s\n", leader.Get("name"))
								fmt.Printf("  • location: %s\n", leader.Get("location"))
								
								break
							}
						}
						if !newLeaderElected {
							time.Sleep(500 * time.Millisecond)
							fmt.Print(".")
						}
					}
					
					if !newLeaderElected {
						fmt.Println("\n⚠️ No new leader elected after timeout!")
						clusterLogger.Printf("No new leader elected after timeout")
					}
				}
			case <-ctx.Done():
				// Context cancelled, do nothing
				return
			}
		}()
		
		// Run for longer to observe re-election
		fmt.Println("\n🕒 System will run for 60 seconds to observe behavior...")
		fmt.Println("   (Press Ctrl+C to terminate earlier)")
		
		// Wait for shutdown signal or timeout
		runDuration := 60 * time.Second
		runTimer := time.NewTimer(runDuration)
		
		select {
		case <-ctx.Done():
			// Ctrl+C was pressed
			runTimer.Stop()
		case <-runTimer.C:
			fmt.Println("\n⏰ Automatic shutdown after 60 seconds...")
		}
	} else {
		fmt.Println("\n❌ No leader elected within timeout")
		clusterLogger.Printf("No leader elected within timeout")
	}
	
	// Stop all nodes and clean up
	shutdownCluster(nodes, clusterLogger, baseDir, stoppedNodes)
}

func shutdownCluster(nodes []*raft.RaftNode, logger *log.Logger, baseDir string, stoppedNodes map[int]bool) {
	fmt.Println("\n🔄 Stopping all nodes...")
	logger.Println("Stopping all nodes...")
	
	for i, node := range nodes {
		if node != nil && !stoppedNodes[i+1] {
			// Only try to stop nodes that haven't been stopped already
			node.Stop()
			stoppedNodes[i+1] = true
		}
	}
	
	fmt.Println("✅ Cluster shutdown complete.")
	logger.Println("Cluster shutdown complete.")
	
	fmt.Println("\n📁 Log files are available in the raft-data directory:")
	fmt.Printf("  • %s/cluster.log\n", baseDir)
	
	for i := 1; i <= len(nodes); i++ {
		fmt.Printf("  • %s/node%d/node%d.log\n", baseDir, i, i)
	}
}

func getStatusEmoji(success bool) string {
	if success {
		return "✅"
	}
	return "❌"
}
