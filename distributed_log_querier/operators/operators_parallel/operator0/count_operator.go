package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	
)


// WordCountTracker manages persistent word counting
type WordCountTracker struct {
	mu    sync.RWMutex
	counts map[string]int
}

// NewWordCountTracker creates a new tracker and loads existing counts
func NewWordCountTracker(stateFilePath string) *WordCountTracker {
	tracker := &WordCountTracker{
		counts: make(map[string]int),
	}
	tracker.loadCounts(stateFilePath)
	return tracker
}

// loadCounts reads existing word counts from the persistent file
func (wct *WordCountTracker) loadCounts(stateFilePath string) {
	wct.mu.Lock()
	defer wct.mu.Unlock()

	// Open the file, create if not exists
	file, err := os.OpenFile(stateFilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening state file: %v\n", err)
		return
	}
	defer file.Close()

	// Read and parse existing counts
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), ":")
		if len(parts) == 2 {
			word := parts[0]
			count, err := strconv.Atoi(parts[1])
			if err == nil {
				wct.counts[word] = count
			}
		}
	}
}

// updateCount increments the count for a given word and saves to file
func (wct *WordCountTracker) updateCount(word string, stateFilePath string) {
	wct.mu.Lock()
	defer wct.mu.Unlock()

	// Increment count
	wct.counts[word]++

	// Write updated counts to file
	file, err := os.Create(stateFilePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating state file: %v\n", err)
		return
	}
	defer file.Close()

	// Write each word and its count to the file
	for w, count := range wct.counts {
		fmt.Fprintf(file, "%s:%d\n", w, count)
	}
}

func main() {
	// Create a new word count tracker
	//stateFilePath := distributed_log_querier.GetDistributedLogQuerierDir()
	//stateFilePath = filepath.Join(stateFilePath, "business","word_count_state.txt")
	//_,currentFile,_,_ := runtime.Caller(0)
	//fmt.Println(currentFile)
	//dir := filepath.Dir(currentFile) //operator{i}
	stateFilePath := "/home/sra9/g28/distributed_log_querier/operators/operators_parallel/operator0/word_count_state.txt"
	//stateFilePath = filepath.Join(dir,stateFilePath)
	//fmt.Println(stateFilePath)
	tracker := NewWordCountTracker(stateFilePath)
	// Read input from stdin
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	// Get the input word
	word := scanner.Text()

	// Update the count for the word
	tracker.updateCount(word,stateFilePath)

	// Convert counts to JSON for output
	jsonBytes, err := json.Marshal(tracker.counts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error converting to JSON: %v\n", err)
		os.Exit(1)
	}

	// Print the entire word count map as JSON
	fmt.Print(string(jsonBytes))
}