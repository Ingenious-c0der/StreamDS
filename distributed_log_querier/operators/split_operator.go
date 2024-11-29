package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

func main2() {
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	input := scanner.Text()
	words := strings.Fields(input)

	// Convert to JSON array
	jsonBytes, err := json.Marshal(words)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error converting to JSON: %v\n", err)
		os.Exit(1)
	}

	fmt.Print(string(jsonBytes))
}