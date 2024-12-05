package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func main9() {
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	input := scanner.Text()
	fields := strings.Split(input, ",")
	
	if len(fields) < 6 {
		fmt.Fprintf(os.Stderr, "Invalid input format\n")
		os.Exit(1)
	}
	rate, err := strconv.ParseFloat(fields[5], 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing rate: %v\n", err)
		os.Exit(1)
	}

	var result interface{}
	if rate > 0.5 {
		result = []string{}
	} else {
		//return zoneDesc and 1 
		result = []interface{}{fields[8]}
	}

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error converting to JSON: %v\n", err)
		os.Exit(1)
	}

	fmt.Print(string(jsonBytes))
}