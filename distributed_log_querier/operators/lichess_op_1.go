package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)
//Simple: Winner = White & victory_status=Mate

func main10() {
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
	
	if len(fields) < 14 {
		jsonBytes, _ := json.Marshal([]string{})
		fmt.Println(string(jsonBytes))
	}
	winner := fields[6]
	var result interface{}
	if winner == "white"  {
		result = []string{fields[0]}
	} else {
		//return zoneDesc and 1 
		result = []interface{}{}
	}

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error converting to JSON: %v\n", err)
		os.Exit(1)
	}

	fmt.Print(string(jsonBytes))
}