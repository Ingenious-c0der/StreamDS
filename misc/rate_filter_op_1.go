package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)
//Simple : Active ="no" & blockNum = 1000
func main13() {
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
	
	if len(fields) < 12 {
		jB , _ :=json.Marshal([]interface{}{})
		fmt.Println(string(jB))
	}
	active := fields[12]
	object_id := fields[2]
	var result interface{}
	if active == "No" {
		result = []string{object_id}
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