package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"slices"
)

func main14() {
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
	moves:= fields[4]
	victory_status := fields[5]
	opening_name:= fields[14]
	moves_int, err := strconv.ParseFloat(moves, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing rate: %v\n", err)
		os.Exit(1)
	}
	arr := []string{"mate", "resign", "outoftime"}
	openings := []string {"Scandinavian Defense", "Sicilian Defense", "Indian Game", "Caro-Kann Defense", "Italian Game"}
	var result interface{}
	if winner == "white" && moves_int > 40 && slices.Contains(arr, victory_status) && slices.Contains(openings, opening_name) {
		opening_name_comps := strings.Split(opening_name, " ")
		condensed_opening_name := opening_name_comps[0] + "_" + opening_name_comps[1]
		result = []string{condensed_opening_name}
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