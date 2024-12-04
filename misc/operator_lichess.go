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

func main() {
	// Open the input file
	file, err := os.Open("/Users/ingenious/Documents/DSMP1_backup/CS-425-MP/MP1/g28/HYDFS/business/Lichess.txt")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	resultMap := make(map[string]int)
	total := 0
	for scanner.Scan() {
		input := scanner.Text()
		fields := strings.Split(input, ",")

		if len(fields) < 15 {
			continue
		}

		winner := fields[6]
		moves := fields[4]
		victory_status := fields[5]
		opening_name := fields[14]

		moves_int, err := strconv.ParseFloat(moves, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing moves: %v\n", err)
			continue
		}

		arr := []string{"mate", "resign", "outoftime"}
		openings := []string{"Scandinavian Defense", "Sicilian Defense", "Indian Game", "Caro-Kann Defense", "Italian Game"}

		if winner == "white" && moves_int > 40 && slices.Contains(arr, victory_status) && slices.Contains(openings, opening_name) {
			resultMap[opening_name]++
			total++
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading file: %v\n", err)
		os.Exit(1)
	}

	jsonBytes, err := json.Marshal(resultMap)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error converting to JSON: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(total)

	fmt.Println(string(jsonBytes))
}