package main


import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"encoding/csv"
)


func main() {
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	input := scanner.Text()
	reader := csv.NewReader(strings.NewReader(input))
    reader.LazyQuotes = true
    reader.Comma = ','
    record, err := reader.Read()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error reading CSV: %v\n", err)
        os.Exit(1)
    }
	result := []string{record[2], record[3]}
	jsonBytes, err := json.Marshal(result)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error converting to JSON: %v\n", err)
		os.Exit(1)
	}

	fmt.Print(string(jsonBytes))
}