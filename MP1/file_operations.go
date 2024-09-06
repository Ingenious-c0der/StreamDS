package main

import (
	"bufio"
	"bytes"
	"distributed_log_querier/grep"
	"flag"
	"io"
	"math/rand"
	"os"
	"strings"
)

const (
	charList = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

func appendToFile(filePath string, data []byte) error {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	_, err = writer.Write(data)
	if err != nil {
		return err
	}

	err = writer.Flush()
	if err != nil {
		return err
	}

	return nil
}

// func readFromFile(filePath string) error {
// 	file, err := os.Open(filePath)
// 	if err != nil {
// 		return err
// 	}
// 	defer file.Close()

// 	scanner := bufio.NewScanner(file)
// 	for scanner.Scan() {
// 		fmt.Println(scanner.Text())
// 	}

// 	if err := scanner.Err(); err != nil {
// 		return err
// 	}

// 	return nil
// }

func randomizer() []byte {
	size := 2048
	data := make([]byte, size)
	for i := range data {
		data[i] = charList[(rand.Intn(len(charList)))]
	}
	return data
}

func grep_command(options []string, input io.Reader) ([]string, error) {
	// Parse flags and arguments
	p := grep.ParseParams()
	flag.CommandLine.Parse(options)

	// Prepare output buffer and input reader
	output := new(bytes.Buffer)
	inputCloser := io.NopCloser(input)

	// Run grep command
	cmd := grep.Command(inputCloser, output, os.Stderr, p, flag.Args())
	if err := cmd.Run(); err != nil {
		return nil, err
	}

	// Split and return the result lines
	results := strings.Split(strings.TrimSpace(output.String()), "\n")
	return results, nil
}
// func main() {
// 	file, err := os.Open("../sample.txt")
// 	if err != nil {
// 		fmt.Println("Error opening file:", err)
// 		return
// 	}
// 	defer file.Close()

// 	// Prepare the grep options and pattern
// 	options := []string{"af" ,"-c"} // grep options and pattern

// 	// Call the grep_command
// 	results, err := grep_command(options, file)
// 	if err != nil {
// 		fmt.Println("Error:", err)
// 		return
// 	}

// 	// Print the results
// 	for _, line := range results {
// 		fmt.Println(line)
// 	}

// 	fmt.Println("Successfully ran grep command")
// }
