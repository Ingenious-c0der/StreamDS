package main

import (
	"bufio"
	"bytes"
	"distributed_log_querier/grep"
	"flag"
	"fmt"
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

func readFromFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fmt.Println(scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

func randomizer() []byte {
	size := 2048
	data := make([]byte, size)
	for i := range data {
		data[i] = charList[(rand.Intn(len(charList)))]
	}
	return data
}

func grep_command(pattern string, input io.Reader) ([]string, error) {
	args := strings.Fields(pattern)
	p := grep.ParseParams()
	flag.CommandLine.Parse(args)
	output := new(bytes.Buffer)
	inputCloser := io.NopCloser(input)
	cmd := grep.Command(inputCloser, output, os.Stderr, p, flag.Args())
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	results := strings.Split(strings.TrimSpace(output.String()), "\n")
	return results, nil
}

// func main() {
// 	filepathName := "machine1.i.log"
// 	generatedData := randomizer()

// 	err := appendToFile(filepathName, generatedData)
// 	if err != nil {
// 		fmt.Println("the error is as following => ", err)
// 		return
// 	}

// 	err1 := readFromFile(filepathName)
// 	if err1 != nil {
// 		fmt.Println("the error is as following => ", err)
// 		return
// 	}

// 	fmt.Println("succesfully entered data in file")

// }
