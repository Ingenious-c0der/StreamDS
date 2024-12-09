package main
import (
	"bufio"
    "encoding/json"
    "encoding/csv"
    "fmt"
    "os"
    "strings"
)

func main56(){
	scanner := bufio.NewScanner(os.Stdin)
    
    // Read parameter
    if !scanner.Scan() {
        fmt.Fprintf(os.Stderr, "Error reading parameter\n")
        os.Exit(1)
    }
    parameter := scanner.Text()

    // Read input
    if !scanner.Scan() {
        fmt.Fprintf(os.Stderr, "Error reading input\n")
        os.Exit(1)
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
    var result interface{}
    if parameter == record[6]{
        result = []string{record[8]}
    } else {
        result = []interface{}{}
    }

    jsonBytes, err := json.Marshal(result)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error converting to JSON: %v\n", err)
        os.Exit(1)
    }

    fmt.Print(string(jsonBytes))

}