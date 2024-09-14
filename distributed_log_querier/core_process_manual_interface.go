package main 


import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
	"distributed_log_querier/core_process"
	"bufio"
)

// func TestMainManual() {
// 	main_manual()
// }

func main() {
	var wg sync.WaitGroup
	var pattern string
	
	fmt.Print("Enter the machine name : ")
	var name string
	fmt.Scan(&name)
	fmt.Print("Enter the machine address(port) : ")
	var port string
	fmt.Scan(&port)
	reader := bufio.NewReader(os.Stdin)

	// Prompt the user for input
	fmt.Println("Enter auto addresses separated by spaces:")

	// Read a full line of input
	input, _ := reader.ReadString('\n')

	// Trim any trailing newline characters
	input = strings.TrimSpace(input)
	// output:= grepMain(name+".log", input)
	// fmt.Println(string(output))
	// Split the input by spaces into a slice of strings
	autoAddresses := strings.Fields(input)
	for i, address := range autoAddresses {
		autoAddresses[i] = strings.TrimSpace(address)
	}
	fmt.Println("Auto addresses", autoAddresses)
	peers := sync.Map{}
	alive_peers := sync.Map{}
	grep_result_accumulator := sync.Map{}
	var latencyStart time.Time; 

	fmt.Printf("Starting instance %s on port %s\n", name, port)
	wg.Add(2)
	go distributed_log_querier.ListenOnNetwork(&pattern, port, name,&latencyStart, &grep_result_accumulator, &peers, &alive_peers, &wg)
	go distributed_log_querier.SetupCommTerminal(&pattern, name, autoAddresses,&latencyStart, &grep_result_accumulator, &peers, &alive_peers, &wg)

	wg.Wait()
}