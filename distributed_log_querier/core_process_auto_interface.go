package main

// import (
// 	"distributed_log_querier/core_process"
// 	"fmt"
// 	"os"
// 	"strings"
// 	"sync"
// 	"time"
// )
/*
Important, when running unit tests make sure you are in the parent directory and
switch the function name here from main_auto to main and change the name of main function in 
distributed_log_querier/core_process_auto_manual.go to main_manual. As unit tests need to use main 
function in core_process_auto_interface.go file.
*/
// func main(){
// 	var wg sync.WaitGroup
// 	var pattern string

// 	// Read name and port from environment variables
// 	name := os.Getenv("SELF_NAME")
// 	port := os.Getenv("PORT")
// 	// name:="test"
// 	// port:="8080"
// 	if name == "" || port == "" {
// 		fmt.Println("Please provide the machine name and port")
// 		return
// 	}

// 	autoAddresses := strings.Split(os.Getenv("AUTO_ADDRESSES"), " ")
// 	//autoAddresses := []string{"[::]:8081"}
// 	// Print auto addresses
// 	fmt.Println(autoAddresses)
// 	fmt.Println(len(autoAddresses))
// 	peers := sync.Map{}
// 	alive_peers := sync.Map{}
// 	grep_result_accumulator := sync.Map{}
// 	var latencyStart time.Time

// 	fmt.Printf("Starting instance %s on port %s\n", name, port)
// 	wg.Add(2)
// 	go distributed_log_querier.ListenOnNetwork(&pattern, port, name, &latencyStart, &grep_result_accumulator, &peers, &alive_peers, &wg)
// 	go distributed_log_querier.SetupCommTerminal(&pattern, name, autoAddresses, &latencyStart, &grep_result_accumulator, &peers, &alive_peers, &wg)

// 	wg.Wait()
// }
