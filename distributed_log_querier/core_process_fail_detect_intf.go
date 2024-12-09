//go:build main1
// +build main1

package main

import (
	"distributed_log_querier/core_process"
	"fmt"
	"os"
	"strconv"
	"sync"
)
func main(){ 
	var wg sync.WaitGroup
	fmt.Println("Starting the distributed log querier")
	intro_address := os.Getenv("INTRO_ADDRESS")
	if intro_address == "" {
		fmt.Println("Please provide the intro address")
		return
	}
	version := os.Getenv("VERSION")
	if version == "" {
		fmt.Println("Please provide the version")
		return
	}
	self_port := os.Getenv("SELF_PORT")
	if self_port == "" {
		fmt.Println("Please provide the self port")
		return
	}
	log_file_name := os.Getenv("LOG_FILE_NAME")
	if log_file_name == "" {
		//create hash on the basis of self port
		log_file_name = "log_file_" + self_port + ".log"
	}
	isIntroducer := os.Getenv("IS_INTRODUCER")
	var isintro bool
	if isIntroducer == "" {
		isintro= false 
	}else{
		isintro = true
	}
	//remove for local testing
	// selfHYDFSPort:= os.Getenv("HSP")
	// if selfHYDFSPort == "" {
	// 	fmt.Println("Please provide the self port")
	// 	return
	// }
	//establish the connection with hydfs layer using self pipe using safe conn 
	//VM MARKER
	selfHYDFSPort := subtractStrings(self_port, 3030)
	if selfHYDFSPort == "" {
		fmt.Println("Error in substracting strings")
		return
	}
	//VM MARKER END
	safeConn := distributed_log_querier.StartSelfPipeHYDFS(selfHYDFSPort)
	if safeConn == nil {
		fmt.Println("Error in starting self pipe")
		return
	}
	go distributed_log_querier.Startup(intro_address, version, self_port, log_file_name, isintro,&wg, safeConn)
	//go distributed_log_querier.SetupTerminal(&wg)	
	wg.Add(2)
	wg.Wait()
}

func subtractStrings(a string, b int) string{
	//convert a to int and the subtract b from it
	//return the result as string
	a_int, err :=strconv.Atoi(a)
	if err != nil {
		fmt.Println("Error in converting string to int in subtract strings")
		return ""
	}
	a_int = a_int - b
	return strconv.Itoa(a_int)
}
