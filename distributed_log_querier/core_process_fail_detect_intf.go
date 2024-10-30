// +build main1
package main 
import (
	"distributed_log_querier/core_process"
	"sync"
	"os"
	"fmt"
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
		fmt.Println("Please provide the log file name")
		return
	}
	isIntroducer := os.Getenv("IS_INTRODUCER")
	var isintro bool
	if isIntroducer == "" {
		isintro= false 
	}else{
		isintro = true
	}

	go distributed_log_querier.Startup(intro_address, version, self_port, log_file_name, isintro,&wg)
	//go distributed_log_querier.SetupTerminal(&wg)	
	wg.Add(2)
	wg.Wait()
}
