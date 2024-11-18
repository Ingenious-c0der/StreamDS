package main 

import (
	"fmt"
	"distributed_log_querier/core_process"
	"os"
	"sync"
)


func main(){
	var wg sync.WaitGroup
	hydfsGlobalPort := os.Getenv("HGP")
	if hydfsGlobalPort == "" {
		fmt.Println("Please provide the hydfs global port")
		return
	}
	hyDFSSelfPort := os.Getenv("HSP")
	if hyDFSSelfPort == "" {
		fmt.Println("Please provide the hydfs self port")
		return
	}
	self_hydfs_address := distributed_log_querier.GetOutboundIP().String()
	if self_hydfs_address == "" {
		fmt.Println("Error in getting the self hydfs address")
		return
	}
	self_hydfs_address = self_hydfs_address + ":" + hydfsGlobalPort
	wg.Add(1)
	go distributed_log_querier.StartHyDFS(hyDFSSelfPort, hydfsGlobalPort, self_hydfs_address, &wg)
	//wait
	wg.Wait()
	

}

