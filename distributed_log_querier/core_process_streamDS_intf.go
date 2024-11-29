package main 

import (
	"fmt"
	"distributed_log_querier/core_process"
	"os"
	"sync"
)


func main(){
	//run as 
	//IS_LEADER=true SGP=8080 HSP=3050 go run core_process_streamDS_intf.go 
	var wg sync.WaitGroup
	streamDSGlobalPort := os.Getenv("SGP")
	if streamDSGlobalPort == "" {
		fmt.Println("Please provide the hydfs global port")
		return
	}
	hyDFSSelfPort := os.Getenv("HSP")
	if hyDFSSelfPort == "" {
		fmt.Println("Please provide the hydfs self port")
		return
	}
	selfStreamDSAddress := distributed_log_querier.GetOutboundIP().String()
	if selfStreamDSAddress == "" {
		fmt.Println("Error in getting the self hydfs address")
		return
	}
	isLeader := os.Getenv("IS_LEADER")
	var isLead bool
	if isLeader == "" {
		isLead = false
	}else{
		isLead = true
	}
	selfStreamDSAddress = selfStreamDSAddress + ":" + streamDSGlobalPort
	wg.Add(1)
	go distributed_log_querier.StartStreamDS(isLead, selfStreamDSAddress, hyDFSSelfPort, streamDSGlobalPort, &wg)
	//wait
	wg.Wait()
	

}