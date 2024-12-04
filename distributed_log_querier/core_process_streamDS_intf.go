//go:build main4
// +build main4
package main 

import (
	"fmt"
	"distributed_log_querier/core_process"
	"os"
	"sync"
	"strconv"
)


func main(){
	//run as 
	//IS_LEADER=true SGP=8080 HSP=3050 go run core_process_streamDS_intf.go DEMO
	//IS_LEADER=true SGP=9091 go run core_process_streamDS_intf.go LOCAL 
	var wg sync.WaitGroup
	streamDSGlobalPort := os.Getenv("SGP")
	if streamDSGlobalPort == "" {
		fmt.Println("Please provide the hydfs global port")
		return
	}
	//VM MARKER
	hyDFSSelfPort := os.Getenv("HSP")
	if hyDFSSelfPort == "" {
		fmt.Println("Please provide the hydfs self port")
		return
	}
	//VM MARKER END
	//hyDFSSelfPort := subtractStrings(streamDSGlobalPort, 4040)
	selfStreamDSAddress := distributed_log_querier.GetOutboundIP().String()
	if selfStreamDSAddress == "" {
		fmt.Println("Error in getting the self hydfs address")
		return
	}
	fmt.Println("selfStreamDSAddress: ", selfStreamDSAddress)
	isLeader := os.Getenv("IS_LEADER")
	var isLead bool
	if isLeader == "" {
		isLead = false
	}else{
		isLead = true
	}
	safeConn:= distributed_log_querier.StartStreamDSPipe(hyDFSSelfPort)
	if safeConn == nil {
		fmt.Println("Error in starting self stream DS pipe to hydfs")
		return
	}
	//added to make sure node ids are same across hydfs and stream DS layer irrespective of the differering ports leading to different ids
	//VM MARKER CHECK?
	manip_address := subtractStrings(streamDSGlobalPort, 4040)
	selfStreamDSAddress = selfStreamDSAddress + ":" + manip_address
	//VM MARKER END
	wg.Add(1)
	go distributed_log_querier.StartStreamDS(isLead, safeConn,selfStreamDSAddress, hyDFSSelfPort, streamDSGlobalPort, &wg)
	//wait
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