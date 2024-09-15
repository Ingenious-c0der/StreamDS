package main

import (
	"distributed_log_querier/functions_utility"
	"fmt"
	"testing"
	"time"
)

func TestDistributedDifferentGrepCommands(t *testing.T) {
	distributed_diff_grep_commands(t)
}

func distributed_diff_grep_commands(t *testing.T) {
	// Define the base port and auto addresses for 10 instances
	self_name := "vm1"
	self_port := "8080"
	NUM_INSTANCES := 4 //total number of instances including the current instance
	self_address := "172.22.156.92:8080" // hard coded for vm1 instance which is 2801
	auto_addresses := []string{
		"172.22.156.92:8080",
		"172.22.158.92:8080",
		"172.22.94.92:8080",
		"172.22.156.93:8080",
		"172.22.158.93:8080",
		"172.22.94.93:8080",
		"172.22.156.94:8080",
		"172.22.158.94:8080",
		"172.22.94.94:8080",
		"172.22.156.95:8080",
	}
	//declare autoAddresses as empty string list
	autoAddresses := []string{}
	for i := 0; i < NUM_INSTANCES; i++ { 
		if(auto_addresses[i] != self_address){
			autoAddresses = append(autoAddresses, auto_addresses[i])
		}
	}

	cmd, stdin, err:= functions_utility.StartInstance(self_port, self_name, autoAddresses)
	if err != nil {
		t.Fatalf("Error starting instance %s: %v", self_name, err)
	}
	stdin.Write([]byte("CONN AUTO\n")) //connect to all the other instances 

	// Wait for a while to allow processes to run and communicate
	time.Sleep(5 * time.Second)
	grep_patterns := []string{
		"grep 'DELETE'", 
		"grep -c 'http'", //frequent pattern
		"grep 'ERROR' | grep -v 'DEBUG'", //pipe operator
		"grep -E '\\b(1[0-9][0-9]|2[0-0][0-9]|[0-9]{1,2})\\.(0?[0-9]|[1-4][0-9]|50)\\.[0-9]{1,3}\\.[0-9]{1,3}\\b'",
		"grep 'PUT'", //20% occurence 
		"grep 'POST'",
		"grep  -E 'Aug|Feb|Dec'",
	}

	for _, pattern := range grep_patterns {
		err := functions_utility.SendCommand(stdin, pattern)
		if err != nil {
			t.Errorf("Error sending GREP command to process: %v", err)
		} else {
			fmt.Println("Sent GREP command")
		}
		time.Sleep(5 * time.Second)
	}

	// Wait for a while to allow processes to run and communicate
	time.Sleep(2 * time.Second)

	// Gracefully terminate instances by sending the "EXIT" command
	err_exit := functions_utility.SendExitCommand(stdin)
	if err_exit != nil {
		t.Errorf("Error sending EXIT command: %v", err)
	} else {
		fmt.Println("Sent EXIT command")
	}
	err_cmd := cmd.Wait()
	if err_cmd != nil {
		t.Errorf("Error waiting for process with PID %d to exit: %v", cmd.Process.Pid, err)
	} else {
		fmt.Printf("Process with PID %d exited cleanly\n", cmd.Process.Pid)
	}
	
}
