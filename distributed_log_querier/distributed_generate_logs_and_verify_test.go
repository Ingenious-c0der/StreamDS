package main

import (
	"distributed_log_querier/functions_utility"
	"fmt"
	"testing"
	"time"
	"os"
   "strings"
)

func TestDistributedGenerateLogsAndVerify(t *testing.T) {
	distributed_generate_logs_and_verify(t)
}

func distributed_generate_logs_and_verify(t *testing.T) {
	var custom_patterns []string
	custom_filename := os.Getenv("CUSTOM_FILENAME")
	custom_patterns_input := os.Getenv("CUSTOM_PATTERNS")
	if custom_filename == "" {
		custom_filename = "test_run.log"
	}
	if custom_patterns_input == "" {
		custom_patterns = []string{"NEWLINE"}
	}else{
		// Split the input by spaces into a slice of strings separated by comma
		custom_patterns = strings.Split(custom_patterns_input, ",")
	}
	NUM_INSTANCES := 4 //total number of instances including the current instance
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
	self_address := "172.22.156.92:8080" // hard coded for vm1 instance which is 2801
	// Start instances and collect their command and stdin references
	self_name := "vm1"
	self_port := "8080"
	//declare autoAddresses as empty string list
	autoAddresses := []string{}
	for i := 0; i < NUM_INSTANCES; i++ { //+1 because we are not considering self_address
		if(auto_addresses[i] != self_address){
			autoAddresses = append(autoAddresses, auto_addresses[i])
		}
	}

	cmd, stdin, err:= functions_utility.StartInstance(self_port, self_name, autoAddresses)
	if err != nil {
		t.Fatalf("Error starting instance %s: %v", self_name, err)
	}
	stdin.Write([]byte("CONN AUTO\n")) //connected to all the other instances 
	time.Sleep(2 * time.Second)

	
	// Send the "LOG" command to the first instance
	custom_gen_pattern:= custom_patterns[0] 
	err_sent := functions_utility.SendCommand(stdin, "TEST FILE "+custom_filename+" "+custom_gen_pattern)
	if err_sent != nil {
		t.Errorf("Error sending LOG command: %v", err)
	} else {
		fmt.Println("Sent Test file generation command for " + custom_filename)
	}
	// Wait for a while to allow processes to run and communicate
	time.Sleep(2 * time.Second)
	for _,custom_pattern := range custom_patterns{
			// Send the grep command to each sender now
			command := fmt.Sprintf("grep '%s' <fnactual %s>", custom_pattern, custom_filename)
			err_2 := functions_utility.SendCommand(stdin, command)
			if err_2 != nil {
				t.Errorf("Error sending GREP command: %v", err_2)
			} else {
				fmt.Println("Sent GREP command")
			}
			time.Sleep(2 * time.Second)
	}
	time.Sleep(2 * time.Second)

	// Gracefully terminate instance by sending the "EXIT" command
	err_exit := functions_utility.SendExitCommand(stdin)
	if err_exit != nil {
		t.Errorf("Error sending EXIT command: %v", err)
	} else {
		fmt.Println("Sent EXIT command")
	}
	
	// Wait for processes to exit cleanly
	err_cmd := cmd.Wait()
	if err_cmd != nil {
		t.Errorf("Error waiting for process with PID %d to exit: %v", cmd.Process.Pid, err)
	} else {
		fmt.Printf("Process with PID %d exited cleanly\n", cmd.Process.Pid)
	}
	
}
