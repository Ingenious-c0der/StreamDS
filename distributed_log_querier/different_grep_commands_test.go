package main

import (
	"fmt"
	"io"
	"os/exec"
	"testing"
	"time"
	"distributed_log_querier/functions_utility"
)

func TestDifferentGrepCommands(t *testing.T) {
	diff_grep_commands(t)
}

func diff_grep_commands(t *testing.T) {
	// Define the base port and auto addresses for 10 instances
	var instances []*exec.Cmd
	var stdins []io.WriteCloser

	NUM_INSTANCES := 5

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
	//self_address:= "172.22.156.92:8080"
	// Start instances and collect their command and stdin references
	for i := 1; i <= NUM_INSTANCES; i++ {
		var autoAddresses []string
		port := fmt.Sprintf(auto_addresses[i-1])
		name := fmt.Sprintf("vm%d", i)
		// Create instances and connect them
		for j := 1; j <= NUM_INSTANCES; j++ {
			if auto_addresses[i-1] != auto_addresses[j-1] {
				autoAddresses = append(autoAddresses, auto_addresses[j-1])
			}
		}
		//maybe need to actually call this from another VM. 
		cmd, stdin, err := functions_utility.StartInstance(port, name, autoAddresses)
		if err != nil {
			t.Fatalf("Error starting instance %s: %v", name, err)
		}
		instances = append(instances, cmd)
		stdins = append(stdins, stdin)
	}


	// Wait for a while to allow processes to run and communicate
	time.Sleep(5 * time.Second)

	// Send CONN AUTO command to all instances
	fmt.Println("Sending CONN AUTO command to all instances")
	for _, stdin := range stdins {
		err := functions_utility.SendCommand(stdin, "CONN AUTO")
		time.Sleep(2 * time.Second)
		if err != nil {
			t.Errorf("Error sending CONN AUTO to process: %v", err)
		} else {
			fmt.Println("Sent CONN AUTO command")
		}
	}

	time.Sleep(5 * time.Second)

	grep_patterns := []string{
		"grep 'DELETE'",
		"grep 'http'",
		"grep 'ERROR' | grep -v 'DEBUG'",
		"grep -E '\\b(1[0-9][0-9]|2[0-0][0-9]|[0-9]{1,2})\\.(0?[0-9]|[1-4][0-9]|50)\\.[0-9]{1,3}\\.[0-9]{1,3}\\b'",
	}

	selectedMachine := stdins[0]
	for _, pattern := range grep_patterns {
		err := functions_utility.SendCommand(selectedMachine, pattern)
		time.Sleep(3 * time.Second)
		if err != nil {
			t.Errorf("Error sending GREP command to process: %v", err)
		} else {
			fmt.Println("Sent GREP command")
		}
	}

	// Wait for a while to allow processes to run and communicate
	time.Sleep(2 * time.Second)

	// Gracefully terminate instances by sending the "EXIT" command
	for _, stdin := range stdins {
		err := functions_utility.SendExitCommand(stdin)
		if err != nil {
			t.Errorf("Error sending EXIT command: %v", err)
		} else {
			fmt.Println("Sent EXIT command")
		}
	}

	// Wait for all processes to exit cleanly
	for _, cmd := range instances {
		err := cmd.Wait()
		if err != nil {
			t.Errorf("Error waiting for process with PID %d to exit: %v", cmd.Process.Pid, err)
		} else {
			fmt.Printf("Process with PID %d exited cleanly\n", cmd.Process.Pid)
		}
	}
}