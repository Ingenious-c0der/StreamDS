package main

import (
	"distributed_log_querier/functions_utility"
	"fmt"
	"io"
	"os/exec"
	"testing"
	"time"
)

func TestDifferentGrepCommands(t *testing.T) {
	diff_grep_commands(t)
}

func diff_grep_commands(t *testing.T) {
	// Define the base port and auto addresses for 10 instances
	var instances []*exec.Cmd
	var stdins []io.WriteCloser
	basePort := 8080
	NUM_INSTANCES := 5
	// Start instances and collect their command and stdin references
	for i := 1; i <= NUM_INSTANCES; i++ {
		var autoAddresses []string
		port := fmt.Sprintf("%d", basePort+i)
		name := fmt.Sprintf("vm%d", i)
		// Create NUM instances and connect them
		for j := 1; j <= NUM_INSTANCES; j++ {
			if(j != i){
			autoAddresses = append(autoAddresses, fmt.Sprintf("[::]:%d", basePort+j))
			}
		}
		cmd, stdin, err := functions_utility.StartInstance(port, name, autoAddresses)
		if err != nil {
			fmt.Println(err)
			continue
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
