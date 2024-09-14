package main

import (
	"distributed_log_querier/functions_utility"
	"fmt"
	"io"
	"os/exec"
	"testing"
	"time"
)

func TestGenerateLogsAndVerify(t *testing.T) {
	generate_logs_and_verify(t)
}

func generate_logs_and_verify(t *testing.T) {
	var instances []*exec.Cmd
	var stdins []io.WriteCloser
	custom_filename := "test_run.log"
	custom_pattern := "NEWLINE"
	NUM_INSTANCES := 10
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
	self_address := "172.22.156.92:8080"
	// Start instances and collect their command and stdin references
	for i := 1; i <= NUM_INSTANCES; i++ {
		var autoAddresses []string
		port := fmt.Sprintf(auto_addresses[i-1])
		name := fmt.Sprintf("vm%d", i)
		// Create instances and connect them
		for j := 1; j <= NUM_INSTANCES; j++ {
			if self_address != auto_addresses[j-1] {
				autoAddresses = append(autoAddresses, auto_addresses[j-1])
			}
		}
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
	time.Sleep(2 * time.Second)
	//now the connected system is ready

	sender := stdins[0]
	// Send the "LOG" command to the first instance
	err := functions_utility.SendCommand(sender, "TEST FILE "+custom_filename+" "+custom_pattern)
	if err != nil {
		t.Errorf("Error sending LOG command: %v", err)
	} else {
		fmt.Println("Sent Test file generation command for " + custom_filename)
	}
	// Wait for a while to allow processes to run and communicate
	time.Sleep(2 * time.Second)

	// Send the grep command to each sender now
	command := fmt.Sprintf("grep '%s' <fnactual %s>", custom_pattern, custom_filename)
	err_2 := functions_utility.SendCommand(sender, command)
	if err_2 != nil {
		t.Errorf("Error sending GREP command: %v", err_2)
	} else {
		fmt.Println("Sent GREP command")
	}
	// Wait for a while to allow processes to run commands and communicate
	time.Sleep(5 * time.Second)

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
