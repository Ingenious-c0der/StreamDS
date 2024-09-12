package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
)

func startInstance(port string, name string, autoAddresses []string) (*exec.Cmd, io.WriteCloser, error) {
	// Construct the command to run the Go program
	cmd := exec.Command("go", "run", "concurrent_safe_core_process_auto.go")

	// Set environment variables to mock input data
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("SELF_NAME=%s", name),
		fmt.Sprintf("PORT=%s", port),
		fmt.Sprintf("AUTO_ADDRESSES=%s", strings.Join(autoAddresses, " ")),
	)

	// Connect stdout and stderr for logs
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Create a pipe to write to the process's stdin
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("error creating stdin pipe for %s: %v", name, err)
	}

	// Start the instance
	err = cmd.Start()
	if err != nil {
		return nil, nil, fmt.Errorf("error starting instance %s on port %s: %v", name, port, err)
	}

	fmt.Printf("Started instance %s on port %s with PID %d\n", name, port, cmd.Process.Pid)

	// Return both the command and stdin so we can send commands later
	return cmd, stdin, nil
}

func sendCommand(stdin io.WriteCloser, command string) error {
	_, err := stdin.Write([]byte(command + "\n"))
	if err != nil {
		return fmt.Errorf("error sending command: %v", err)
	}
	return nil
}

func sendExitCommand(stdin io.WriteCloser) error {
	_, err := stdin.Write([]byte("EXIT\n"))
	if err != nil {
		return fmt.Errorf("error sending EXIT command: %v", err)
	}
	return stdin.Close() // Close stdin after sending the command
}

func main() {
	// Define the base port and auto addresses for 10 instances
	basePort := 8080
	var instances []*exec.Cmd
	var stdins []io.WriteCloser
	
	NUM_INSTANCES:=9

	

	// Start instances and collect their command and stdin references
	for i := 1; i <= NUM_INSTANCES; i++ {
		var autoAddresses []string
		port := fmt.Sprintf("%d", basePort+i)
		name := fmt.Sprintf("vm%d", i)
		// Create 9 instances and connect them
		for j := 1; j <= NUM_INSTANCES; j++ {
			if(j != i){
			autoAddresses = append(autoAddresses, fmt.Sprintf("[::]:%d", basePort+j))
			}
		}
		cmd, stdin, err := startInstance(port, name, autoAddresses)
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
		err := sendCommand(stdin, "CONN AUTO")
		time.Sleep(2*time.Second)
		if err != nil {
			fmt.Printf("Error sending CONN AUTO to process: %v\n", err)
		} else {
			fmt.Println("Sent CONN AUTO command")
		}
	}

	time.Sleep(5 * time.Second)

	for _, stdin := range stdins {
		err := sendCommand(stdin, "GREP PAT grep '17/Aug'")
		time.Sleep(3*time.Second)
		if err != nil {
			fmt.Printf("Error sending GREP command to process: %v\n", err)
		} else {
			fmt.Println("Sent GREP command")
		}
	}
	// Wait for a while to allow processes to run and communicate
	time.Sleep(2 * time.Second)
	// Gracefully terminate instances by sending the "EXIT" command
	for _, stdin := range stdins {
		err := sendExitCommand(stdin)
		if err != nil {
			fmt.Printf("Error sending EXIT command: %v\n", err)
		} else {
			fmt.Println("Sent EXIT command")
		}
	}

	// Wait for all processes to exit cleanly
	for _, cmd := range instances {
		err := cmd.Wait()
		if err != nil {
			fmt.Printf("Error waiting for process with PID %d to exit: %v\n", cmd.Process.Pid, err)
		} else {
			fmt.Printf("Process with PID %d exited cleanly\n", cmd.Process.Pid)
		}
	}
}