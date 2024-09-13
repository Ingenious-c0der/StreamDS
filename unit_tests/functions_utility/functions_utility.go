package functions_utility

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

func StartInstance(port string, name string, autoAddresses []string) (*exec.Cmd, io.WriteCloser, error) {
	// Construct the command to run the Go program
	cmd := exec.Command("go", "run", "/Users/ingenious/Desktop/Code/CS-425-MP/MP1/core_process_auto.go")

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

func SendCommand(stdin io.WriteCloser, command string) error {
	_, err := stdin.Write([]byte(command + "\n"))
	if err != nil {
		return fmt.Errorf("error sending command: %v", err)
	}
	return nil
}

func SendExitCommand(stdin io.WriteCloser) error {
	_, err := stdin.Write([]byte("EXIT\n"))
	if err != nil {
		return fmt.Errorf("error sending EXIT command: %v", err)
	}
	return stdin.Close() // Close stdin after sending the command
}
