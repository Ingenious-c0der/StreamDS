package unit_tests
//this test simply spawns NUM_INSTANCES instances and sends 
//the grep command "grep 'DELETE'" to each instance, from each instance
import (
	"fmt"
	"io"
	"os/exec"
	"time"
	"testing"
	"unit_tests/functions_utility"
)
func TestSpawnerFunction(t *testing.T){
	main()
}
func main() {
	// Define the base port and auto addresses for 10 instances
	basePort := 8080
	var instances []*exec.Cmd
	var stdins []io.WriteCloser
	
	NUM_INSTANCES:=2

	

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
		time.Sleep(2*time.Second)
		if err != nil {
			fmt.Printf("Error sending CONN AUTO to process: %v\n", err)
		} else {
			fmt.Println("Sent CONN AUTO command")
		}
	}

	time.Sleep(5 * time.Second)

	for _, stdin := range stdins {
		err := functions_utility.SendCommand(stdin, "GREP PAT grep 'DELETE'")
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
		err := functions_utility.SendExitCommand(stdin)
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
