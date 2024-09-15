package main
//this test simply spawns NUM_INSTANCES instances and sends 
//the grep command "grep 'DELETE'" to each instance, from each instance
import (
	"fmt"
	"time"
	"testing"
	"distributed_log_querier/functions_utility"
)
func TestDistributedSpawnerFunction(t *testing.T){
	testDistributedSpawn(t)
}
func testDistributedSpawn(t *testing.T) {
	// Define the base port and auto addresses for 10 instances
	
	NUM_INSTANCES:=10
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


	// Wait for a while to allow processes to run and communicate
	time.Sleep(2 * time.Second)
	err_send := functions_utility.SendCommand(stdin, "grep 'http' -i")
	if err_send != nil {
		fmt.Printf("Error sending GREP command to process: %v\n", err)
	} else {
		fmt.Println("Sent GREP command")
	}

	// Wait for a while to allow processes to run and communicate
	time.Sleep(3 * time.Second)
	// Gracefully terminate instance by sending the "EXIT" command
	
	err_exit := functions_utility.SendExitCommand(stdin)
	if err_exit != nil {
		fmt.Printf("Error sending EXIT command: %v\n", err)
	} else {
		fmt.Println("Sent EXIT command")
	}
	

	// Wait for  processe to exit cleanly
	err_cmd := cmd.Wait()
	if err_cmd != nil {
		fmt.Printf("Error waiting for process with PID %d to exit: %v\n", cmd.Process.Pid, err)
	} else {
		fmt.Printf("Process with PID %d exited cleanly\n", cmd.Process.Pid)
	}

}
