// package main

// import (
// 	"fmt"
// 	"os/exec"
// )

// func GrepMain(machine_file_name string, pattern string)   string {
	
// 	pattern = pattern + machine_file_name
// 	cmd := exec.Command("bash", "-c", pattern)
// 	op, err := cmd.CombinedOutput()
// 	if err != nil {
// 		fmt.Println("Error in grep util ->", err)
// 	}
// 	//convert bytes to string 
// 	return string(op)
// }
