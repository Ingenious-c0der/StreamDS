package main

import (
	"fmt"
	"os/exec"
)

func grepMain() {
	// filepath to find if file present
	file_name := "../todos.txt"
	pattern := "grep -n 'query patterns' " + file_name
	cmd := exec.Command("bash", "-c", pattern)
	op, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Println("Error ->", err)
		// fmt.Println(string(op))
	}
	fmt.Println(string(op))
	//
}
