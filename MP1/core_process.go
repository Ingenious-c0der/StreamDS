package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

func grepMain(machine_file_name string, pattern string) string {
	pattern = pattern + " " + machine_file_name
	pattern = strings.ReplaceAll(pattern, "\n", "")
	cmd := exec.Command("bash", "-c", pattern)
	op, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Println("Error in grep util ->", err)
	}
	//convert bytes to string
	return string(op)
}

// PROTO CONN EXCG <name>
func sendNameToPeer(self_name string, conn net.Conn) {
	communicateToPeer(conn, fmt.Sprint("CONN PEXCG ", self_name))
	// P EXCG says its an handshake initiator, hence the remote client should return its name with REXCG
}

func updatePeerList(mu *sync.Mutex, peers *map[string]net.Conn, name string, conn net.Conn) {
	mu.Lock()
	defer mu.Unlock()
	(*peers)[name] = conn
}

func printPeerList(peers *map[string]net.Conn) {
	counter := 0
	for name, conn := range *peers {
		conn_addr := conn.RemoteAddr().String()
		fmt.Fprintf(os.Stdout, "Peer %d : %s : %s\n", counter, name, conn_addr)
		counter++
	}
}

func runGREPLocal(self_name string, pattern string) string {
	machine_file_name := self_name + ".log"
	output := grepMain(machine_file_name, pattern)
	return output
}

func update_grep_accumulator(accu_mu *sync.Mutex, accumulator *map[string]string, name string, result string) {
	defer accu_mu.Unlock()
	accu_mu.Lock()
	(*accumulator)[name] = result
}

func printGREPResults(accu_results map[string]string) {
	for name, result := range accu_results {
		fmt.Println(name, result)
	}
}

func handleConnection(conn net.Conn, self_name string, pattern *string, grep_result_accumulator *map[string]string, peers *map[string]net.Conn, alive_peers *map[string]net.Conn, wg *sync.WaitGroup) {
	defer wg.Done()
	defer conn.Close()

	var mu sync.Mutex
	var accu_mu sync.Mutex
	var alive_mu sync.Mutex
	var yam_u sync.Mutex

	//TODO: create alive ack system
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		msg := scanner.Text()
		fmt.Printf("Received: %s\n", msg)
		go func(msg string) {
			if strings.Contains(msg, "CONN REXCG") {
				tokens := strings.Split(msg, " ")
				name := tokens[2]
				updatePeerList(&mu, peers, name, conn)
			}
			if strings.Contains(msg, "CONN PEXCG") {
				tokens := strings.Split(msg, " ")
				name := tokens[2]
				updatePeerList(&mu, peers, name, conn)
			}
			if strings.Contains(msg, "GREP PAT") {
				tokens := strings.Split(msg, " ")
				*pattern = (strings.Join(tokens[2:], " "))
				result := runGREPLocal(self_name, *pattern)
				result = "GREP RET " + self_name + " " + result
				communicateToPeer(conn, result)
				//invoke grep function here to search the file
				//return the matches
			}
			if strings.Contains(msg, "GREP RET") {
				tokens := strings.Split(msg, " ")

				name := tokens[2]
				var result string
				if len(tokens) > 3 {
					result = strings.Join(tokens[3:], " ")
				} else {
					result = " empty response from machine"
				}
				yam_u.Lock()
				update_grep_accumulator(&accu_mu, grep_result_accumulator, name, result)
				yam_u.Unlock()
				fmt.Println(len(*grep_result_accumulator), len(*alive_peers), "len check")
				if len(*grep_result_accumulator) == len(*alive_peers) {
					//results from all other peers have accumulated successfully
					fmt.Println("All results accumulated", *pattern)
					(*grep_result_accumulator)[self_name] = runGREPLocal(self_name, *pattern)
					printGREPResults(*grep_result_accumulator)
					//clear the accumulator for the next run
					accu_mu.Lock()
					*grep_result_accumulator = (make(map[string]string))
					accu_mu.Unlock()
				}
			}
			if strings.Contains(msg, "CONN AACK") {
				tokens := strings.Split(msg, " ")
				name := tokens[2]
				//updating alive peers who acknowledged
				updatePeerList(&alive_mu, alive_peers, name, conn)

			}
			if strings.Contains(msg, "CONN ALIVE") {
				msg := fmt.Sprint("CONN AACK ", self_name)
				communicateToPeer(conn, msg)
			}
			if strings.Contains(msg, "PRNT APLIST") {
				printPeerList(alive_peers)
			}
		}(msg)
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading from connection: %v\n", err)
	}
	fmt.Print("connection closed unexpectedly ")
}

func communicateToPeer(peer net.Conn, message ...string) bool {
	var error_message string
	var message_actual string
	if len(message) == 2 {
		error_message = message[1]
		message_actual = message[0]
	} else {
		error_message = "Error Sending message: %v \n"
		message_actual = message[0]
	}
	message_actual = strings.TrimSpace(message_actual)
	_, err := peer.Write([]byte(message_actual + "\n"))
	if err != nil {
		fmt.Fprint(os.Stderr, error_message, err)
		return false
	}
	return true
}

// currently used for GREP broadcast
func multiCastMessageToPeers(peers *map[string]net.Conn, message string) {
	fmt.Println("Multicasting message to alive peers", len(*peers))

	for _, conn := range *peers {
		communicateToPeer(conn, message)
	}
}

func connectToPeer(pattern *string, address string, self_name string, grep_result_accumulator *map[string]string, wg *sync.WaitGroup, peerList *map[string]net.Conn, alive_peers *map[string]net.Conn) (bool, net.Conn) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Printf("Error connecting to %s: %v\n", address, err)
		return false, conn
	}

	sendNameToPeer(self_name, conn)
	wg.Add(1)
	go handleConnection(conn, self_name, pattern, grep_result_accumulator, peerList, alive_peers, wg)
	return true, conn
}

func setupCommTerminal(pattern *string, self_name string, auto_addresses []string, grep_result_accumulator *map[string]string, peers *map[string]net.Conn, alive_peers *map[string]net.Conn, wg *sync.WaitGroup) {
	defer wg.Done()
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println(">>> ")
		message, _ := reader.ReadString('\n')
		if strings.Contains(message, "CONN INIT") {
			// CONN INIT <peer_name> <tid?>
			tokens := strings.Fields(message)
			if succ, conn := connectToPeer(pattern, tokens[2], self_name, grep_result_accumulator, wg, peers, alive_peers); succ {
				fmt.Printf("Connected to %s - %s successfully", conn.RemoteAddr().String(), tokens[2])
			} else {
				fmt.Printf("Failed to connect to %s", tokens[2])
			}

		} else if strings.Contains(message, "PRNT PLIST") {
			printPeerList(peers)
		} else if strings.Contains(message, "GREP PAT") {
			//store grep pattern for local search
			//clear alive_peers list
			tokens := strings.Split(message, " ")
			*pattern = (strings.Join(tokens[2:], " "))
			*alive_peers = make(map[string]net.Conn)
			//checking which peers are alive at the moment
			fmt.Print("Checking for alive peers")
			for _, conn := range *peers {
				msg := fmt.Sprint("CONN ALIVE ", self_name)
				communicateToPeer(conn, msg)
			}
			//send message only to peers who acknowledged in above alive check
			//TODO work around the delay added here
			//add a delay here to wait for all peers to respond
			time.Sleep(2 * time.Second)

			multiCastMessageToPeers(alive_peers, message)
		} else if strings.Contains(message, "CONN AUTO") {
			//use the auto_addresses to connect to peers
			for _, address := range auto_addresses {
				if succ, conn := connectToPeer(pattern, address, self_name, grep_result_accumulator, wg, peers, alive_peers); succ {
					fmt.Printf("Connected to %s - %s successfully", conn.RemoteAddr().String(), address)
				} else {
					fmt.Printf("Failed to connect to %s", address)
				}
			}
		} else if strings.Contains(message, "EXIT") {
			fmt.Println("Exiting the program")
			//close connections here
			for _, conn := range *peers {
				conn.Close()
			}
			os.Exit(0)
		}
	}
}

// the server component for the symmetric client, listens on the port for incoming connections
func listenOnNetwork(pattern *string, port string, self_name string, grep_result_accumulator *map[string]string, peers *map[string]net.Conn, alive_peers *map[string]net.Conn, wg *sync.WaitGroup) {
	listener, err := net.Listen("tcp", ":"+port)
	information_string := fmt.Sprint("CONN REXCG ", self_name)
	if err != nil {
		fmt.Println("Error starting listener:", err)
		os.Exit(1)
	}
	defer listener.Close()
	fmt.Println("Listening on port", listener.Addr().String())
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		fmt.Println("Connection Accepted")

		succ := communicateToPeer(conn, information_string, "Error occured during exchanging names")
		if succ {
			fmt.Println("Finalizing connection...")
			wg.Add(1)
			go handleConnection(conn, self_name, pattern, grep_result_accumulator, peers, alive_peers, wg)
		} else {
			fmt.Println("Failed to exchange names, won't try connection further")
		}

	}
}

func main() {

	var wg sync.WaitGroup
	var pattern string
	peers := make(map[string]net.Conn)
	alive_peers := make(map[string]net.Conn)
	grep_result_accumulator := make(map[string]string)
	
	fmt.Print("Enter the machine name : ")
	var name string
	fmt.Scan(&name)
	fmt.Print("Enter the machine address(port) : ")
	var port string
	fmt.Scan(&port)
	reader := bufio.NewReader(os.Stdin)

	// Prompt the user for input
	fmt.Println("Enter auto addresses separated by spaces:")

	// Read a full line of input
	input, _ := reader.ReadString('\n')

	// Trim any trailing newline characters
	input = strings.TrimSpace(input)
	// output:= grepMain(name+".log", input)
	// fmt.Println(string(output))
	// Split the input by spaces into a slice of strings
	auto_addresses := strings.Fields(input)
	for i, address := range auto_addresses {
		auto_addresses[i] = strings.TrimSpace("[::]:" + address)
	}
	fmt.Println("Auto addresses", auto_addresses)
	wg.Add(2)
	go listenOnNetwork(&pattern, port, name, &grep_result_accumulator, &peers, &alive_peers, &wg)
	go setupCommTerminal(&pattern, name, auto_addresses, &grep_result_accumulator, &peers, &alive_peers, &wg)
	// Wait for all connections to finish
	wg.Wait()

}
