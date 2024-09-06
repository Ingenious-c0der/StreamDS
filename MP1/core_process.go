package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

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

func runGREPLocal(pattern string) string {
	return "output"
}

func update_grep_accumulator(accu_mu *sync.Mutex, accumulator *map[string]string, name string, result string) {
	defer accu_mu.Unlock()
	accu_mu.Lock()
	fmt.Println("Actually updating grep accumulator", len(*accumulator))
	(*accumulator)[name] = result
}

func printGREPResults(accu_results map[string]string) {
	for name, result := range accu_results {
		fmt.Println(name, result)
	}
}

func handleConnection(conn net.Conn, self_name string, grep_result_accumulator *map[string]string, peers *map[string]net.Conn, alive_peers *map[string]net.Conn, wg *sync.WaitGroup) {
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
				pattern := tokens[2]
				result := runGREPLocal(pattern)
				result = "GREP RET " + self_name + " " + result
				communicateToPeer(conn, result)
				//invoke grep function here to search the file
				//return the matches
			}
			if strings.Contains(msg, "GREP RET") {
				tokens := strings.Split(msg, " ")
				name := tokens[2]
				result := tokens[3]
				yam_u.Lock()
				update_grep_accumulator(&accu_mu, grep_result_accumulator, name, result)
				yam_u.Unlock()
				fmt.Println(len(*grep_result_accumulator), len(*alive_peers), "len check")
				if len(*grep_result_accumulator) == len(*alive_peers) {
					//results from all other peers have accumulated successfully
					(*grep_result_accumulator)[self_name] = runGREPLocal("pattern")
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
				fmt.Print("updating peer in alive list for", name)

				updatePeerList(&alive_mu, alive_peers, name, conn)
				fmt.Printf("current alive peers len %v ", len(*alive_peers))
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

func connectToPeer(address string, self_name string, grep_result_accumulator *map[string]string, wg *sync.WaitGroup, peerList *map[string]net.Conn, alive_peers *map[string]net.Conn) (bool, net.Conn) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Printf("Error connecting to %s: %v\n", address, err)
		return false, conn
	}

	sendNameToPeer(self_name, conn)
	wg.Add(1)
	go handleConnection(conn, self_name, grep_result_accumulator, peerList, alive_peers, wg)
	return true, conn
}

func setupCommTerminal(self_name string, grep_result_accumulator *map[string]string, peers *map[string]net.Conn, alive_peers *map[string]net.Conn, wg *sync.WaitGroup) {
	defer wg.Done()
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println(">>> ")
		message, _ := reader.ReadString('\n')
		if strings.Contains(message, "CONN INIT") {
			// CONN INIT <peer_name> <tid?>
			tokens := strings.Fields(message)
			if succ, conn := connectToPeer(tokens[2], self_name, grep_result_accumulator, wg, peers, alive_peers); succ {
				fmt.Printf("Connected to %s - %s successfully", conn.RemoteAddr().String(), tokens[2])
			} else {
				fmt.Printf("Failed to connect to %s", tokens[2])
			}

		} else if strings.Contains(message, "PRNT PLIST") {
			printPeerList(peers)
		} else if strings.Contains(message, "GREP") {
			//clear alive_peers list
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
func listenOnNetwork(port string, self_name string, grep_result_accumulator *map[string]string, peers *map[string]net.Conn, alive_peers *map[string]net.Conn, wg *sync.WaitGroup) {
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
			go handleConnection(conn, self_name, grep_result_accumulator, peers, alive_peers, wg)
		} else {
			fmt.Println("Failed to exchange names, won't try connection further")
		}

	}
}

func main() {

	var wg sync.WaitGroup
	peers := make(map[string]net.Conn)
	alive_peers := make(map[string]net.Conn)
	grep_result_accumulator := make(map[string]string)
	fmt.Print("Enter the machine name : ")
	var name string
	fmt.Scan(&name)
	fmt.Print("Enter the machine address(port) : ")
	var port string
	fmt.Scan(&port)
	wg.Add(2)
	go listenOnNetwork(port, name, &grep_result_accumulator, &peers, &alive_peers, &wg)
	fmt.Println("here")
	go setupCommTerminal(name, &grep_result_accumulator, &peers, &alive_peers, &wg)
	// Wait for all connections to finish
	wg.Wait()

}
