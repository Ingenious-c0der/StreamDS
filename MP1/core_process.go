package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
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

func printPeerList( peers *map[string]net.Conn) {
	counter := 0
	for name, conn := range *peers {
		conn_addr := conn.RemoteAddr().String()
		fmt.Fprintf(os.Stdout, "Peer %d : %s : %s\n", counter, name, conn_addr)
		counter++
	}
}

func runGREPLocal(pattern string) string{
	return "output"
}



func update_grep_accumulator(accu_mu *sync.Mutex, accumulator *map[string]string, name string, result string){
	defer accu_mu.Unlock()
	accu_mu.Lock()
	(*accumulator)[name] = result 
}


func printGREPResults(accu_results map[string]string){
	for name, result := range accu_results{
		fmt.Println(name, result)
	}
}

func handleConnection(conn net.Conn, self_name string, peers *map[string]net.Conn, wg *sync.WaitGroup) {
	defer wg.Done()
	defer conn.Close()
	var grep_result_accumulator map[string]string
	var mu sync.Mutex
	var accu_mu sync.Mutex 
	
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		msg := scanner.Text()
		fmt.Printf("Received: %s\n", msg)
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
		if strings.Contains(msg, "GREP PAT"){
			tokens:= strings.Split(msg, " ")
			pattern := tokens[2]
			result:= runGREPLocal(pattern)
			result = "GREP RET "+ self_name + " "+ result 
			communicateToPeer(conn, result)

			//invoke grep function here to search the file
			//return the matches
		}
		if strings.Contains(msg, "GREP RET"){
			tokens:= strings.Split(msg, " ")
			name:= tokens[2]
			result:= tokens[3]
			update_grep_accumulator(&accu_mu, &grep_result_accumulator, name , result)
			if len(grep_result_accumulator) == len(*peers)-1 {
				//results from all other peers have accumulated successfully
				printGREPResults(grep_result_accumulator)
				//clear the accumulator
				grep_result_accumulator = make(map[string]string)
				
			}
			
		}
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

//currently used for GREP broadcast
func multiCastMessageToPeers(peers *map[string]net.Conn, message string){
	for _, conn := range *peers{
		communicateToPeer(conn, message)
	}
}

func connectToPeer(address string, self_name string, wg *sync.WaitGroup, peerList *map[string]net.Conn) (bool, net.Conn) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Printf("Error connecting to %s: %v\n", address, err)
		return false, conn
	}

	sendNameToPeer(self_name, conn)
	wg.Add(1)
	go handleConnection(conn,self_name, peerList, wg)
	return true, conn
}

func setupCommTerminal(self_name string, peers *map[string]net.Conn, wg *sync.WaitGroup) {
	defer wg.Done()
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println(">>> ")
		message, _ := reader.ReadString('\n')
		if strings.Contains(message, "CONN INIT") {
			// CONN INIT <peer_name> <tid?>
			tokens := strings.Fields(message)
			if succ, conn := connectToPeer(tokens[2], self_name, wg, peers); succ {
				fmt.Printf("Connected to %s - %s successfully", conn.RemoteAddr().String(), tokens[2])
			} else {
				fmt.Printf("Failed to connect to %s", tokens[2])
			}

		}else if strings.Contains(message, "PRNT PLIST"){
			printPeerList(peers)
		}else if strings.Contains(message, "GREP"){
			multiCastMessageToPeers(peers, message)
		}
		// else if strings.Contains(message, "EXIT"){
		// 	fmt.Print("Closing current client")
		// 	//close connections here
		// }

	}
}

//the server component for the symmetric client, listens on the port for incoming connections
func listenOnNetwork(port string, self_name string, peers *map[string]net.Conn, wg *sync.WaitGroup) {
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
			go handleConnection(conn, self_name, peers, wg)
		} else {
			fmt.Println("Failed to exchange names, won't try connection further")
		}

	}
}

func main() {

	var wg sync.WaitGroup
	peers := make(map[string]net.Conn)
	fmt.Print("Enter the machine name : ")
	var name string
	fmt.Scan(&name)
	fmt.Print("Enter the machine address(port) : ")
	var port string
	fmt.Scan(&port)
	wg.Add(2)
	go listenOnNetwork(port, name, &peers, &wg)
	fmt.Println("here")
	go setupCommTerminal(name, &peers, &wg)
	// Wait for all connections to finish
	wg.Wait()

}
