package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	//"regexp"
)

//currently using MAP directly instead map[string]
// type MachineCliConn struct {
// 	Name string
// 	Conn net.Conn
// }

// func NewMachineCliConn(name string, conn net.Conn ) * MachineCliConn{
// 	return &MachineCliConn{
// 		Name: name ,
// 		Conn : conn ,
// 	}
// }

//func handleMessage(message string)

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

func handleConnection(conn net.Conn, peers *map[string]net.Conn, wg *sync.WaitGroup) {
	defer wg.Done()
	defer conn.Close()
	var mu sync.Mutex
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

func connectToPeer(address string, self_name string, wg *sync.WaitGroup, peerList *map[string]net.Conn) (bool, net.Conn) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Printf("Error connecting to %s: %v\n", address, err)
		return false, conn
	}

	sendNameToPeer(self_name, conn)
	wg.Add(1)
	go handleConnection(conn, peerList, wg)
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
		}

		// else if strings.Contains(message, "GREP"){

		// }
		// else if strings.Contains(message, "EXIT"){
		// 	fmt.Print("Closing current client")
		// 	//close connections here
		// }

	}
}

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

		succ := communicateToPeer(conn, information_string, "Error during exchanging names")
		if succ {
			fmt.Println("finalizing connection")
			wg.Add(1)
			go handleConnection(conn, peers, wg)
		} else {
			fmt.Println("failed to exchange names, won't connect further")
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
