package distributed_log_querier

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
	"strconv"
	"runtime"
	"path/filepath"
	"regexp"
)



func extractFilename(s string) (filename string, cleanStr string) {
	// Compile a regular expression to match the pattern starting with <fnactual
	re := regexp.MustCompile(`<fnactual\s+([^>]+)>`)

	// Extract the filename from the pattern
	match := re.FindStringSubmatch(s)
	if len(match) > 1 {
		// The first submatch contains the filename
		filename = match[1]
		// Remove the entire substring including <fnactual ...>
		cleanStr = re.ReplaceAllString(s, "")
	}
	return
}


func grepMain(machine_file_name string, pattern string) string {
	// Get the directory of the current Go file
	_, currentFile, _, _ := runtime.Caller(0)
	dir := filepath.Dir(currentFile)
	dir = filepath.Dir(dir)
	//special provision fnactual mainly for test suite to pass the actual filename instead of the 
	//default machine_file_name, but can be used in general as well
	if(strings.Contains(pattern, "fnactual")){
		machine_file_name, pattern = extractFilename(pattern)
	}
	if(strings.Contains(pattern, "<filename>")){
		pattern = strings.ReplaceAll(pattern, "<filename>", machine_file_name)
	}else{
		pattern = pattern + " " + machine_file_name
	}
	
	pattern = strings.ReplaceAll(pattern, "\n", "")
	fmt.Println("Searching pattern ->", pattern)
	cmd := exec.Command("bash", "-c", pattern)
	cmd.Dir = dir
	op, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Println("Error in grep util, maybe the pattern was malformed ->", err)
	}
	// Convert bytes to string
	return string(op)
}

// PROTO CONN EXCG <name>
func sendNameToPeer(self_name string, conn net.Conn) {
	communicateToPeer(conn, fmt.Sprint("CONN PEXCG ", self_name))
	// P EXCG says its a handshake initiator, hence the remote client should return its name with REXCG
}

func updatePeerList(mu *sync.Mutex, peers *sync.Map, name string, conn net.Conn) {
	mu.Lock()
	defer mu.Unlock()
	peers.Store(name, conn)
}

func printPeerList(peers *sync.Map) {
	var counter int
	peers.Range(func(key, value interface{}) bool {
		name := key.(string)
		conn := value.(net.Conn)
		conn_addr := conn.RemoteAddr().String()
		fmt.Fprintf(os.Stdout, "Peer %d : %s : %s\n", counter, name, conn_addr)
		counter++
		return true
	})
}

func runGREPLocal(self_name string, pattern string) string {
	machine_file_name := self_name + ".log"
	output := grepMain(machine_file_name, pattern)
	return output
}

func update_grep_accumulator(accu_mu *sync.Mutex, accumulator *sync.Map, name string, result string) {
	accu_mu.Lock()
	defer accu_mu.Unlock()
	accumulator.Store(name, result)
}

func printGREPResults(accu_results *sync.Map) {
	accu_results.Range(func(key, value interface{}) bool {
		name := key.(string)
		result := value.(string)
		fmt.Println(name, ": Lines Matched : ",result)
		return true
	})
}
func getSyncMapLength(m *sync.Map) int {
	count := 0
	m.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}

func readMultilineMessage(reader *bufio.Reader, delimiter string) (string, error) {
    var result strings.Builder
    for {
        line, err := reader.ReadString('\n')  // Read line by line
        if err != nil {
            return "", err  // Return error if reading fails
        }
        result.WriteString(line)
        if strings.Contains(line, delimiter) {  // Check for delimiter
            break  // Stop reading when delimiter is found
        }
    }
    // Return message with the delimiter removed
    return strings.Replace(result.String(), delimiter, "", -1), nil
}
// YAPS : yet another param for sync
func handleConnection(conn net.Conn, self_name string, pattern *string, latencyStart *time.Time, grep_result_accumulator *sync.Map, peers *sync.Map, alive_peers *sync.Map, wg *sync.WaitGroup) {
	defer wg.Done()
	defer conn.Close()

	var mu sync.Mutex
	var accu_mu sync.Mutex
	var alive_mu sync.Mutex
	var yam_u sync.Mutex

	reader := bufio.NewReader(conn)
	for {
		msg, err := readMultilineMessage(reader, "END_OF_MESSAGE")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading from connection: %v\n", err)
			return
		}
		msg = strings.TrimRight(msg, "\n")
		go func(msg string) {
			if strings.Contains(msg, "CONN REXCG") {
				fmt.Printf("Received name from peer: %s\n", msg)
				tokens := strings.Split(msg, " ")
				name := tokens[2]
				updatePeerList(&mu, peers, name, conn)
			}
			if strings.Contains(msg, "CONN PEXCG") {
				fmt.Printf("Received name from peer: %s\n", msg)
				tokens := strings.Split(msg, " ")
				name := tokens[2]
				updatePeerList(&mu, peers, name, conn)
			}
			if strings.Contains(msg, "GREP PAT") {
				tokens := strings.Split(msg, " ")
				*pattern = (strings.Join(tokens[2:], " "))
				// Invoke grep function here to search the file
				// Return the matches to the peer
				result := runGREPLocal(self_name, *pattern)
				result = "GREP RET " + self_name + " " + result
				communicateToPeer(conn, result)
				
			}
			if strings.Contains(msg, "GREP RET") {
				tokens := strings.Split(msg, " ")
				//fmt.Println("Following GREP len tokens received :", len(tokens))
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
				fmt.Println(getSyncMapLength(grep_result_accumulator), getSyncMapLength(alive_peers), "len check")
				if getSyncMapLength(grep_result_accumulator) == getSyncMapLength(alive_peers) {

					// Results from all other peers have accumulated successfully
					latencyEnd := time.Now()
					//latency in milliseconds
					latency:= latencyEnd.Sub(*latencyStart).Milliseconds()
					latencyStr:=strconv.FormatInt(latency,10)
					//convert to string

					fmt.Println("Latency for the GREP search: (ms) ", latency)
					fmt.Println("All results accumulated for ", *pattern)
					result := runGREPLocal(self_name, *pattern)
					grep_result_accumulator.Store(self_name, result)
					// Print the GREP results
					// Store the results in a file with file name self_name.txt
					storage_file := self_name + ".txt"
					_, currentFile, _, _ := runtime.Caller(0)
					dir := filepath.Dir(currentFile)
				
					// Construct the full path for the storage file
					fullPath := filepath.Join(dir, storage_file)
					file, err := os.Create(fullPath)
					if err != nil {
						fmt.Println("Error creating file: ", err)
					}
					file.Write([]byte("Results for pattern: " + *pattern + "\n" + "Latency : " + latencyStr +"ms"+ "\n\n"))
					grep_result_accumulator.Range (func(key, value interface{}) bool {
						name := key.(string)
						result := value.(string)
						num_matches := strings.Count(result, "\n") +1 // the last line does not have a newline by default
						if !strings.Contains(*pattern, "-c") {
						fmt.Println(name, " : Lines Matched : ", num_matches)
						file.Write([]byte(name + " : Lines Matched : "+ strconv.Itoa(num_matches)+ "\n" + result + "\n\n"))
						}else{
							printGREPResults(grep_result_accumulator)
							//since the pattern is -c, we will already get the aggregated count
							file.Write([]byte(name + " : Lines Matched : "+ result + "\n"))
						}
						return true
					})
					//printGREPResults(grep_result_accumulator)
					file.Close()
					// Clear the accumulator for the next run
					accu_mu.Lock()
					*grep_result_accumulator = sync.Map{}
					accu_mu.Unlock()
				}
			}
			if strings.Contains(msg, "CONN AACK") {
				tokens := strings.Split(msg, " ")
				name := tokens[2]
				// Updating alive peers who acknowledged
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
	_, err := peer.Write([]byte(message_actual + "END_OF_MESSAGE\n"))
	if err != nil {
		fmt.Fprint(os.Stderr, error_message, err)
		return false
	}
	return true
}

// Currently used for GREP broadcast
func multiCastMessageToPeers(peers *sync.Map, message string) {
	fmt.Println("Multicasting message to alive peers", peersCount(peers))

	peers.Range(func(key, value interface{}) bool {
		conn := value.(net.Conn)
		communicateToPeer(conn, message)
		return true
	})
}

func peersCount(peers *sync.Map) int {
	count := 0
	peers.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

func connectToPeer(pattern *string, address string, self_name string, latencyStart *time.Time, grep_result_accumulator *sync.Map, wg *sync.WaitGroup, peerList *sync.Map, alive_peers *sync.Map) (bool, net.Conn) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Printf("Error connecting to %s: %v\n", address, err)
		return false, conn
	}

	sendNameToPeer(self_name, conn)
	wg.Add(1)
	go handleConnection(conn, self_name, pattern,latencyStart, grep_result_accumulator, peerList, alive_peers, wg)
	return true, conn
}

func SetupCommTerminal(pattern *string, self_name string, auto_addresses []string, latencyStart *time.Time,grep_result_accumulator *sync.Map, peers *sync.Map, alive_peers *sync.Map, wg *sync.WaitGroup) {
	defer wg.Done()
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println(">>> ")
		message, _ := reader.ReadString('\n')
		if strings.Contains(message, "CONN INIT") {
			// CONN INIT <peer_name> <tid?>
			tokens := strings.Fields(message)
			if succ, conn := connectToPeer(pattern, tokens[2], self_name,latencyStart, grep_result_accumulator, wg, peers, alive_peers); succ {
				fmt.Printf("Connected to %s - %s successfully\n", conn.RemoteAddr().String(), tokens[2])
			} else {
				fmt.Printf("Failed to connect to %s", tokens[2])
			}

		} else if strings.Contains(message, "PRNT PLIST") {
			printPeerList(peers)
		} else if strings.Contains(message, "grep") {
			// Store grep pattern for local search
			// Clear alive_peers list
			//tokens := strings.Split(message, " ")
			//*pattern = (strings.Join(tokens[2:], " "))
			*pattern = message
			message = fmt.Sprint("GREP PAT ", *pattern)
			*alive_peers = sync.Map{}
			// Checking which peers are alive at the moment
			fmt.Println("Finding alive peers")
			peers.Range(func(key, value interface{}) bool {
				conn := value.(net.Conn)
				msg := fmt.Sprint("CONN ALIVE ", self_name)
				communicateToPeer(conn, msg)
				return true
			})
			// Send message only to peers who acknowledged in the above alive check
			// TODO: work around the delay added here
			// Add a delay here to wait for all peers to respond
			time.Sleep(2 * time.Second)

			multiCastMessageToPeers(alive_peers, message)
			*latencyStart = time.Now();
		} else if strings.Contains(message, "CONN AUTO") {
			// Use the auto_addresses to connect to peers
			for _, address := range auto_addresses {
				if succ, conn := connectToPeer(pattern, address, self_name,latencyStart, grep_result_accumulator, wg, peers, alive_peers); succ {
					fmt.Printf("Connected to %s - %s successfully\n", conn.RemoteAddr().String(), address)
				} else {
					fmt.Printf("Failed to connect to %s", address)
				}
			}
		} else if strings.Contains(message, "EXIT") {
			fmt.Println("Exiting the program")
			// Close connections here
			peers.Range(func(key, value interface{}) bool {
				conn := value.(net.Conn)
				conn.Close()
				return true
			})
			os.Exit(0)
		}
	}
}

// The server component for the symmetric client, listens on the port for incoming connections
func ListenOnNetwork(pattern *string, port string, self_name string, latencyStart *time.Time, grep_result_accumulator *sync.Map, peers *sync.Map, alive_peers *sync.Map, wg *sync.WaitGroup) {
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

		succ := communicateToPeer(conn, information_string, "Error occurred during exchanging names")
		if succ {
			fmt.Println("Finalizing connection...")
			wg.Add(1)
			go handleConnection(conn, self_name, pattern,latencyStart, grep_result_accumulator, peers, alive_peers, wg)
		} else {
			fmt.Println("Failed to exchange names, won't try connection further")
		}
	}
}
func main() {

	var wg sync.WaitGroup
	var pattern string
	
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
	autoAddresses := strings.Fields(input)
	for i, address := range autoAddresses {
		autoAddresses[i] = strings.TrimSpace(address)
	}
	fmt.Println("Auto addresses", autoAddresses)
	peers := sync.Map{}
	alive_peers := sync.Map{}
	grep_result_accumulator := sync.Map{}
	var latencyStart time.Time; 

	fmt.Printf("Starting instance %s on port %s\n", name, port)
	wg.Add(2)
	go ListenOnNetwork(&pattern, port, name,&latencyStart, &grep_result_accumulator, &peers, &alive_peers, &wg)
	go SetupCommTerminal(&pattern, name, autoAddresses,&latencyStart, &grep_result_accumulator, &peers, &alive_peers, &wg)

	wg.Wait()

}