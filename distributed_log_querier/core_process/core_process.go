package distributed_log_querier

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

/*
Function to support extraction of custom filename if passed in the grep command
example usage : grep -c "PATTERN" <fnactual custom_filename.log> -> custom_filename.log
*/
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

/*
Function used to staisfy the unit test constraint of spawning a new test log file with some
known patterns and some random patterns. It generates a file with given filename and pattern
Currently it is hard coded for 10 pattern matches and 120 random text lines, this can be easily changed
if required. The log file will contiain 10 lines of pattern matches and other 120 lines of random text
*/
func generate_test_log_file(filename string, pattern string) {

	//following is done to get the directory of the current file
	//irrespective of the directory from which the go run command is run
	//for the file to be created in the same directory as the root dir.
	_, currentFile, _, _ := runtime.Caller(0)
	dir := filepath.Dir(currentFile)
	dir = filepath.Dir(dir)
	filename = filepath.Join(dir, filename)

	// Creating the file
	file, err := os.Create(filename)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close() // Closing the file when the function returns
	// write the pattern to the file (10 times)
	for i := 0; i < 10; i++ {
		_, err := file.WriteString(pattern + "\n")
		if err != nil {
			fmt.Println("Error writing to file:", err)
			return
		}
	}
	// write random text to the file (120 lines)
	//now this can be actual random text instead of "Random text"
	//but we want to avoid the edge case where the random text matches the pattern for
	//the sake of the test
	for i := 0; i < 120; i++ {
		_, err := file.WriteString("Random text\n")
		if err != nil {
			fmt.Println("Error writing to file:", err)
			return
		}
	}
}

/*
Function that actually runs the grep command on the current (local) machine
*/
func grepMain(machine_file_name string, pattern string) string {
	// Get the directory of the current Go file for the same reason as explained in
	// the function generate_test_log_file
	_, currentFile, _, _ := runtime.Caller(0)
	dir := filepath.Dir(currentFile)
	dir = filepath.Dir(dir)
	//special provision fnactual is mainly for test suite to pass the actual filename instead of the
	//default machine_file_name, but can be used in general as well
	if strings.Contains(pattern, "fnactual") {
		machine_file_name, pattern = extractFilename(pattern)
	}

	// incase you need to specify where the filename should be placed in the grep command, you can
	// use the <filename> tag in the pattern, and it will be replaced by the machine_file_name
	// if not present, the filename will be appended at the end of the pattern
	// mainly done to support pipe operators
	if strings.Contains(pattern, "<filename>") {
		pattern = strings.ReplaceAll(pattern, "<filename>", machine_file_name)
	} else {
		pattern = pattern + " " + machine_file_name
	}

	pattern = strings.ReplaceAll(pattern, "\n", "") // remove any newline characters
	fmt.Println("Searching pattern ->", pattern)
	cmd := exec.Command("bash", "-c", pattern)
	cmd.Dir = dir                   // Set the directory for the command
	op, err := cmd.CombinedOutput() // Run the command and get the output
	if err != nil {
		fmt.Println("Error in grep util, maybe the pattern was malformed ->", err)
	}
	// Convert bytes to string
	return string(op)
}

// PROTOCOL CONN <P/R>EXCG <name>
func sendNameToPeer(self_name string, conn net.Conn) {
	communicateToPeer(conn, fmt.Sprint("CONN PEXCG ", self_name))
	// P EXCG says its a name exchange initiator, (P for protagonist)
	// hence the remote client should return its name with REXCG (R for responder)
	// if this is not implemented there will be name exchange echos
}

/*
Function to update the peer list map with the new peer connection <name> : <conn>
*/
func updatePeerList(mu *sync.Mutex, peers *sync.Map, name string, conn net.Conn) {
	mu.Lock()
	defer mu.Unlock()
	peers.Store(name, conn)
}

// simple print functionality
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

// support function to call the grepMain function for local search
func runGREPLocal(self_name string, pattern string) string {
	machine_file_name := self_name + ".log"
	output := grepMain(machine_file_name, pattern)
	return output
}

// function to update the grep accumulator map with the results from the peers <name> : <result>
func update_grep_accumulator(accu_mu *sync.Mutex, accumulator *sync.Map, name string, result string) {
	accu_mu.Lock()
	defer accu_mu.Unlock()
	accumulator.Store(name, result)
}

// function to print the grep results from the accumulator
func printGREPResults(accu_results *sync.Map) {
	accu_results.Range(func(key, value interface{}) bool {
		name := key.(string)
		result := value.(string)
		fmt.Println(name, ": Lines Matched : ", result)
		return true
	})
}

// custom function to get the length of the sync map
func getSyncMapLength(m *sync.Map) int {
	count := 0
	m.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}

// function to parse the multiline grep result message from peers
func readMultilineMessage(reader *bufio.Reader, delimiter string) (string, error) {
	var result strings.Builder
	for {
		line, err := reader.ReadString('\n') // Read line by line
		if err != nil {
			return "", err // Return error if reading fails
		}
		result.WriteString(line)
		if strings.Contains(line, delimiter) { // Check for delimiter
			break // Stop reading when delimiter is found
		}
	}
	// Return message with the delimiter removed
	return strings.Replace(result.String(), delimiter, "", -1), nil
}

// main function that serves as the lifelong handler for the connection
// once a connection is initiated, either through listen on network or through
// connectToPeer, handle connection.
// We agree that there are way too many parameters and we can refactor this to a context object instead.
func handleConnection(conn net.Conn, self_name string, pattern *string, latencyStart *time.Time, grep_result_accumulator *sync.Map, peers *sync.Map, alive_peers *sync.Map, wg *sync.WaitGroup) {
	defer wg.Done()
	defer conn.Close()

	var mu sync.Mutex       //mutex lock for main peerList
	var accu_mu sync.Mutex  // mutex lock for grep accumulator
	var alive_mu sync.Mutex // mutex lock for alive peers list

	// also a mutex lock for grep accumulator but used in a parallel go routine
	// compared to the accu_mu lock above
	var yam_u sync.Mutex

	reader := bufio.NewReader(conn)
	for {
		msg, err := readMultilineMessage(reader, "END_OF_MESSAGE") //end of file flag for (large) grep results
		if err != nil {
			fmt.Println(os.Stderr, "Connection closed unexpectedly with " + conn.RemoteAddr().String())
			return
		}
		msg = strings.TrimRight(msg, "\n")
		go func(msg string) {
			if strings.Contains(msg, "CONN REXCG") {
				fmt.Printf("Sent to & Received name from peer: %s\n", msg)
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
				//GREP PAT is a protocol header for indicating that the message
				//contains a grep command pattern
				tokens := strings.Split(msg, " ")
				*pattern = (strings.Join(tokens[2:], " "))      //extract the pattern
				result := runGREPLocal(self_name, *pattern)     // run the grep command locally
				result = "GREP RET " + self_name + " " + result //generate the response message with protocol header
				communicateToPeer(conn, result)                 // send the response back to the requestor

			}
			//condition for the grep results return from the peers
			if strings.Contains(msg, "GREP RET") {
				tokens := strings.Split(msg, " ")
				//fmt.Println("Following GREP len tokens received :", len(tokens))
				name := tokens[2] // third parameter is always the machine name which generated
				//the grep result
				var result string
				//check if the grep result is not empty
				if len(tokens) > 3 {
					result = strings.Join(tokens[3:], " ")
				} else {
					result = " empty response from machine"
				}
				//store the results from all the machines in a sync map called grep_result_accumulator
				yam_u.Lock()
				update_grep_accumulator(&accu_mu, grep_result_accumulator, name, result)
				yam_u.Unlock()
				//fmt.Println(getSyncMapLength(grep_result_accumulator), getSyncMapLength(alive_peers), "len check")
				//to know whether all the (alive) machines have responded with results, we check the length of the grep accumulator
				// and compare it with the length of the alive peers list (1->1 mapping)
				//if there are no alive peers then just print the local results
				if getSyncMapLength(grep_result_accumulator) == getSyncMapLength(alive_peers) || getSyncMapLength(alive_peers) == 0 {
					var total_lines int = 0
					result := runGREPLocal(self_name, *pattern) //run the grep command locally
					// Results from all other peers have accumulated successfully and the query run is done
					latencyEnd := time.Now()
					latency := latencyEnd.Sub(*latencyStart).Milliseconds() //latency in milliseconds
					latencyStr := strconv.FormatInt(latency, 10)

					fmt.Println("Latency for the GREP search: (ms) ", latency)
					fmt.Println("All results accumulated for ", *pattern)

					grep_result_accumulator.Store(self_name, result)
					// Store the results in a file with file name self_name.txt
					storage_file := self_name + ".txt"
					_, currentFile, _, _ := runtime.Caller(0)
					dir := filepath.Dir(currentFile)
					dir = filepath.Dir(dir)
					// Construct the full path for the storage file
					fullPath := filepath.Join(dir, storage_file)
					file, err := os.Create(fullPath)
					if err != nil {
						fmt.Println("Error creating file: ", err)
					}
					file.Write([]byte("Results for pattern: " + *pattern + "\n" + "Latency : " + latencyStr + "ms" + "\n\n"))
					grep_result_accumulator.Range(func(key, value interface{}) bool {
						name := key.(string)
						result := value.(string)
						if !strings.Contains(*pattern, "-c") {
							num_matches := strings.Count(result, "\n")
							if name != self_name {
								//since the last line is trimmed of \n from the above code when it comes from other machines
								//in msg = strings.TrimRight(msg, "\n")
								num_matches = num_matches + 1

							}
							total_lines = total_lines + num_matches
							fmt.Println(name, " : Lines Matched : ", num_matches)
							file.Write([]byte(name + " : Lines Matched : " + strconv.Itoa(num_matches) + "\n" + result + "\n\n"))
						} else {
							printGREPResults(grep_result_accumulator)
							res_int, err := strconv.Atoi(result)
							if err != nil {
								fmt.Println("Error converting string to int")
								res_int = 0
							}
							total_lines = total_lines + res_int
							//since the pattern is -c, we will already get the aggregated count
							file.Write([]byte(name + " : Lines Matched : " + result + "\n"))
						}
						return true
					})
					fmt.Println("Total lines matched across all machines: ", total_lines)
					//printGREPResults(grep_result_accumulator)
					file.Close()
					// Clear the accumulator for the next run
					accu_mu.Lock()
					*grep_result_accumulator = sync.Map{}
					accu_mu.Unlock()
				}
			}
			// protocol for sending alive acknowledge (AACK) to the peer who sent CONN ALIVE request
			if strings.Contains(msg, "CONN AACK") {
				tokens := strings.Split(msg, " ")
				name := tokens[2]
				// Updating alive peers who acknowledged
				updatePeerList(&alive_mu, alive_peers, name, conn)
			}
			// protocol for checking if the peer is alive
			if strings.Contains(msg, "CONN ALIVE") {
				msg := fmt.Sprint("CONN AACK ", self_name)
				communicateToPeer(conn, msg)
			}
			// protocol for custom log file generation
			if strings.Contains(msg, "TEST FILE") {
				tokens := strings.Split(msg, " ")
				filename := tokens[2]
				pattern := strings.Join(tokens[3:], " ")
				generate_test_log_file(filename, pattern)
			}
			// protocol for printing the alive peers list
			if strings.Contains(msg, "PRNT APLIST") {
				printPeerList(alive_peers)
			}
		}(msg)
	}
}

// function to send messages to the peers
func communicateToPeer(peer net.Conn, message ...string) bool {
	var error_message string
	var message_actual string
	if len(message) == 2 {
		error_message = message[1] //if a custom error message is passed
		message_actual = message[0]
	} else {
		error_message = "Error Sending message: %v \n" // else use the default error message
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

// used for GREP multicast and test file gen multicast
func multiCastMessageToPeers(peers *sync.Map, message string) {
	fmt.Println("Multicasting message to alive peers", peersCount(peers))

	peers.Range(func(key, value interface{}) bool {
		conn := value.(net.Conn)
		communicateToPeer(conn, message)
		return true
	})
}

// count the number of peers in the sync map
func peersCount(peers *sync.Map) int {
	count := 0
	peers.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

// function to connect to the peer
func connectToPeer(pattern *string, address string, self_name string, latencyStart *time.Time, grep_result_accumulator *sync.Map, wg *sync.WaitGroup, peerList *sync.Map, alive_peers *sync.Map) (bool, net.Conn) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Printf("Error connecting to %s: %v\n", address, err)
		return false, conn
	}
	sendNameToPeer(self_name, conn)
	wg.Add(1) // Add a wait group for the new connection
	go handleConnection(conn, self_name, pattern, latencyStart, grep_result_accumulator, peerList, alive_peers, wg)
	return true, conn
}

/*
The main function that sets up the terminal for the user to interact with the program, user can type in
certain protocols directly for setting up connection or running grep command, its also used to see the grep command result
and view any errors. The user can also type in EXIT to safely exit the program closing all the connections and
giving up the port.
*/
func SetupCommTerminal(pattern *string, self_name string, auto_addresses []string, latencyStart *time.Time, grep_result_accumulator *sync.Map, peers *sync.Map, alive_peers *sync.Map, wg *sync.WaitGroup) {
	defer wg.Done()
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println(">>> ")
		message, _ := reader.ReadString('\n')
		if strings.Contains(message, "CONN INIT") {
			// CONN INIT <peer_address>
			tokens := strings.Fields(message)
			if succ, conn := connectToPeer(pattern, tokens[2], self_name, latencyStart, grep_result_accumulator, wg, peers, alive_peers); succ {
				fmt.Printf("Connected to %s - %s successfully\n", conn.RemoteAddr().String(), tokens[2])
			} else {
				fmt.Printf("Failed to connect to %s", tokens[2])
			}

		} else if strings.Contains(message, "PRNT PLIST") {
			printPeerList(peers)
		} else if strings.Contains(message, "grep") {
			//tokens := strings.Split(message, " ")
			//*pattern = (strings.Join(tokens[2:], " "))
			*pattern = message
			message = fmt.Sprint("GREP PAT ", *pattern)
			*alive_peers = sync.Map{}
			// First check which peers are alive at the moment
			fmt.Println("Finding alive peers")
			peers.Range(func(key, value interface{}) bool {
				conn := value.(net.Conn)
				msg := fmt.Sprint("CONN ALIVE ", self_name)
				communicateToPeer(conn, msg)
				return true
			})

			// uses a static delay of 2 seconds to wait for all the peers to respond
			// an optimized version could be to run the local grep and then wait out the next n seconds
			// till its 2 seconds, but anyway as of now the local grep run is in ms so it should be fine
			time.Sleep(2 * time.Second)
			// Send message only to peers who acknowledged in the above alive check
			multiCastMessageToPeers(alive_peers, message)
			*latencyStart = time.Now() //measure the start time for latency
		} else if strings.Contains(message, "CONN AUTO") {
			// Use the auto_addresses to connect to peers
			for _, address := range auto_addresses {
				if succ, conn := connectToPeer(pattern, address, self_name, latencyStart, grep_result_accumulator, wg, peers, alive_peers); succ {
					fmt.Printf("Connected to %s - %s successfully\n", conn.RemoteAddr().String(), address)
				} else {
					fmt.Printf("Failed to connect to %s", address)
				}
			}
		} else if strings.Contains(message, "EXIT") {
			fmt.Println("Exiting the program")
			// Closing connections here
			peers.Range(func(key, value interface{}) bool {
				conn := value.(net.Conn)
				conn.Close()
				return true
			})
			os.Exit(0)
		} else if strings.Contains(message, "TEST FILE") {
			tokens := strings.Fields(message)
			if len(tokens) < 4 {
				fmt.Println("Invalid test run command")
				continue
			}
			filename := tokens[2]
			pattern := strings.Join(tokens[3:], " ")
			generate_test_log_file(filename, pattern) //create the test file locally first
			multiCastMessageToPeers(peers, message)   // send the TEST FILE GENERATION command to all peers
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
			go handleConnection(conn, self_name, pattern, latencyStart, grep_result_accumulator, peers, alive_peers, wg)
		} else {
			fmt.Println("Failed to exchange names, won't try connection further")
		}
	}
}

/*
Manual main function to run the distributed log querier, the user can enter the machine name and the port
it isn't used by external packages, but contains some comments to explain the code elsewhere
*/
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

	autoAddresses := strings.Fields(input) // Split the input by spaces into a slice of strings
	for i, address := range autoAddresses {
		autoAddresses[i] = strings.TrimSpace(address)
	}
	fmt.Println("Auto addresses", autoAddresses)
	
	//use of sync map allows for concurrent read and write operations, unlike the regular go maps
	// which are not thread safe. Our code qualifies for the conditions where sync maps should be used
	// i.e a single key is written only once by independent go routines
	//(no updates) and only multiple reads are done concurrently
	peers := sync.Map{}
	alive_peers := sync.Map{}
	grep_result_accumulator := sync.Map{}
	var latencyStart time.Time

	fmt.Printf("Starting instance %s on port %s\n", name, port)
	wg.Add(2)
	go ListenOnNetwork(&pattern, port, name, &latencyStart, &grep_result_accumulator, &peers, &alive_peers, &wg)
	go SetupCommTerminal(&pattern, name, autoAddresses, &latencyStart, &grep_result_accumulator, &peers, &alive_peers, &wg)

	wg.Wait()

}
