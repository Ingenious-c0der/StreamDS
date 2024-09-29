package distributed_log_querier

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)


var subsetList []string
var peerList []string
var mode string // specifies the mode of operation, either "SUSPECT" (suspicion mechanism) or "NONSUSPECT"
// var periodicity = 2            // specifies the periodicity of the ping messages in seconds
var pingChan chan bool // channel to stop the pinger
var logFileName string
var pingTimeout = time.Second * 5 // Define your timeout duration
var self_hash string              //hash of the current node
var suspectList sync.Map          // List of suspected nodes, ONLY added if you are the owner of SUSPECTED NODE
var self_incarnationNumber int    // Incarnation number for the current node
var packetDropPercentage int = 0  // Percentage of packets to drop
var introack chan bool

func initPingChan() {
	if pingChan == nil {
		pingChan = make(chan bool)
	}
}

func stopPinger() {
	if pingChan != nil {
		close(pingChan)
		pingChan = nil
	}
}

func startPinger(nodeHashes []string, peerLastPinged *sync.Map) {
	stopPinger() // Ensure any existing pinger is stopped
	initPingChan()
	go pingNodesRoundRobin(nodeHashes, pingChan, peerLastPinged)
}

func pingNode(address string) {
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		fmt.Println("Error resolving UDP address:", err)
		return
	}
	self_address := GetAddressfromHash(&self_hash)
	communicateUDPToPeer("PING$"+self_address, addr)
}

func resetPeersStatus(nodeHashList []string, peerStatus *sync.Map, peerLastPinged *sync.Map) {
	// Clear existing entries
	peerStatus.Range(func(key, _ interface{}) bool {
		peerStatus.Delete(key)
		return true
	})
	peerLastPinged.Range(func(key, _ interface{}) bool {
		peerLastPinged.Delete(key)
		return true
	})

	// Fill both maps with the current time
	//add 2 second to the current time to avoid immediate timeouts
	currentTime := time.Now().Add(3500 * time.Millisecond)
	for _, address := range nodeHashList {
		peerStatus.Store(address, currentTime)
		peerLastPinged.Store(address, currentTime)
	}
}

func pingNodesRoundRobin(nodeHashes []string, stopPingChan chan bool, peerLastPinged *sync.Map) {

	index := 0
	for {
		select {
		case <-stopPingChan:
			// Stop pinging when signal is received
			fmt.Println("Stopping ping due to membership change... ", time.Now().Format("15:04"))
			return
		default:
			if index == len(nodeHashes) {
				index = 0
				//randomize the list
				nodeHashes = RandomizeList(nodeHashes)
			}
			// Ping the current node in the list
			if len(nodeHashes) > 0 {
				//fmt.Println("Pinging ", nodeHashes[index])
				address := GetAddressfromHash(&nodeHashes[index])
				pingNode(address)
				(*peerLastPinged).Store(nodeHashes[index], time.Now())

				//fmt.Println("Peer Last Pinged -> ", peerLastPinged)
				// Move to the next node (round-robin)
				index = (index + 1)
			}
			// Sleep for a periodicity seconds before pinging the next node
			time.Sleep(time.Millisecond * 500)

		}
	}
}

func communicateWithAPacketDropChance(dropChance int, message string, addr *net.UDPAddr) {
	if dropChance > 0 {
		// Generate a random number between 0 and 99
		randomValue := rand.Intn(100)

		// If the random value is less than the drop percentage, simulate a packet drop
		if randomValue < dropChance {
			fmt.Println("Simulating packet drop")
			return
		}
	}
	communicateUDPToPeer(message, addr)
}

// communicateUDPToPeer sends a message to the specified peer address via UDP.
func communicateUDPToPeer(message string, addr *net.UDPAddr, pass_self ...bool) {
	// Resolve the UDP connection

	if len(pass_self) > 0 {

		selfAddressStr := GetAddressfromHash(&self_hash) // Assuming this returns a string like "ip:port"
		selfAddress, err := net.ResolveUDPAddr("udp", selfAddressStr)
		if err != nil {
			fmt.Println("Error resolving self address:", err)
			return
		}
		conn, err := net.DialUDP("udp", selfAddress, addr)
		if err != nil {
			fmt.Println("Error creating UDP connection:", err)
			return
		}
		defer conn.Close()

		// Convert the message to bytes
		_, err = conn.Write([]byte(message))
		if err != nil {
			fmt.Println("Error sending message:", err)
			return
		}

		//fmt.Printf("Sent message: '%s' to %s\n", message, addr.String())
	} else {
		conn, err := net.DialUDP("udp", nil, addr)
		if err != nil {
			fmt.Println("Error creating UDP connection:", err)
			return
		}
		defer conn.Close()

		// Convert the message to bytes
		_, err = conn.Write([]byte(message))
		if err != nil {
			fmt.Println("Error sending message:", err)
			return
		}

		//fmt.Printf("Sent message: '%s' to %s\n", message, addr.String())
	}

}

// multicastUDPToPeers sends a message to multiple peers via UDP.
func multicastUDPToPeers(message string, addresses []string) {
	for _, address := range addresses {
		addr, err := net.ResolveUDPAddr("udp", address)
		if err != nil {
			fmt.Println("Error resolving UDP address:", err)
			return
		}
		communicateUDPToPeer(message, addr)
	}
}

// handleUDPMessage handles an incoming UDP message.
func handleUDPMessage(message string, addr *net.UDPAddr, peerStatus *sync.Map, peerLastPinged *sync.Map, membershipList *sync.Map) {
	if !strings.Contains(message, "PING") && !strings.Contains(message, "PINGACK") {
		fmt.Printf("Received message from %s: %s\n", addr.String(), message)
	}
	//trim message
	message = strings.TrimSpace(message)
	if strings.Contains(message, "INTRO") && !strings.Contains(message, "INTROACK") {
		membershipData := GetMembershipList(membershipList)
		membershipDataJson, err := json.Marshal(membershipData)
		if err != nil {
			fmt.Println("Error marshalling membership list:", err)
			return
		}

		msg_string := "INTROACK " + string(membershipDataJson)

		tokens := strings.Split(message, "$")
		nodeIncarnationNumber, err := strconv.Atoi(tokens[3])
		if err != nil {
			fmt.Println("Error converting string to int")
			return
		}
		nodeHashnew := strings.Join(tokens[1:3], "-")
		fmt.Println("Node Hash New + Incarnation num -> ", nodeHashnew, " ", nodeIncarnationNumber)
		_, ok := membershipList.Load(nodeHashnew)
		if ok {
			//we have already added this node, maybe hes retrying to get the membership list
			communicateUDPToPeer(string(msg_string), addr)
			fmt.Println("Sent Retry INTROACK message to ", addr.String())
			return
		}
		//add the new node to the membership list
		AddToMembershipList(membershipList, nodeHashnew, nodeIncarnationNumber)
		//fmt.Println("Should have added new node hash to membership list")
		WriteLog(logFileName, "JOINED "+nodeHashnew)

		//reply to the new node with the membership list
		communicateUDPToPeer(string(msg_string), addr)
		fmt.Println("Sent INTROACK message to ", addr.String())
		//send message to subset nodes to update their membership list
		multicastUDPToPeers("UPD$ADD$"+nodeHashnew+"$"+strconv.Itoa(nodeIncarnationNumber), subsetList)

		//recalculate the subset list
		subsetList, peerList = GetRandomizedPingTargets(membershipList, self_hash)
		stopPinger()
		resetPeersStatus(peerList, peerStatus, peerLastPinged) // Reset the peers status
		startPinger(peerList, peerLastPinged)                  // start a new pinger with the updated subset list

	} else if strings.Contains(message, "PING") && !strings.Contains(message, "PINGACK") {
		return_address := strings.Split(message, "$")[1]
		//send a ping ack to the return address
		netAddr, err := net.ResolveUDPAddr("udp", return_address)
		if err != nil {
			fmt.Println("Error resolving UDP address:", err)
			return
		}
		communicateWithAPacketDropChance(packetDropPercentage, "PINGACK$"+self_hash+"$"+strconv.Itoa(self_incarnationNumber), netAddr)

	} else if strings.Contains(message, "PINGACK") {
		incomingPeer := strings.Split(message, "$")[1]
		(*peerStatus).Store(incomingPeer, time.Now()) // Update the last seen time for the peer
		peer_incarNum, err := strconv.Atoi(strings.Split(message, "$")[2])
		if err != nil {
			fmt.Println("Error converting string to int")
			return
		}
		//check if the incoming peer is in the suspect list
		if mode == "SUSPECT" {
			//check if you are the owner of the suspected node
			_, ok := suspectList.Load(incomingPeer)
			if ok {
				//remove the node from the suspect list
				suspectList.Delete(incomingPeer)
			}
			if GetStatus(membershipList, incomingPeer) == "SUSPECT" {
				UpdateMembershipList(membershipList, incomingPeer, "ALIVE", peer_incarNum)
				multicastUDPToPeers("UPD$ALIVE$"+incomingPeer+"$"+strconv.Itoa(peer_incarNum), subsetList)
			}
		}
	} else if strings.Contains(message, "INTROACK") {
		introack <- true
		tokens := strings.Split(message, " ")
		membershipDataJson := strings.Join(tokens[1:], " ")
		var membershipData []string

		err := json.Unmarshal([]byte(membershipDataJson), &membershipData)
		if err != nil {
			fmt.Println("Error unmarshalling membership list:", err)
			return
		}
		//update the membership list
		fmt.Println("Received Membership Data -> ", membershipDataJson)
		for _, nodeHash := range membershipData {
			nodeHash = strings.TrimSpace(nodeHash)
			fmt.Println("Node Hash -> ", nodeHash)
			fmt.Println(strings.Split(nodeHash, "$"))
			node_hash := strings.Split(nodeHash, "$")[0]
			status := strings.Split(nodeHash, "$")[1]
			incarnationNum, err := strconv.Atoi(strings.Split(nodeHash, "$")[2])
			fmt.Println("Incarnation Num -> ", incarnationNum)
			if err != nil {
				fmt.Println("Error converting string to int")
				return
			}
			AddToMembershipListWithStatus(membershipList, node_hash, status, incarnationNum)
		}
		//recalculate the subset list
		subsetList, peerList = GetRandomizedPingTargets(membershipList, self_hash)
		stopPinger()
		resetPeersStatus(peerList, peerStatus, peerLastPinged) // Reset the peers status
		startPinger(peerList, peerLastPinged)
	} else if strings.Contains(message, "UPD$ADD$") {
		nodeHash := strings.Split(message, "$")[2]
		nodeIncarNum, err := strconv.Atoi(strings.Split(message, "$")[3])
		if err != nil {
			fmt.Println("Error converting string to int")
			return
		}
		_, ok := membershipList.Load(nodeHash)
		if ok {
			//do nothing to avoid echos
		} else {
			AddToMembershipList(membershipList, nodeHash, nodeIncarNum)
			//recalculate the subset list

			//get the current file path
			WriteLog(logFileName, "JOINED "+nodeHash)
			//multicast the message to the subset nodes
			multicastUDPToPeers("UPD$ADD$"+nodeHash+"$"+strconv.Itoa(nodeIncarNum), subsetList)
			subsetList, peerList = GetRandomizedPingTargets(membershipList, self_hash)
			stopPinger()
			resetPeersStatus(peerList, peerStatus, peerLastPinged) // Reset the peers status
			startPinger(peerList, peerLastPinged)

		}
	} else if strings.Contains(message, "UPD$CONFIRM$") {
		nodeHash := strings.Split(message, "$")[2]
		_, ok := membershipList.Load(nodeHash)
		//fmt.Println("Node Hash -> ", nodeHash)
		//print ok value
		//fmt.Println("Ok Value -> ", ok)
		if ok {
			//fmt.Println("Here inside UPD$CONFIRM$")
			DeleteFromMembershipList(membershipList, nodeHash)
			//fmt.Println("Deleted from membership list")
			WriteLog(logFileName, "CRASHED "+nodeHash)
			//multicast the message to the subset nodes
			multicastUDPToPeers("UPD$CONFIRM$"+nodeHash, subsetList)
			//recalculate the subset list
			subsetList, peerList = GetRandomizedPingTargets(membershipList, self_hash)
			stopPinger()
			resetPeersStatus(peerList, peerStatus, peerLastPinged) // Reset the peers status
			startPinger(peerList, peerLastPinged)
		}
	} else if strings.Contains(message, "UPD$SUS$") {
		if mode == "SUSPECT" {
			nodeHash := strings.Split(message, "$")[2]
			currIncarnationNum, err1 := strconv.Atoi(strings.Split(message, "$")[3])
			if err1 != nil {
				fmt.Println("Error converting string to int 324")
				return
			}
			if nodeHash == self_hash {
				//if you are the node being suspected, increment your incarnation number
				if currIncarnationNum >= self_incarnationNumber {
					self_incarnationNumber++
					SetIncarnationNum(membershipList, self_hash, self_incarnationNumber)
					//multicast the message to the subset nodes
					multicastUDPToPeers("UPD$ALIVE$"+self_hash+"$"+strconv.Itoa(currIncarnationNum), subsetList)
				}
			} else {
				//fmt.Println("Node Hash -> ", nodeHash)
				
				prevIncarnationNum := GetIncarnationNum(membershipList, nodeHash)
				if prevIncarnationNum == -1 {
					fmt.Println("Suspected Node is not in the membership list!!")
				} else {
					//suspect overrides alive status only if the incarnation number is greater or equal
					//if the node is already suspected, update the incarnation number
					if currIncarnationNum >= prevIncarnationNum && GetStatus(membershipList, nodeHash) == "ALIVE" {
						fmt.Println("Now suspecting node -> ", nodeHash)
						UpdateMembershipList(membershipList, nodeHash, "SUSPECT", currIncarnationNum)

						//multicast the message to the subset nodes
						multicastUDPToPeers("UPD$SUS$"+nodeHash+"$"+strconv.Itoa(currIncarnationNum), subsetList)
					} else if currIncarnationNum > prevIncarnationNum && GetStatus(membershipList, nodeHash) == "SUSPECT" {
						UpdateMembershipList(membershipList, nodeHash, "SUSPECT", currIncarnationNum)
						fmt.Println("Now suspecting node with new incarnation num-> ", nodeHash)
						//multicast the message to the subset nodes
						multicastUDPToPeers("UPD$SUS$"+nodeHash+"$"+strconv.Itoa(currIncarnationNum), subsetList)
					}
				}
			}
		}
	} else if strings.Contains(message, "UPD$ALIVE$") {
		if mode == "SUSPECT" {
			//parse the message
			tokens := strings.Split(message, "$")
			nodeHash := tokens[2]
			currIncarnationNum, err := strconv.Atoi(tokens[3])
			if err != nil {
				fmt.Println("Error converting string to int")
				return
			}
			prevIncarnationNum := GetIncarnationNum(membershipList, nodeHash)
			if prevIncarnationNum == -1 {
				fmt.Println("Node is not in the membership list Second!!")
			} else {
				//alive overrides suspect status only if the incarnation number is greater
				if currIncarnationNum > prevIncarnationNum {
					//check if you are the owner of the suspected node
					_, ok := suspectList.Load(nodeHash)
					if ok {
						//remove the node from the suspect list
						suspectList.Delete(nodeHash)
						//WriteLog(logFileName, "NOSUSPECT "+nodeHash)
					}
					UpdateMembershipList(membershipList, nodeHash, "ALIVE", currIncarnationNum)
					multicastUDPToPeers("UPD$ALIVE$"+nodeHash+"$"+strconv.Itoa(currIncarnationNum), subsetList)
				}
			}
		}

	} else if strings.Contains(message, "UPD$LEAVE$") {
		nodeHash := strings.Split(message, "$")[2]
		_, ok := membershipList.Load(nodeHash)
		if ok {
			DeleteFromMembershipList(membershipList, nodeHash)
			WriteLog(logFileName, "LEFT "+nodeHash)
			//multicast the message to the subset nodes
			multicastUDPToPeers("UPD$LEAVE$"+nodeHash, subsetList)

			//recalculate the subset list
			subsetList, peerList = GetRandomizedPingTargets(membershipList, self_hash)
			stopPinger()
			resetPeersStatus(peerList, peerStatus, peerLastPinged) // Reset the peers status
			startPinger(peerList, peerLastPinged)                  // start a new pinger with the updated subset list

		}

	}
}

func StartUDPListener(port int, peerStatus *sync.Map, peerLastPinged *sync.Map, membershipList *sync.Map) {
	address := net.UDPAddr{
		Port: port,
		IP:   net.ParseIP("0.0.0.0"),
	}
	conn, err := net.ListenUDP("udp", &address)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer conn.Close()
	fmt.Printf("Listening for UDP packets on port %d...\n", port)
	buf := make([]byte, 1024)
	for {
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			fmt.Printf("Error reading from UDP connection: %v", err)
			continue
		}
		go handleUDPMessage(string(buf[:n]), addr, peerStatus, peerLastPinged, membershipList)
	}

}

// Check for timeouts in a separate goroutine
func monitorPingTimeouts(mode *string, peerStatus *sync.Map, peerLastPinged *sync.Map, membershipList *sync.Map) {
	for {

		time.Sleep(time.Second) // Check every second
		//fmt.Println("PeerStatusList -> ", peersStatus)
		peerStatus.Range(func(key, value interface{}) bool {
			//fmt.Println("Peer -> ", peer)
			peer := key.(string)
			lastSeen := value.(time.Time)
			lastPinged, ok := peerLastPinged.Load(peer)
			if !ok {
				_, ok2 := peerStatus.Load(peer)
				if !ok2 {
					fmt.Println("Reload overdue ", peer)

					//break out of the loop
					return false
				} else {
					fmt.Println("Peer not in last pinged list, need to update  : ", peer)
					//break out of the loop
					return false
				}
			}
			if lastPinged == nil {
				fmt.Println("Last pinged is nil CHECK")
				return true
			}
			//convert the last pinged time to time.Time
			lastPingedTime := lastPinged.(time.Time)
			diff := lastSeen.Sub(lastPingedTime)
			if diff < 0 {
				diff = -diff
			}
			//fmt.Println(peer+" Diff -> ", diff)

			if diff > pingTimeout && GetStatus(membershipList, peer) == "ALIVE" {
				fmt.Println(peer+" Timeout Diff -> ", diff)
				//delete(peersStatus, peer) // Remove the peer from the list
				fmt.Println("Current Mode -> ", *mode)
				if (*mode) == "SUSPECT" {
					fmt.Printf("Now suspecting Peer %s\n", peer)
					UpdateMembershipList(membershipList, peer, "SUSPECT", GetIncarnationNum(membershipList, peer))
					go suspectNode(peer, peerStatus, peerLastPinged, membershipList)
					sus_incar := GetIncarnationNum(membershipList, peer)
					multicastUDPToPeers("UPD$SUS$"+peer+"$"+strconv.Itoa(sus_incar), subsetList)
				} else {
					fmt.Printf("Peer %s has timed out.\n", peer)
					//multicast the message to the subset nodes
					DeleteFromMembershipList(membershipList, peer)
					multicastUDPToPeers("UPD$CONFIRM$"+peer, subsetList)
					//recalculate the subset list
					subsetList, peerList = GetRandomizedPingTargets(membershipList, self_hash)
					stopPinger()
					resetPeersStatus(peerList, peerStatus, peerLastPinged) // Reset the peers status
					startPinger(peerList, peerLastPinged)                  // start a new pinger with the updated subset list

				}
			}
			return true
		})
	}
}
func suspectNode(nodeHash string, peerStatus *sync.Map, peerLastPinged *sync.Map, membershipList *sync.Map) {
	// Check if the node is already suspected
	if _, alreadySuspected := suspectList.LoadOrStore(nodeHash, time.Now()); alreadySuspected {
		return
	}
	go func() {
		fmt.Println("Suspecting node", nodeHash)
		time.Sleep(10 * time.Second)

		// After timeout, check if the node is still suspected
		suspectTime, exists := suspectList.Load(nodeHash)
		if !exists {
			// Node has been cleared from suspect list, do nothing
			return
		}
		fmt.Println("Suspect time: ", suspectTime)
		fmt.Println("Current time: ", time.Now())
		// Check if the suspect time is older than pingTimeout
		if time.Since(suspectTime.(time.Time)) >= pingTimeout {
			fmt.Println("Suspected node", nodeHash, "has timed out")

			// Node is still suspected after timeout, assume it has crashed
			DeleteFromMembershipList(membershipList, nodeHash)

			suspectList.Delete(nodeHash)

			// Write to the log file
			WriteLog(logFileName, "CRASHED "+nodeHash)

			// Multicast the crash message to peers
			multicastUDPToPeers("UPD$CONFIRM$"+nodeHash, subsetList)
			//recalculate the subset list
			subsetList, peerList = GetRandomizedPingTargets(membershipList, self_hash)
			stopPinger()
			resetPeersStatus(peerList, peerStatus, peerLastPinged) // Reset the peers status
			startPinger(peerList, peerLastPinged)                  // start a new pinger with the updated subset list

		}
	}()
}

func SetupTerminal(wg *sync.WaitGroup, membershipList *sync.Map) {
	defer wg.Done()
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println(">>> ")
		text, _ := reader.ReadString('\n')
		if strings.Contains(text, "LEAVE") {
			fmt.Println("Leaving the group")
			// multicast the message to the subset nodes
			multicastUDPToPeers("UPDATE$LEAVE$"+self_hash, subsetList)
			fmt.Println("Stopping the pinger")
			// stop the pinger
			pingChan <- true
			fmt.Println("Stopped the pinger")
			WriteLog(logFileName, "LEFT "+self_hash)
			fmt.Print("Wrote final log. Exiting...\n")
			os.Exit(0)
		} else if strings.Contains(text, "PRNT SUBSET") {
			fmt.Println(subsetList)
		} else if strings.Contains(text, "PRNT MEMSET") {
			membershipData := GetMembershipList(membershipList)
			fmt.Println("Total Count :", len(membershipData))
			//print each member on new line
			fmt.Println("START")
			for _, nodeHash := range membershipData {
				fmt.Println(nodeHash)
			}
			fmt.Println("END")
		} else if strings.Contains(text, "list_self") {
			fmt.Println(self_hash)
		} else if strings.Contains(text, "enable_sus") {
			mode = "SUSPECT"
		} else if strings.Contains(text, "disable_sus") {
			mode = "NONSUSPECT"
		} else if strings.Contains(text, "status_sus") {
			fmt.Println(mode)
		} else if strings.Contains(text, "list_sus") {
			//iterate over membership list and print out the suspected nodes
			membershipList.Range(func(key, value interface{}) bool {
				// Ensure key and value are of the expected type
				k, ok1 := key.(string)
				v, ok2 := value.(string)
				if ok1 && ok2 {
					// If both key and value are strings, append the value (or key) to the nodes list
					tokens := strings.Split(v, "$")
					if tokens[0] == "SUSPECT" {
						fmt.Println(k)
					}
				}
				return true

			})
		}
	}
}
func Startup(introducer_address string, version string, port string, log_file string, is_introducer bool, wg *sync.WaitGroup) {

	pingChan = make(chan bool, 1)
	introack = make(chan bool, 1)
	logFileName = log_file

	membershipList := sync.Map{}
	self_incarnationNumber = 0
	subsetList = make([]string, 0)
	peerList = make([]string, 0)
	suspectList = sync.Map{}
	peerLastPinged := sync.Map{}
	mode = "NONSUSPECT"

	//periodicity = 2

	pingTimeout = time.Second * 4

	peerStatus := sync.Map{}
	//start the UDP listener
	port_int, err_int := strconv.Atoi(port)
	if err_int != nil {
		fmt.Println("Error converting port to integer")
		return
	}
	fmt.Println("Starting instance on port ", port)
	if is_introducer {
		self_hash = GetOutboundIP().String() + ":" + port + "-" + version
		AddToMembershipList(&membershipList, self_hash, self_incarnationNumber)
		go StartUDPListener(port_int, &peerStatus, &peerLastPinged, &membershipList)
	} else {
		{
			addr, err := net.ResolveUDPAddr("udp", introducer_address)
			if err != nil {
				fmt.Println("Error resolving UDP address:", err)
				return
			}
			// Retry mechanism for getting the membership list
			retries := 5
			introR := false
			for retries > 0 {
				// Send intro message to the introducer
				self_intro_message := "INTRO$" + GetOutboundIP().String() + ":" + port + "$" + version + "$" + strconv.Itoa(self_incarnationNumber)
				self_hash = GetOutboundIP().String() + ":" + port + "-" + version
				AddToMembershipList(&membershipList, self_hash, self_incarnationNumber)
				go StartUDPListener(port_int, &peerStatus, &peerLastPinged, &membershipList)
				communicateUDPToPeer(self_intro_message, addr, true)
				// Wait for INTROACK for 3 seconds
				timeout := time.After(3 * time.Second)
				select {
				case <-introack: // You'll need to create this channel, set it to true when INTROACK is received
					fmt.Println("INTROACK received, proceeding...")
					retries = 0 // Exit retry loop once INTROACK is received
					introR = true
				case <-timeout:
					fmt.Println("No INTROACK received, retrying...")
					retries--
				}
			}

			if retries == 0 && !introR {
				fmt.Println("Failed to receive INTROACK after retries. Exiting...")
				return
			}
		}
	}
	fmt.Println("Self Hash -> ", self_hash)
	go monitorPingTimeouts(&mode, &peerStatus, &peerLastPinged, &membershipList)
	go SetupTerminal(wg, &membershipList)
	fmt.Println("Setup terminal")
	wg.Done()
}
