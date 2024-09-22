package distributed_log_querier

import (
	
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"encoding/json"
	"time"
	"strconv"
	"bufio"
)

var membershipList sync.Map
var subsetList []string
var peerList []string
var mode string= "NONSUSPECT" // specifies the mode of operation, either "SUSPECT" (suspicion mechanism) or "NONSUSPECT"
var periodicity = 2 // specifies the periodicity of the ping messages in seconds
var pingChan chan bool // channel to stop the pinger
var logFileName string
var pingTimeout = time.Second * 5 // Define your timeout duration
var peersStatus = make(map[string]time.Time) // Track when we last received a PINGACK from each peer
var self_hash string //hash of the current node
func pingNode(address string){
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		fmt.Println("Error resolving UDP address:", err)
		return
	}
	communicateUDPToPeer("PING$",addr)
}

func resetPeersStatus(nodeHashList []string){
	peersStatus = make(map[string]time.Time) // Reset the peers status
	//fill the peers status with the current time
	for _, address := range nodeHashList {
		peersStatus[address] = time.Now()
	}
}

func pingNodesRoundRobin(addresses []string, stopPingChan chan bool){

	index := 0
	for {
		select {
		case <-stopPingChan:
			// Stop pinging when signal is received
			fmt.Println("Stopping ping due to membership change...")
			return
		default:
			if index == len(addresses) {
				RandomizeList(addresses)
				index = 0
			}
			fmt.Println("Pinging ", addresses[index])
			// Ping the current node in the list
			if len(addresses) > 0 {
				pingNode(addresses[index])
				// Move to the next node (round-robin)
				index = (index + 1) % len(addresses)
			}
			// Sleep for a periodicity seconds before pinging the next node
			time.Sleep(time.Duration(periodicity) * time.Second)
		}
	}
}


// communicateUDPToPeer sends a message to the specified peer address via UDP.
func communicateUDPToPeer(message string, addr *net.UDPAddr ,pass_self ...bool) {
	// Resolve the UDP connection
	fmt.Println("Pass self -> ", pass_self)
	if(len(pass_self)>0 ){

		selfAddressStr := GetAddressfromHash(&self_hash) // Assuming this returns a string like "ip:port"
		selfAddress, err := net.ResolveUDPAddr("udp", selfAddressStr)
		if err != nil {
			fmt.Println("Error resolving self address:", err)
			return
		}
		conn, err := net.DialUDP("udp",selfAddress, addr)
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
	
		fmt.Printf("Sent message: '%s' to %s\n", message, addr.String())
	}else{
		conn, err := net.DialUDP("udp",nil, addr)
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
	
		fmt.Printf("Sent message: '%s' to %s\n", message, addr.String())
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
func handleUDPMessage(message string, addr *net.UDPAddr) {
	fmt.Printf("Received message from %s: %s\n", addr.String(), message)
	//trim message
	message = strings.TrimSpace(message)
	if (strings.Contains(message, "INTRO") && !strings.Contains(message, "INTROACK")) {
		membershipData:= GetMembershipList(&membershipList)
		membershipDataJson, err := json.Marshal(membershipData)
		if err != nil {
			fmt.Println("Error marshalling membership list:", err)
			return
		}
		msg_string:= "INTROACK " + string(membershipDataJson)
		tokens := strings.Split(message, "$")
		nodeHashnew := strings.Join(tokens[1:], "-")
		fmt.Println("Node Hash New -> ", nodeHashnew)
		//add the new node to the membership list
		AddToMembershipList(&membershipList,nodeHashnew)
		fmt.Println("Should have added new node hash to membership list")
		WriteLog(logFileName, "JOINED "+nodeHashnew)

		//reply to the new node with the membership list
		communicateUDPToPeer(string(msg_string),addr)

		//send message to subset nodes to update their membership list
		multicastUDPToPeers("UPD$ADD$"+nodeHashnew, subsetList)

		//recalculate the subset list
		subsetList, peerList= GetRandomizedPingTargets(&membershipList,self_hash)
		pingChan <- true // stop the current pinger
		resetPeersStatus(peerList) // Reset the peers status
		go pingNodesRoundRobin(subsetList, pingChan) // start a new pinger with the updated subset list


	}else if(strings.Contains(message,"PINGACK")){
		peersStatus[addr.String()] = time.Now() // Update the last seen time for the peer
	}else if(strings.Contains(message,"PING") && !strings.Contains(message,"PINGACK")){
		communicateUDPToPeer("PINGACK",addr)
	}else if(strings.Contains(message,"INTROACK")){
		tokens := strings.Split(message, " ")
		membershipDataJson := strings.Join(tokens[1:], " ")
		var membershipData []string
		fmt.Println("Received Membership Data -> ", membershipDataJson)
		err := json.Unmarshal([]byte(membershipDataJson), &membershipData)
		if err != nil {
			fmt.Println("Error unmarshalling membership list:", err)
			return
		}
		//update the membership list

		for _, nodeHash := range membershipData {
			nodeHash = strings.TrimSpace(nodeHash)
			fmt.Println("Node Hash -> ", nodeHash)
			node_hash := strings.Split(nodeHash, "$")[0]
			status := strings.Split(nodeHash, "$")[1]
			AddToMembershipListWithStatus(&membershipList,node_hash,status)
		}
		//recalculate the subset list
		subsetList, peerList = GetRandomizedPingTargets(&membershipList,self_hash)
		pingChan <- true // stop the current pinger
		resetPeersStatus(peerList) // Reset the peers status
		go pingNodesRoundRobin(subsetList, pingChan) // start a new pinger with the updated subset list
	}else if(strings.Contains(message,"UPD$ADD$")){
		nodeHash := strings.Split(message, "$")[2]
		_, ok := membershipList.Load(nodeHash)
		if ok{
			//do nothing to avoid echos
		}else{
			AddToMembershipList(&membershipList,nodeHash)
			//get the current file path
			WriteLog(logFileName, "JOINED " +nodeHash)
			//multicast the message to the subset nodes
			multicastUDPToPeers("UPD$ADD$"+nodeHash, subsetList)
			//recalculate the subset list
			subsetList, peerList = GetRandomizedPingTargets(&membershipList,self_hash)
			pingChan <- true // stop the current pinger
			resetPeersStatus(peerList) // Reset the peers status
			go pingNodesRoundRobin(subsetList, pingChan) // start a new pinger with the updated subset list

		}
	}else if(strings.Contains(message,"UPD$DEL$")){
		nodeHash := strings.Split(message, "$")[2]
		_, ok := membershipList.Load(nodeHash)
		if ok{
			DeleteFromMembershipList(&membershipList,nodeHash)
			

			WriteLog(logFileName, "CRASHED " + nodeHash)
			//multicast the message to the subset nodes
			multicastUDPToPeers("UPD$DEL$"+nodeHash, subsetList)
			//recalculate the subset list
			subsetList, peerList = GetRandomizedPingTargets(&membershipList,self_hash)
			pingChan <- true // stop the current pinger
			resetPeersStatus(peerList) // Reset the peers status
			go pingNodesRoundRobin(subsetList, pingChan) // start a new pinger with the updated subset list
		}else{
			//do nothing to avoid echos
		}
	}else if(strings.Contains(message,"UPD$SUS$")){
	}else if(strings.Contains(message,"UPD$NOSUS$")){
	}else if(strings.Contains(message,"UPD$LEAVE$")){
		nodeHash := strings.Split(message, "$")[2]
		_, ok := membershipList.Load(nodeHash)
		if ok{
			DeleteFromMembershipList(&membershipList,nodeHash)
			WriteLog(logFileName, "LEFT " + nodeHash)
			//multicast the message to the subset nodes
			multicastUDPToPeers("UPD$LEAVE$"+nodeHash, subsetList)
			
			//recalculate the subset list
			subsetList, peerList= GetRandomizedPingTargets(&membershipList,self_hash)
			pingChan <- true // stop the current pinger
			resetPeersStatus(peerList) // Reset the peers status
			go pingNodesRoundRobin(subsetList, pingChan) // start a new pinger with the updated subset list

	}else{
		//do nothing to avoid echos
	}



}}

func StartUDPListener(port int){
	address:= net.UDPAddr{
		Port: port,
		IP: net.ParseIP("0.0.0.0"),
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
		go handleUDPMessage(string(buf[:n]), addr)
	}

}

// Check for timeouts in a separate goroutine
func monitorPingTimeouts() {
	for {
		
		time.Sleep(time.Second) // Check every second
		now := time.Now()
		for peer, lastSeen := range peersStatus {
			fmt.Println("Peer -> ", peer)
			fmt.Println("Last Seen -> ", lastSeen)
			fmt.Println("Now -> ", now)
			fmt.Println("Diff -> ", now.Sub(lastSeen))
			if now.Sub(lastSeen) > pingTimeout {
				fmt.Printf("Peer %s has timed out.\n", peer)
				//delete(peersStatus, peer) // Remove the peer from the list
				if(mode == "SUSPECT"){
				}else{
					//multicast the message to the subset nodes
					DeleteFromMembershipList(&membershipList,peer)
					multicastUDPToPeers("UPD$DEL$"+peer, subsetList)
					//recalculate the subset list
					subsetList, peerList = GetRandomizedPingTargets(&membershipList,self_hash)
					pingChan <- true // stop the current pinger
					resetPeersStatus(peerList) // Reset the peers status
					go pingNodesRoundRobin(subsetList, pingChan) // start a new pinger with the updated subset list
					break 
				}
			}
		}
	}
}
func SetupTerminal(wg* sync.WaitGroup){
	defer wg.Done()
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println(">>> ")
		text, _ := reader.ReadString('\n')
		if strings.Contains(text, "LEAVE") {
			fmt.Println("Leaving the group")
			// multicast the message to the subset nodes
			multicastUDPToPeers("UPDATE$LEAVE$" + self_hash, subsetList)
			fmt.Println("Stopping the pinger")
			// stop the pinger
			pingChan <- true
			fmt.Println("Stopped the pinger")
			WriteLog(logFileName, "LEFT "+ self_hash)
			fmt.Print("Wrote final log. Exiting...\n")
		
			// exit the program
			os.Exit(0)
		}else if(strings.Contains(text, "PRNT SUBSET")){
			fmt.Println(subsetList)
		}else if(strings.Contains(text, "PRNT MEMSET")){
			membershipData:= GetMembershipList(&membershipList)
			fmt.Println(membershipData)
		}
	}
}
func Startup(introducer_address string, version string, port string, log_file string, is_introducer bool, wg * sync.WaitGroup){
	
	pingChan = make(chan bool,1)
	
	logFileName = log_file

	membershipList = sync.Map{}
	
	subsetList = make([]string, 0)
	peerList = make([]string, 0)
	mode = "NONSUSPECT"

	periodicity = 2

	pingTimeout = time.Second * 7

	peersStatus = make(map[string]time.Time)

	fmt.Println("Starting instance on port ", port)
	if(is_introducer){
		self_hash = GetOutboundIP().String()+ ":" +port + "-"+ version
		AddToMembershipList(&membershipList,self_hash)
	}else{
		addr, err := net.ResolveUDPAddr("udp", introducer_address)
		if err != nil {
			fmt.Println("Error resolving UDP address:", err)
			return
		}
		//send an intro message to the introducer
		self_intro_message := "INTRO$"+ GetOutboundIP().String()+ ":" + port + "$" + version
		self_hash = GetOutboundIP().String()+ ":" +port + "-"+ version
		AddToMembershipList(&membershipList,self_hash)
		communicateUDPToPeer(self_intro_message,addr,(true))
	}
	fmt.Println("Self Hash -> ", self_hash)
	//start the UDP listener
	port_int,err_int:= strconv.Atoi(port)
	if(err_int != nil){
		fmt.Println("Error converting port to integer")
		return
	}
	go StartUDPListener(port_int)
	go monitorPingTimeouts()
	fmt.Println("Setup terminal")
	wg.Done()
}