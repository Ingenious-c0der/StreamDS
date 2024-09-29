package distributed_log_querier

import (
	"fmt"
	"strings"
	"sync"
	"os"
	"math/rand"
	"net"
	"time"
	"runtime"
	"path/filepath"
	"strconv"

)



func GetAddressfromHash(hash *string) string {
	tokens := strings.Split(*hash, "-")
	return tokens[0]
}

//function expects the Sync.Map to be key (nodehash):string, value (status): string 
func AddToMembershipList(membershipList *sync.Map, nodeHash string, incarnationNum int) {
	string_val := "ALIVE" + "$" + strconv.Itoa(incarnationNum)
	membershipList.Store(nodeHash, string_val)
}
func AddToMembershipListWithStatus(membershipList *sync.Map, nodeHash string, status string, incarnationNum int) {
	string_val := status + "$" + strconv.Itoa(incarnationNum)
	fmt.Println("Adding to membership list ", string_val)
	membershipList.Store(nodeHash, string_val)
}

func DeleteFromMembershipList(membershipList *sync.Map, nodeHash string) {
	membershipList.Delete(nodeHash)
}
//used for Suspicion mechanism 
func UpdateMembershipList(membershipList *sync.Map, nodeHash string ,status string, incarnationNum int) {
	membershipList.Store(nodeHash, status+"$"+strconv.Itoa(incarnationNum))
}
//returns -1 if node is not in the membership list
func GetIncarnationNum(membershipList *sync.Map, nodeHash string) int {
	value, ok := membershipList.Load(nodeHash)
	if ok {
		//fmt.Println("Value ", value)
		tokens := strings.Split(value.(string), "$")
		//fmt.Println("Tokens", tokens)
		incarnationNum := tokens[1]
		num, err:= strconv.Atoi(incarnationNum)
		if err!=nil{
			fmt.Println("Error converting string to int 50")
			return -1
		}
		return num
	}else {
		return -1
	}
}

func GetStatus(membershipList *sync.Map, nodeHash string) string {
	value, ok := membershipList.Load(nodeHash)
	if ok {
		tokens := strings.Split(value.(string), "$")
		return tokens[0]
	}else {
		return "DEAD"
	}
}

func SetIncarnationNum(membershipList *sync.Map, nodeHash string, incarnationNum int) {
	value, ok := membershipList.Load(nodeHash)
	if ok {
		tokens := strings.Split(value.(string), "$")
		status := tokens[0]
		membershipList.Store(nodeHash, status+"$"+strconv.Itoa(incarnationNum))
	}
}



//returns a list of all the nodes in the membership list.
//in the format [nodehash1$STATUS,nodehash2$STATUS,...]
func GetMembershipList(membershipList *sync.Map) []string {
	nodes := make([]string, 0)
	membershipList.Range(func(key, value interface{}) bool {
		// Ensure key and value are of the expected type
		k, ok1 := key.(string)
		v, ok2 := value.(string)
		if ok1 && ok2{
			// If both key and value are strings, append the value (or key) to the nodes list
			nodes = append(nodes, k+"$"+v) // or append(nodes, k) if you want the keys
		}
		return true
	})
	return nodes
}
//recalculates the subset list based on the current membership list
func GetRandomizedPingTargets(membershipList *sync.Map,self_hash string) ([]string,[] string) {
	nodes := make([]string, 0)
	membershipList.Range(func(key, value interface{}) bool {
		// Ensure key and value are of the expected type
		key1, ok1 := key.(string)
		//avoid pinging yourself
		if ok1 && key1!=self_hash{
			nodes = append(nodes, key1)
		}
		return true
	})

	//fmt.Println("Random", nodes)
	//random shuffle the list
		// Randomly shuffle the slice
		rand.Shuffle(len(nodes), func(i, j int) {
			nodes[i], nodes[j] = nodes[j], nodes[i]
		})
		// Print the randomly shuffled list
	//fmt.Println("Randomly shuffled list:", nodes)
	totalNodes:= len(nodes) + 1 
	//if there are less than 3 nodes, return the list as is
	if totalNodes<=3{
		addressList:= make([]string, 0)
		for _, node := range nodes {
			addressList = append(addressList, GetAddressfromHash(&node))
		}
		return addressList, nodes
	}
	k:= (totalNodes)/2
	fmt.Println("K -> ", k)
	//if there are more than 3 nodes, return the first k nodes
	nodes = nodes[:k]
	addressList:= make([]string, 0)
	for _, node := range nodes[:k] {
		addressList = append(addressList, GetAddressfromHash(&node))
	}
	return addressList, nodes
}

//used for round robin mechanism
func RandomizeList(nodes []string) []string {
	// Randomly shuffle the slice
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
	return nodes
}


func GetMembershipListAddresses(membershipList *sync.Map) []string {
	nodes := make([]string, 0)
	membershipList.Range(func(key, value interface{}) bool {
		// Ensure key and value are of the expected type
		k, ok1 := key.(string)
		if ok1  {
			address:= GetAddressfromHash(&k)
			nodes = append(nodes, address)
		}
		return true
	})
	return nodes
}

func WriteLog(logFileName string, message string) {
	// Get the current file's directory and move up to the parent directory
	_, currentFile, _, _ := runtime.Caller(0)
	dir := filepath.Dir(currentFile)
	dir = filepath.Dir(dir)

	// Construct the full log file path
	fileName := filepath.Join(dir, logFileName)

	// Open the file for appending, create it if it doesn't exist, and write only mode
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening or creating log file:", err)
		return
	}
	defer file.Close()

	// Get the current time and prepend it to the message
	currentTime := time.Now().String()
	message = currentTime + " " + message + "\n"

	// Write the message to the file
	_, err_2 := file.WriteString(message)
	if err_2 != nil {
		fmt.Println("Error writing to log file:", err_2)
	}
}

func GetOutboundIP() net.IP {
    conn, err := net.Dial("udp", "8.8.8.8:80")
    if err != nil {
       fmt.Println(err)
    }
    defer conn.Close()

    localAddr := conn.LocalAddr().(*net.UDPAddr)

    return localAddr.IP
}
