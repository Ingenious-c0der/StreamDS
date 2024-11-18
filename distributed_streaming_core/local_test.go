package main

import (
	distributed_log_querier "distributed_log_querier/core_process"
	"sync"
	"strconv"
	"fmt"
	"testing"
	"math/rand"
	"sort"
)


func TestLocalTesting(t *testing.T) {
	//testKeyTableAddRemove(t)
	//testKeyTableAddRemove(t)
	testFileAndNodeIdMaptoRing(t)
}


func testFileAndNodeIdMaptoRing(t *testing.T) {
	keyTable := sync.Map{}
// 	ipAddresses := []string{
// 	"192.168.0.103:6061","192.168.0.103:6062", "192.168.0.103:6063","192.168.0.103:6064", "192.168.0.103:6065", 
// "192.168.0.103:6066","192.168.0.103:6067", "192.168.0.103:6068","192.168.0.103:6069", "192.168.0.103:6070"}
ipAddresses := []string{
	"192.168.0.103:6061","192.168.0.103:6062", "192.168.0.103:6063","192.168.0.103:6070"}
	filenames := []string{ "fourthFile.txt"}
	m:=10 // number of nodes
	file_map := make(map[int][]string) // maps where the file is stored
	replica_map := make(map[int][]string) // maps where the replica is stored
	for _,ip := range ipAddresses {
		nodeID := distributed_log_querier.GetPeerID(ip,m)
		distributed_log_querier.KeyTableAdd(&keyTable, nodeID)
	}
	printSyncMap(&keyTable)
	for _, file := range filenames {
		fileHash := distributed_log_querier.GetFileID(file,m)
		coodID:= distributed_log_querier.GetHyDFSCoordinatorID(&keyTable, fileHash)
		x, y := distributed_log_querier.GetHYDFSSuccessorIDs(coodID, &keyTable)
		//if the file is not in the map
		if _, ok := file_map[coodID]; !ok {
			// If key doesn't exist, initialize it with the new slice
			file_map[coodID] = []string{strconv.Itoa(fileHash)}
		} else {
			// If the key exists, retrieve the slice, append to it, and store it back in the map
			file_map[coodID] = append(file_map[coodID], strconv.Itoa(fileHash))
		}
		//replica 1
		if _, ok := replica_map[x]; !ok {
			// If key doesn't exist, initialize it with the new slice
			replica_map[x] = []string{strconv.Itoa(fileHash)}
		} else {
			// If the key exists, retrieve the slice, append to it, and store it back in the map
			replica_map[x] = append(replica_map[x], strconv.Itoa(fileHash))
		}
		//replica 2
		if _, ok := replica_map[y]; !ok {
			// If key doesn't exist, initialize it with the new slice
			replica_map[y] = []string{strconv.Itoa(fileHash)}
		} else {
			// If the key exists, retrieve the slice, append to it, and store it back in the map
			replica_map[y] = append(replica_map[y], strconv.Itoa(fileHash))
		}
	}
	//iterate over the file_map
	for key, value := range file_map {
		sort.Strings(value)
		fmt.Println("Key: ", key, "Value: ", value)
	}
	//iterate over the replica_map
	fmt.Println("Replica Map")
	for key, value := range replica_map {
		sort.Strings(value)
		fmt.Println("Key: ", key, "Value: ", value)
	}


}



func testKeyTableAddRemove(t *testing.T) {
	keyTable := sync.Map{}
	distributed_log_querier.KeyTableAdd(&keyTable, 102)
	printSyncMap(&keyTable)
	distributed_log_querier.KeyTableAdd(&keyTable, 204)
	printSyncMap(&keyTable)
	distributed_log_querier.KeyTableAdd(&keyTable, 306)
	printSyncMap(&keyTable)
	distributed_log_querier.KeyTableAdd(&keyTable, 1020)
	printSyncMap(&keyTable)
	distributed_log_querier.KeyTableAdd(&keyTable, 111)
	printSyncMap(&keyTable)
	x,y := distributed_log_querier.GetHYDFSSuccessorIDs(102, &keyTable)
	fmt.Println("Successor of 102: ", x, " - ", y)
	x,y = distributed_log_querier.GetHYDFSSuccessorIDs(1020, &keyTable)
	fmt.Println("Successor of 1020: ", x, " - ", y)
	x,y = distributed_log_querier.GetHYDFSSuccessorIDs(306, &keyTable)
	fmt.Println("Successor of 306: ", x, " - ", y)
	fmt.Println("Now removing")
	distributed_log_querier.KeyTableRemove(&keyTable, 102)
	printSyncMap(&keyTable)
	distributed_log_querier.KeyTableRemove(&keyTable, 204)
	printSyncMap(&keyTable)
	distributed_log_querier.KeyTableRemove(&keyTable, 306)
	printSyncMap(&keyTable)
	distributed_log_querier.KeyTableRemove(&keyTable, 1020)
	printSyncMap(&keyTable)
}
func printSyncMap(m *sync.Map) {
	m.Range(func(key, value interface{}) bool {
		fmt.Println(key, value)
		return true
	})
	fmt.Println("*****")
}

func generateFilenames(count int) []string {
	// Seed the random generator
	filenames := make([]string, count)

	for i := 0; i < count; i++ {
		// Random number between 1 and 9999 to make each filename unique
		randomNumber := rand.Intn(9999) + 1
		filenames[i] = fmt.Sprintf("file%d", randomNumber)
	}

	return filenames
}