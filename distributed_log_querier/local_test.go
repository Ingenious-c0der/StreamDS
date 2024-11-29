package main

import (
	distributed_log_querier "distributed_log_querier/core_process"
	"sync"
	"strconv"
	"fmt"
	"testing"
	"math/rand"
	"sort"
	"encoding/json"
	"os"
	"io"
	"path/filepath"
	"bytes"
)


func TestLocalTesting(t *testing.T) {
	//testKeyTableAddRemove(t)
	//testKeyTableAddRemove(t)
	//testFileAndNodeIdMaptoRing(t)
	//testLocalOperatorRuns(t)
	//testBufferWritingFunctionality(t)

	// count , err := countLinesFromFile(t)
	// if err != nil {
	// 	fmt.Println("Error reading file: ", err)
	// }
	// fmt.Println("Number of lines in the file: ", count)

	//testParitioning(t)
	//testLoops(t)
	//testReadPartitioning(t)
	//testLocalOperatorRunSplitLine(t)
	testLocalOperatorRunWordCount(t)
}

func testReadPartitioning(t *testing.T) {
	lines, err := distributed_log_querier.ReadFilePartition("server9.log", 50 , 52)
	if err != nil {
		fmt.Println("Error reading file: ", err)
	}
	fmt.Println("Number of lines in the file: ", len(lines))
	for _, line := range lines {
		fmt.Println(line)
	}
}

func testLoops(t *testing.T) {
	num_tasks := 3 
	for  i := 0; i < num_tasks; i++ {
		fmt.Println("Task Stage2: ", i)
	}
	for i:= num_tasks; i < 2*num_tasks; i++ {
		fmt.Println("Task Stage1: ", i)
	}
	for i:= 2*num_tasks; i < 3*num_tasks; i++ {
		fmt.Println("Task Stage0: ", i)
	}
}
func testParitioning(t *testing.T) {
	paritions := distributed_log_querier.GetFairPartitions(100, 1)
	fmt.Println(paritions)
	paritions = distributed_log_querier.GetFairPartitions(1313, 3)
	fmt.Println(paritions)
}

func testBufferWritingFunctionality(t *testing.T) {

	bufferMap := make(map[string]string)
	for i := 0; i < 10; i++ {
		bufferMap["key"+ strconv.Itoa(i)] = "value" + strconv.Itoa(i)
	}
	for key, value := range bufferMap {
		fmt.Println("Key: ", key, "Value: ", value)
	}
	
	for i := 0; i < 10; i++ {
		
		formatted_buffer := distributed_log_querier.FormatAsBuffer(bufferMap)
		//write to the file 
		distributed_log_querier.WriteBufferToFileTestOnly(formatted_buffer, "test.txt")
		//read from the file
		bufferMapCurrent, error := distributed_log_querier.ReadLastBuffer("test.txt")
		if error != nil {
			fmt.Println("Error reading buffer from file" , error)
		}
		//get buffer map length 
		bufferlength:= getMapLength(bufferMapCurrent)
		if bufferlength == 10 - i{
			fmt.Println("Test Passed", bufferlength)
		}else{
			fmt.Println("Test Failed", bufferlength)
		}
		//remove the last element from the buffer
		
		delete(bufferMap, "key" + strconv.Itoa(10-i-1))
	}
	delete(bufferMap, "key" + strconv.Itoa(0))
	distributed_log_querier.WriteBufferToFileTestOnly(distributed_log_querier.FormatAsBuffer(bufferMap), "test.txt")
	//try reading on an empty buffer
	_, error := distributed_log_querier.ReadLastBuffer("test.txt")
	if error != nil {
		fmt.Println("Error reading buffer from file" , error)
	}else{
		fmt.Println("Test Failed")
	}
}


func countLinesFromFile(t *testing.T) (int, error) {
	fileName := "test.txt"
	//read from fetched dir 
	dir := distributed_log_querier.GetDistributedLogQuerierDir()
	filePath := filepath.Join(dir, "Fetched", fileName)
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	count := 0
	buf := make([]byte, 32*1024)
	lineSep := []byte{'\n'}

	for {
		c, err := file.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil
		case err != nil:
			return count, err
		}
	}	
}



func testLocalOperatorRunSplitLine(t *testing.T) {
	ip_string := "hello world this is a test"
	output := distributed_log_querier.RunOperator("split_operator", ip_string)
	var splitMap []string
	err := json.Unmarshal([]byte(output), &splitMap)
	if err != nil {
		fmt.Println("Error converting to JSON: ", err)
	}
	fmt.Println(splitMap)

}


func testLocalOperatorRunWordCount(t *testing.T) {
	// testOperatorRuns(t)
	ip_string := "hello"
	output := distributed_log_querier.RunOperator("count_operator", ip_string)
	var countMap map[string]int
	err := json.Unmarshal([]byte(output), &countMap)
	if err != nil {
		fmt.Println("Error converting to JSON: ", err)
	}
	if !(countMap["hello"] == 1 ){
		fmt.Println("Test Failed")
	}
	ip_string = "hello"
	output = distributed_log_querier.RunOperator("count_operator", ip_string)
	err = json.Unmarshal([]byte(output), &countMap)
	if err != nil {
		fmt.Println("Error converting to JSON: ", err)
	}
	if !(countMap["hello"] == 2 ){
		fmt.Println("Test Failed")
	}
	ip_string = "world"
	output = distributed_log_querier.RunOperator("count_operator", ip_string)
	err = json.Unmarshal([]byte(output), &countMap)
	if err != nil {
		fmt.Println("Error converting to JSON: ", err)
	}
	if !(countMap["world"] == 1 && countMap["hello"] == 2){
		fmt.Println("Test Failed")
	}

	//loop for 1000 times with different words
	for i := 0; i < 1000; i++ {
		ip_string = "word" + strconv.Itoa(i)
		output = distributed_log_querier.RunOperator("count_operator", ip_string)
		err = json.Unmarshal([]byte(output), &countMap)
		if err != nil {
			fmt.Println("Error converting to JSON: ", err)
		}
		if !(countMap["world"] == 1 && countMap["hello"] == 2 && countMap["word" + strconv.Itoa(i)] == 1){
			fmt.Println("Test Failed")
		}
	}


	fmt.Println("Test Passed")
	
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

func getMapLength(m map[string]string) int {
	count := 0
	for range m {
		count++
	}
	return count
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