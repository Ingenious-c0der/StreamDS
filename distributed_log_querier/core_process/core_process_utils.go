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
	"crypto/sha1"
	"math/big"
	"sort"
	"io"
	"sync/atomic"

)

type SafeConn struct {
    conn net.Conn
    mu   sync.Mutex
}

type StringIntPair struct {
	Key   string
	Value int
}

type LamportClock struct {
    timestamp int64
}

func (sc *SafeConn) SafeWrite(data []byte) (int, error) {
    sc.mu.Lock()
    defer sc.mu.Unlock()
    return sc.conn.Write(data)
}

// Increment safely increments the Lamport timestamp
func (lc *LamportClock) Increment() int64 {
    return atomic.AddInt64(&lc.timestamp, 1)
}

func GetHYDFSSuccessorIDs(self_id int, keytable *sync.Map) (int, int){
	//get the wrapped 2 successors of the current node
	//get the keys from the table and then sort them
	keys := make([]int, 0)
	keytable.Range(func(k, v interface{}) bool {
		keys = append(keys, k.(int))
		return true
	})
	if len(keys) < 3{
		fmt.Println("Not enough keys in the keyTable")
		return -1, -1
	}
	
	//sort the keys
	sort.Ints(keys)
	var index int 
	//find the position of the key in the sorted list
	for i, key := range keys{
		if key == self_id{
			index = i
			break
		}
	}
	if index == len(keys)-1{
		return keys[0], keys[1]
	}
	if index == len(keys)-2{
		return keys[index+1], keys[0]
	}
	return keys[index+1], keys[index+2]
}

// GetTimestamp safely retrieves the current timestamp
func (lc *LamportClock) GetTimestamp() int64 {
    return atomic.LoadInt64(&lc.timestamp)
}



//file to show return the values stored locally in all the bays (used for store command)
func ShowLocalFiles(fileNameMap *sync.Map){
	dir := GetDistributedLogQuerierDir()
	//show the files in the file bay
	fmt.Println("Showing storage at " + strconv.Itoa(getSelf_id()) )
	fmt.Println("Original Files")
	files, err := os.ReadDir(filepath.Join(dir, "FileBay"))
	if err != nil {
		fmt.Println("Error reading file bay directory:", err)
	}
	for _, file := range files {
		base := strings.Split(file.Name(), ".")[0]
		hydfsName, ok:= fileNameMap.Load(strings.Split(base, "_")[1])
		if ok{
			fmt.Println(file.Name() + " - " + hydfsName.(string))
		}else{
			fmt.Println(file.Name())
		}
	}
	//show the files in the replica bay
	fmt.Println("Replica Files")
	files, err =
		os.ReadDir(filepath.Join(dir, "ReplicaBay"))
	if err != nil {
		fmt.Println("Error reading replica bay directory:", err)
	}
	for _, file := range files {
		base := strings.Split(file.Name(), ".")[0]
		hydfsName, ok:= fileNameMap.Load(strings.Split(base, "_")[1])
		if ok{
			fmt.Println(file.Name() + " - " + hydfsName.(string))
		}else{
			fmt.Println(file.Name())
		}
	}
	//show the files in the append bay
	fmt.Println("Logical Append Folders")
	files, err = os.ReadDir(filepath.Join(dir, "appendBay"))
	if err != nil {
		fmt.Println("Error reading append bay directory:", err)
	}
	for _, file := range files {
		fmt.Println("Logical append from " + file.Name())
	}
	fmt.Println("**********")
}

//returns the message containing the local support for the file, to be used in ls hydfsfilename command
func GetLocalSupportForFile(fileID string) string{
	support:= ""
	self_id := getSelf_id()
	dir := GetDistributedLogQuerierDir()
	//check if the file exists in the file bay
	selfAddress := GetOutboundIP().String()
	if checkFileExists("FileBay", "original_" + fileID){
			support = support + "Coordinator " + strconv.Itoa(self_id) + "@"+ selfAddress+ " " +"stores the file original_"+ fileID + ".txt\n"
	}
	//check if the file exists in the replica bay
	if checkFileExists("ReplicaBay", "replica_" + fileID){
		support = support + "Replica " + strconv.Itoa(self_id) + "@"+ selfAddress+ " " +"stores the file replica_"+ fileID + ".txt\n"
	}
	//check if the file exists in the append bay
	appendDir := filepath.Join(dir, "appendBay")
	nodeDirs, err := os.ReadDir(appendDir)
	if err != nil {
		fmt.Println("Error reading append directory:", err)
		return ""
	}
	//ensure support has some content
	if len(nodeDirs) == 0{
		return support
	}
	nodeDirNames := make([]string, 0)
	for _, nodeDir := range nodeDirs {
		//directly send nodedir names 
		nodeDirNames = append(nodeDirNames, nodeDir.Name())
	}
	support = support + "Append Files " + strconv.Itoa(self_id) + "@"+ selfAddress + " stores logical appends from " + strings.Join(nodeDirNames, ",") + "\n"
	return support
}



//handles the forwarding of both, append files and replicas
func forwardReplica(lc *LamportClock, conn net.Conn, fileID string, node_ID int){
	file_name:= "original_" + fileID + ".txt"
	send_file_name:= "replica_" + fileID + ".txt"
	sendHyDFSFile(lc, conn, "original", file_name, send_file_name)
	//send the logical append files 
	//check if there are any logical appends for this file 
	dir := GetDistributedLogQuerierDir()
	appendDir := filepath.Join(dir, "appendBay")
	nodeDir := filepath.Join(appendDir, strconv.Itoa(node_ID))
	if _, err := os.Stat(nodeDir); os.IsNotExist(err) {
		fmt.Println("Node directory does not exist,skipping logical appends")
		return
	}
	files, err := os.ReadDir(nodeDir)
	if err != nil {
		fmt.Println("Error reading append directory:", err)
		return
	}
	//send the logical append files
	for _, file := range files {
			node_file_path:= filepath.Join(strconv.Itoa(node_ID), file.Name())
			fmt.Println("Node file path for append" + node_file_path)
			fmt.Println("Sending logical append file " + file.Name() + " to " + conn.RemoteAddr().String())
			sendHyDFSFile(lc, conn, "append", filepath.Join(node_file_path, file.Name()), file.Name())
	}
}

func createFile(lc *LamportClock, local_filename string ,HydfsfileName string,connTable *sync.Map,keyTable *sync.Map,fileNameMap *sync.Map, self_id int, m int){
	//while creating a file, following steps need to be followed
	//1. generate fileID from the filename
	//2. check if the file already exists at the coordinator 
	//3. if it does not then create the file at the coordinator

	//generate the fileID
	fileID := GetFileID(HydfsfileName, m)
	//get the coordinator for this file
	coordinator := GetHyDFSCoordinatorID(keyTable, fileID)
	if coordinator == -1{
		fmt.Println("Not enough keys while checking for COOD")
		return
	}
	fileName:= "original_" + strconv.Itoa(fileID) + ".txt"
	if self_id == coordinator{
		//copy file contents to the file bay
		//first check if file exists in the file bay
		if checkFileExists("FileBay", fileName){
			fmt.Println("File "+ HydfsfileName +" already exists in HYDFS (local)")
			return
		}
		dir:= GetDistributedLogQuerierDir()
		filePath := filepath.Join(dir, "FileBay", fileName)
		//construct the original file path 
		originalFilePath := filepath.Join(dir, "business", local_filename)
		//first create the file
		_, err := os.Create(filePath)
		if err != nil {
			fmt.Println("Error creating file:", err)
			return
		}
		//copy the file contents to the file bay
		err_copy := copyFileContents(originalFilePath, filePath)
		if err_copy != nil{
			fmt.Println("Error copying file to fileBay")
			return
		}
		//broadcast the fileName to the replicas
		broadcastHyDFSMessage(lc, connTable, "MAPSFILE: " + strconv.Itoa(fileID) + " " + HydfsfileName)
		fileNameMap.Store(fileID, HydfsfileName)
		fmt.Println("File " + fileName + " successfully created in HYDFS (local)")

	}else{
		//use the check exist functionality
		//get the conn for the coordinator
		conn, ok := connTable.Load(coordinator)
		if !ok{
			fmt.Println("Error loading conn from connTable")
			return
		}
		fmt.Println("Sending check exist message to " + conn.(net.Conn).RemoteAddr().String())
		//also send hydfsFilename for recovery purposes
		sendHyDFSMessage(lc, conn.(net.Conn), "CHECKEXIST " + strconv.Itoa(fileID) + " " + local_filename + " " + HydfsfileName)
	}
}

//function to be run only at the coordinator
func LogicalTempMerge(fileID string, requestor_node_id string) string {
	//steps:
	//1. check if the file exists in the file bay
	//2. check if any logical appends exist for this file
	//3. collect the append number and sort them with lamport timestamps for the requestor node ONLY
	//4. merge the file with the logical appends and store it in temp
	dir:= GetDistributedLogQuerierDir()
	temp_dir := filepath.Join(dir, "temp")
	fileBay_dir := filepath.Join(dir, "FileBay")
	filePath := filepath.Join(fileBay_dir, "original_" + fileID)
	filePath = filePath + ".txt"
	//check if the file exists in the file bay
	if !checkFileExists("FileBay", "original_" + fileID){
		fmt.Println("File to be logically merged does not exist in the file bay")
		return ""
	}
	//check if there are any logical appends from the requestor node
	appendDir := filepath.Join(dir, "appendBay", requestor_node_id)	
	if _, err := os.Stat(appendDir); os.IsNotExist(err) {
		fmt.Println("No logical appends found for file - NO OP" + fileID)
		//simply copy the file to the temp dir 
		tempFilePath := filepath.Join(temp_dir, "cache_" + fileID)
		tempFilePath = tempFilePath + ".txt"
		err := copyFileContents(filePath, tempFilePath)
		if err != nil {
			fmt.Println("Error copying file to temp:", err)
			return ""
		}
		return tempFilePath
	}else{
		node_list:= make([]StringIntPair, 0)
		files, err := os.ReadDir(appendDir)
		if err != nil {
			fmt.Println("Error reading append directory from logical temp merge :", err)
			return ""
		}
		fmt.Println("Files found in appendDir", files)
		for _, file := range files {
			//filename like append_434_134_500.txt
			// 434 is nodeId, 134 is fileID, 500 is lamport timestamp
			fileBase:= strings.Split(file.Name(), ".")[0]
			fileID_parse := strings.Split(fileBase, "_")[2] 
			if fileID_parse == fileID{
				//get the lamport timestamp
				fmt.Println(fileBase)
				fmt.Println("Lamport :" + strings.Split(fileBase, "_")[3])
				lamport_timestamp, err := strconv.Atoi(strings.Split(fileBase, "_")[3])
				if err != nil{
					fmt.Println("Error converting lamport string to int")
					return ""
				}
				node_list = append(node_list, StringIntPair{filepath.Join(appendDir, file.Name()), lamport_timestamp})
			}
		}
		//sort the node_list based on the lamport timestamp
		sort.Slice(node_list, func(i, j int) bool {
			return node_list[i].Value < node_list[j].Value
		})
		//merge the file with the logical appends
		//first read the original file
		originalFileData, err := os.ReadFile(filePath)
		if err != nil {
			fmt.Println("Error opening file:", err)
			return ""
		}
		//append in place the logical appends with newline
		fmt.Println("Node list for logical merge", node_list)
		for _, appendFile := range node_list{
			appendFileData, err := os.ReadFile(appendFile.Key)
			if err != nil {
				fmt.Println("Error opening file:", err)
				return ""
			}
			originalFileData = append(originalFileData, appendFileData...)
			originalFileData = append(originalFileData, []byte("\n")...)
		}
		//write the merged file back to the temp file
		tempFilePath := filepath.Join(temp_dir, "cache_" + fileID)
		tempFilePath = tempFilePath + ".txt"
		err = os.WriteFile(tempFilePath, originalFileData, 0644)
		if err != nil {
			fmt.Println("Error writing file to temp:", err)
			return ""
		}
		fmt.Println("File " + fileID + " successfully logically merged")
		return tempFilePath
	}
}

func getFile(lc *LamportClock, fileID string, connTable *sync.Map, keyTable *sync.Map, self_id int, requestor_node_id int) bool {
	//steps
	//1. check if the file exists in cacheBay
	//2. if it does then show the file to the user
	//3. if it does not then first check if you are the cood for this file
	//4. if you are the cood, create a temp file with all the logical caches and add the file to cacheBay 
	//5. if you are not the cood, send a message to the cood to get the file (also loads the file in cacheBay)
	if requestor_node_id == self_id{
		res :=checkFileExists("CacheBay", "cache_" + fileID + ".txt")
		if res{
			fmt.Println("File "+ fileID +" already exists in locally in cache and is not stale")
			return true
		}
	}
	//get the coordinator for this file
	fileID_int, err := strconv.Atoi(fileID)
	if err != nil{
		fmt.Println("Error converting fileID to int")
		return false
	}
	coordinator := GetHyDFSCoordinatorID(keyTable, fileID_int)
	if coordinator == -1{
		fmt.Println("Not enough keys while checking for COOD")
		return false
	}
	if self_id == coordinator{
		//create the file in the cache bay
		//first check if the file exists in the file bay
		if !checkFileExists("FileBay", "original_" + fileID+".txt"){
			fmt.Println("File to be fetched does not exist at the supposed cood, file not found!")
			if requestor_node_id == self_id{
				fmt.Println("Requested File " + fileID + " does not exist in HYDFS.")
			}
			return false
		}
		//copy the file to the cache bay
		dir := GetDistributedLogQuerierDir()
		cachePath := filepath.Join(dir, "CacheBay", "cache_" + fileID+".txt")
		tmpPath := LogicalTempMerge(fileID, strconv.Itoa(requestor_node_id))
		//send the file to the requestor node
		if requestor_node_id == self_id{
			//copy the file to the cache bay
			err := copyFileContents(tmpPath, cachePath)
			if err != nil {
				fmt.Println("Error copying file to CacheBay:", err)
				return false
			}
			fmt.Println("File " + fileID + " successfully fetched and now can be read")
			//remove the temp file
			removeFile(tmpPath)
			return true
		}else{
			conn, ok := connTable.Load(requestor_node_id)
			if !ok{
				fmt.Println("Error loading conn from connTable")
				return false
			}
			fmt.Println("Sending cache file " + fileID + " to " + conn.(net.Conn).RemoteAddr().String())
			sendHyDFSFile(lc, conn.(net.Conn), "temp", "cache_" + fileID , "cache_" + fileID)
			//remove the temp file
			removeFile(tmpPath)
			return true
		}
	}else{
		//send the get message to the cood
		conn, ok := connTable.Load(coordinator)
		if !ok{
			fmt.Println("Error loading conn from connTable")
			return false
		}
		fmt.Println("Sending get file message to " + conn.(net.Conn).RemoteAddr().String())
		sendHyDFSMessage(lc, conn.(net.Conn), "GETFILE: " + fileID + " " + strconv.Itoa(requestor_node_id))
		return true
	}
	
}

//tries to find fileID in replica Bay or the Filebay, should be only called on the cood
func mergeFile(fileID string){
	//merge the file on this node
	//steps to merge the file
	//1. check if the file exists in the file bay
	//2. check if any logical appends exist for this file
	//3. collect the append number and sort them with lamport timestamps PER client
	//4. merge the file with the logical appends
	//5. remove the logical appends

	//check if the file exists in the file bay
	if !checkFileExists("FileBay", "original_" + fileID) && !checkFileExists("ReplicaBay", "replica_" + fileID){
		fmt.Println("File to be merged does not exist in the file bay or replica bay")
		return
	}
	//check if the file exists in the file bay OR the replica bay
	//get the file path
	dir := GetDistributedLogQuerierDir()
	filePath := filepath.Join(dir, "FileBay", "original_" + fileID)
	if checkFileExists("FileBay", "original_" + fileID){
		filePath = filepath.Join(dir, "FileBay", "original_" + fileID)
	}else if checkFileExists("ReplicaBay", "replica_" + fileID){
		filePath = filepath.Join(dir, "ReplicaBay", "replica_" + fileID)
	}
	//iterate over the appendsBay to search the logical appends for this file
	// the appendbay dir is structured as appendBay/nodeID/append_file

	appendDir := filepath.Join(dir, "appendBay")
	nodeDirs, err := os.ReadDir(appendDir)
	if err != nil {
		fmt.Println("Error reading append directory:", err)
		return
	}

	//use a queue to store the logical appends
	queue:= make([]string, 0)
	type StringIntPair struct {
		Key   string
		Value int
	}
	
	for _, nodeDir := range nodeDirs {
		//walk the nodeDir, if the file name contains the fileID, push the file path and int of lamport
		//timestamp to the node_list
		node_list:= make([]StringIntPair, 0)
		files, err := os.ReadDir(filepath.Join(appendDir, nodeDir.Name()))
		if err != nil {
			fmt.Println("Error reading append directory:", err)
			return
		}
		for _, file := range files {
			//filename like append_434_134_500.txt
			// 434 is nodeId, 134 is fileID, 500 is lamport timestamp
			fileID_parse := strings.Split(file.Name(), "_")[2] 
			if fileID_parse == fileID{
				//get the lamport timestamp
				fileBase := strings.Split(file.Name(), ".")[0]
				lamport_timestamp, err := strconv.Atoi(strings.Split(fileBase, "_")[3])
				if err != nil{
					fmt.Println("Error converting lamport string to int")
					return
				}
				node_list = append(node_list, StringIntPair{filepath.Join(appendDir, nodeDir.Name(), file.Name()), lamport_timestamp})
			}
		}
		//sort the node_list based on the lamport timestamp
		sort.Slice(node_list, func(i, j int) bool {
			return node_list[i].Value < node_list[j].Value
		})
		//push the file paths to the queue
		for _, pair := range node_list{
			queue = append(queue, pair.Key)
		}
	}
	if len(queue) == 0{
		fmt.Println("No logical appends found for file - NO OP" + fileID)
		return
	}
	//merge the file with the logical appends
	//first read the original file
	originalFileData, err := os.ReadFile(filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	//append in place the logical appends with newline 
	for _, appendFile := range queue{
		appendFileData, err := os.ReadFile(appendFile)
		if err != nil {
			fmt.Println("Error opening file:", err)
			return
		}
		originalFileData = append(originalFileData, appendFileData...)
		originalFileData = append(originalFileData, []byte("\n")...)
	}
	//write the merged file back to the original file
	err = os.WriteFile(filePath, originalFileData, 0644)
	if err != nil {
		fmt.Println("Error writing file:", err)
		return
	}
	//remove the logical appends
	for _, appendFile := range queue{
		removeFile(appendFile)
	}
	fmt.Println("File " + fileID + " successfully merged")
}

func removeFile(filePath string){
	//check if the file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		// if strings.Contains(filePath, "cache"){
		// 	//silent remove trial for cache files
		// 	return 
		// }
		fmt.Println("File does not exist for removal " + filePath)
		return
	}
	err := os.Remove(filePath)
	if err != nil {
		fmt.Println("Error removing file:", err)
	}
}
//only run this function if the append operation is called on the cood for that fileID
//does NOT call forward append to replica
func appendLocal(lc *LamportClock,local_filename string, toAppendFileName string, m int )string{
	//check if the fileID exists in the file bay
	fileID := GetFileID(toAppendFileName,m)
	if !checkFileExists("FileBay", "original_" + strconv.Itoa(fileID)){
		fmt.Println("File to append to does not exist in the file bay")
		return ""
	}
	//copy the contents of the file to the append bay
	dir := GetDistributedLogQuerierDir()
	src_path := filepath.Join(dir, "business", local_filename)
	append_file_name := "append_" + strconv.Itoa(getSelf_id())+"_"+strconv.Itoa(fileID) + "_" + strconv.Itoa(int(lc.timestamp)) + ".txt"
	//check if the directory exists
	appendBayDir := filepath.Join(dir, "appendBay", strconv.Itoa(getSelf_id()))
	if _, err := os.Stat(appendBayDir); os.IsNotExist(err) {
		os.Mkdir(appendBayDir, 0755)
	}
	dest_path := filepath.Join(appendBayDir, append_file_name)
	err := copyFileContents(src_path, dest_path)
	if err != nil {
		fmt.Println("Error copying file to append bay (local):", err)
		return ""
	}
	fmt.Println("File " + local_filename + " successfully appended to " + toAppendFileName)
	return dest_path
}

func appendFile(lc *LamportClock, connTable *sync.Map, keyTable *sync.Map, self_id int, local_filename string, toAppendFileName string, m int){
	//check if the fileID exists in the file bay
	fileID := GetFileID(toAppendFileName,m)
	coordinator := GetHyDFSCoordinatorID(keyTable, fileID)
	if coordinator == -1{
		fmt.Println("Not enough keys while checking for COOD")
		return
	}
	if self_id == coordinator{
		//append the file locally
		appendPath := appendLocal(lc, local_filename, toAppendFileName, m)
		if appendPath == ""{
			fmt.Println("Error appending file locally")
			return	
		}
		//fetch only the filename from the appendPath
		appendFileName := filepath.Base(appendPath)
		//forward the append to the replicas
		forwardAppendToReplica(lc, connTable, keyTable, self_id,appendPath, appendFileName)
	}else{
		//send the append message to the coordinator
		conn, ok := connTable.Load(coordinator)
		if !ok{
			fmt.Println("Error loading conn from connTable")
			return
		}
		fmt.Println("Sending append file to cood : " + conn.(net.Conn).RemoteAddr().String())
		append_file_name := "append_" + strconv.Itoa(getSelf_id())+"_"+strconv.Itoa(fileID) + "_" + strconv.Itoa(int(lc.timestamp)) + ".txt"
		sendHyDFSFile(lc, conn.(net.Conn), "business", local_filename, append_file_name)
	}
	//invalidate the cache for this file i.e. remove file from cacheBay
	removeFile(filepath.Join(GetDistributedLogQuerierDir(), "CacheBay", "cache_" + strconv.Itoa(fileID)+".txt"))
}
//requires actual filename, not the fileID
func checkFileExists(dir_name string, file_name string) bool{
	if !strings.Contains(file_name, ".txt"){
		file_name = file_name + ".txt"
	}
	dir := GetDistributedLogQuerierDir()
	filePath := filepath.Join(dir, dir_name, file_name)
	//fmt.Println("Checking if file exists " + filePath)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		//fmt.Println("File does not exist " + file_name + " in " + dir_name)
		return false
	}
	return true
}
func displayKeyTable(keyTable *sync.Map){
	keys := make([]int, 0)
	fmt.Println("HyDFS Nodes")
	keyTable.Range(func(k, v interface{}) bool {
		keys = append(keys, k.(int))
		fmt.Println(k, v)
		return true
	})
	//also show sorted keys 
	sort.Ints(keys)
	fmt.Println("Sorted Keys : ")
	for _, key := range keys{
		fmt.Print(strconv.Itoa(key) + " ")
	}
	fmt.Println("\n*****")
		
}

func sendHyDFSMessage(lc *LamportClock,conn net.Conn, msg string){
	conn.Write([]byte(msg + "END_OF_MSG\n"))
	lc.Increment()
}

func broadcastHyDFSMessage(lc *LamportClock,connTable *sync.Map, msg string){
	connTable.Range(func(key, value interface{}) bool {
		//do not send the message to the current node
		if key == self_id{
			return true
		}
		conn := value.(net.Conn)
		sendHyDFSMessage(lc, conn, msg)
		return true
	})
}

//file type can be one of replica, append, main, fileType is for source 
func sendHyDFSFile(lc *LamportClock, conn net.Conn, fileType string, local_filename string, sendFileName string){
	dir := GetDistributedLogQuerierDir()
	var filePath string
	if !strings.Contains(local_filename, ".txt"){
		local_filename = local_filename + ".txt"
	}
	if !strings.Contains(sendFileName, ".txt"){
		sendFileName = sendFileName + ".txt"
	}
	if fileType == "replica"{
		//send from the replica bay
		filePath = filepath.Join(dir, "ReplicaBay", local_filename)
	}else if fileType == "append"{
		//send from the append bay
		//filename must contain the nodeID dir when the type is append!
		filePath = filepath.Join(dir, local_filename)
	}else if fileType == "original"{
		//send from the file bay
		filePath = filepath.Join(dir, "FileBay", local_filename)
	}else if fileType=="business"{
		//send from the business dir
		filePath = filepath.Join(dir, "business", local_filename)
	}else if fileType=="temp"{
		filePath = filepath.Join(dir, "temp", local_filename)
	}else {
		fmt.Println("Invalid file type found in sendHyDFSFile")
		return
	}
	fmt.Println("Sending file " + local_filename + " to " + conn.RemoteAddr().String())
	fileData, err := os.ReadFile(filePath)
    if err != nil {
        fmt.Println("Error opening file:", err)
		return
    }
    // Create message with header
    header := fmt.Sprintf("FILE: %s %s\n", fileType, sendFileName)
    message := append([]byte(header), fileData...)
	//add end of message
	message = append(message, []byte("\nEND_OF_MSG\n")...)
    // Send the entire message
    _, err = conn.Write(message)
    if err != nil {
        fmt.Println("Error sending file:", err)
    }

	lc.Increment()
    fmt.Printf("File %s sent successfully\n", filepath.Base(filePath))
}

func receiveHyDFSFile(lc *LamportClock, message string) {
    lines := strings.SplitN(message, "\n", 2)
    if len(lines) < 2 {
       fmt.Println("Invalid file message format")
	   return 
    }
    // Parse the header
    header := strings.Fields(lines[0])
    if len(header) < 3 {
        fmt.Println("Invalid file message header")
    }
    fileName := header[2]
	fmt.Println("Received file " + fileName)
    // Save the file data
    fileData := []byte(lines[1])
	dir := GetDistributedLogQuerierDir()
	destPath := filepath.Join(dir, "ArrivalBay", fileName)
    if err := os.WriteFile(destPath, fileData, 0644); err != nil {
        fmt.Println("Error saving file:", err)
    }

    lc.Increment()
    fmt.Printf("File %s received and saved\n", fileName)
}

func forwardAppendToReplica(lc *LamportClock, connTable *sync.Map, keyTable *sync.Map ,self_id int , filePath string, fileName string){
	//get the successor to the current node
	suc_1, suc_2 := GetHYDFSSuccessorIDs(self_id, keyTable)
	//get conns for these successors
	conn1, ok1 := connTable.Load(suc_1)
	conn2, ok2 := connTable.Load(suc_2)
	if !ok1 || !ok2{
		fmt.Println("Error loading conn from connTable")
		return
	}
	fmt.Println("Forwarding append file " + fileName + " to " + conn1.(net.Conn).RemoteAddr().String() + filePath)
	fmt.Println("Forwarding append file " + fileName + " to " + conn2.(net.Conn).RemoteAddr().String() + filePath)

	//send the file to the successors
	sendHyDFSFile(lc, conn1.(net.Conn), "append", filePath, fileName)
	sendHyDFSFile(lc, conn2.(net.Conn), "append", filePath, fileName)
	fmt.Println("Append File " + fileName + " successfully forwarded to successors")
}

//TODO: optimize the keyTableAdd and keyTableRemove functions, store a readonly sorted list of keys in keyTable with key -1 or something
func GetHyDFSCoordinatorID(keyTable * sync.Map, fileID int) int {
	//search key table for the right co-ordinator
	//get the keys from the table and then sort them
	keys := make([]int, 0)
	keyTable.Range(func(k, v interface{}) bool {
		keys = append(keys, k.(int))
		return true
	})
	//sort the keys
	sort.Ints(keys)
	//find the position of the key in the sorted list
	index := sort.Search(len(keys), func(i int) bool {
		return keys[i] >= fileID
	})
	if index == len(keys){
		if len(keys) == 0{
			return -1
		}
		return keys[0]
	}
	return keys[index]
}

// logical extension of new co-ordinators etc
func KeyTableAdd(keyTable *sync.Map, key int){
	
	//special cases for len 0 and 1 
	//for 0 
	length := getSyncMapLength(keyTable)
	if length == 0{
		value := []int{key,key}
		keyTable.Store(key, value)
		return
	}else if length == 1{
		//for 1
		//get the only key in the table
		var key1 int
		keyTable.Range(func(k, v interface{}) bool {
			key1 = k.(int)
			return false
		})
		if key < key1{
			keyTable.Store(key1, []int{key1,key})
			keyTable.Store(key, []int{key,key1})
		}else{
			keyTable.Store(key1, []int{key,key1})
			keyTable.Store(key, []int{key1,key})
		}
	}else{
		//for len keyTable more than 1
		//get the keys from the table and then sort them 
		keys := make([]int, 0)
		keyTable.Range(func(k, v interface{}) bool {
			keys = append(keys, k.(int))
			return true
		})
		//sort the keys
		sort.Ints(keys)
		//find the position of the key in the sorted list
		index := sort.Search(len(keys), func(i int) bool {
			return keys[i] >= key
		})
		prev_key, nextkey := -1, -1
		
		if index == 0{
			prev_key = keys[len(keys)-1]
			nextkey = keys[0]
		}
		if index == len(keys){
			prev_key = keys[len(keys)-1]
			nextkey = keys[0]
		}
		if index > 0 && index < len(keys){
			prev_key = keys[index-1]
			nextkey = keys[index]
		}
		

		//update the keyTable
		keyTable.Store(nextkey, []int{key,nextkey})
		keyTable.Store(key, []int{prev_key,key})
	}
}

// logical shrinking of the key table
func KeyTableRemove(keyTable *sync.Map, key int){
	lenKT := getSyncMapLength(keyTable)
	if lenKT == 0{
		return
	}else if lenKT == 1{
		keyTable.Delete(key)
	}else{
		//first get the value for that key 
		val,ok := keyTable.Load(key)
		if !ok{
			fmt.Println("Error loading key from keyTable")
			return
		}
		intSlice,ok  := val.([]int)
		if !ok{
			fmt.Println("Error converting interface to int slice")
			return
		}
		//find the next_key to this key
		keys := make([]int, 0)
		keyTable.Range(func(k, v interface{}) bool {
			keys = append(keys, k.(int))
			return true
		})
		//sort the keys
		sort.Ints(keys)
		//find the position of the key in the sorted list
		index := sort.Search(len(keys), func(i int) bool {
			return keys[i] >= key
		})
	
		// Check if the found index actually contains the target value
		if index >= len(keys) || keys[index] != key {
			return 
		}
		//get the next key
		next_key := -1
		if index == len(keys)-1{
			next_key = keys[0]
		}else{
			next_key = keys[index+1]
		}
		stitch_key := intSlice[0]
		keyTable.Store(next_key, []int{stitch_key, next_key})
		keyTable.Delete(key)
	}
}

func ConnTableAdd(connTable *sync.Map, key int, conn net.Conn){
	 connTable.Store(key, conn)
}
func ConnTableRemove(connTable *sync.Map, key int){
	connTable.Delete(key)
}

// getPeerID generates a peer ID by hashing the IP address and port,
// then truncating the result to m bits.
func GetPeerID(ipAddress string, m int) int {
	identifier := ipAddress
	
	// Hash the identifier using SHA-1
	hash := sha1.Sum([]byte(identifier))
	
	// Convert the first m bits of the hash to an integer
	bitLength := m / 8
	if m%8 != 0 {
		bitLength += 1
	}
	
	// Truncate the hash to the required number of bits
	hashTruncated := hash[:bitLength]

	// Convert truncated hash to big.Int and shift to ensure only m bits are used
	bigIntHash := new(big.Int).SetBytes(hashTruncated)
	bigIntMod := new(big.Int).Lsh(big.NewInt(1), uint(m))
	peerID := new(big.Int).Mod(bigIntHash, bigIntMod)

	return int(peerID.Int64())
}
// getFileID generates a file ID by hashing the filename and truncating the result to m bits.
func GetFileID(filename string, m int) int {
	// Hash the filename using SHA-1
	hash := sha1.Sum([]byte(filename))
	
	// Calculate the number of bytes needed to cover m bits
	bitLength := m / 8
	if m%8 != 0 {
		bitLength += 1
	}

	// Truncate the hash to the required number of bits
	hashTruncated := hash[:bitLength]

	// Convert truncated hash to big.Int and apply modulo 2^m
	bigIntHash := new(big.Int).SetBytes(hashTruncated)
	bigIntMod := new(big.Int).Lsh(big.NewInt(1), uint(m))
	fileID := new(big.Int).Mod(bigIntHash, bigIntMod)
	fmt.Println("FileID: for  " + filename + " is ", int(fileID.Int64()))
	return int(fileID.Int64())
}

func GetAddressfromHash(hash *string) string {
	tokens := strings.Split(*hash, "-")
	return tokens[0]
}

func GetDistributedLogQuerierDir() string{
	//for local testing call self_id 
	_,currentFile,_,_ := runtime.Caller(0)
	dir := filepath.Dir(currentFile)
	dir  = filepath.Dir(dir)
	dir = filepath.Dir(dir)
	self_id := getSelf_id()
	dir = filepath.Join(dir, "Nuke")
	dir = filepath.Join(dir, "Node" + strconv.Itoa(self_id))
	//create the directory if it does not exist
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.Mkdir(dir, 0755)
		fmt.Println("Directory created at " + dir)
		//create the file bay, append bay, replica bay, cache bay
		os.Mkdir(filepath.Join(dir, "FileBay"), 0755)
		os.Mkdir(filepath.Join(dir, "appendBay"), 0755)
		os.Mkdir(filepath.Join(dir, "ReplicaBay"), 0755)
		os.Mkdir(filepath.Join(dir, "CacheBay"), 0755)
		os.Mkdir(filepath.Join(dir, "ArrivalBay"), 0755)
		os.Mkdir(filepath.Join(dir, "business"), 0755)
		os.Mkdir(filepath.Join(dir, "temp"), 0755)
		os.Mkdir(filepath.Join(dir, "Fetched"), 0755)
	}
	return dir
}

// Helper function to copy file contents
func copyFileContents(src, dst string) error {
    sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
    defer sourceFile.Close()
    destFile, err := os.Create(dst)
    if err != nil {
        return err
    }
    defer destFile.Close()

    _, err = io.Copy(destFile, sourceFile)
	fmt.Println("Copy Done")
    return err
}

//function to be only called from failure detection code (MP2)
func sendUpdateToHYDFS(hydfsConn *SafeConn, message string){
	_, err := hydfsConn.SafeWrite([]byte(message + "END_OF_MSG\n"))
	if err != nil {
		fmt.Println("Error sending message to HYDFS:", err)
	}
}

//currentlyassumes that the nodehash is in the format "ip:port"
func convertNodeHashForHYDFS(nodeHash string) string {
	//TODO: start here, check what the nodehash actually is
	token := strings.Split(nodeHash, "-")[0]
	port := strings.Split(token, ":")[1]
	address:= strings.Split(token, ":")[0]
	a_int, err :=strconv.Atoi(port)
	if err != nil {
		fmt.Println("Error in converting string to int in port")
		return ""
	}
	a_int = a_int - 2020
	return address + ":" + strconv.Itoa(a_int)
	
}

//function expects the Sync.Map to be key (nodehash):string, value (status): string 
func AddToMembershipList(membershipList *sync.Map, nodeHash string, incarnationNum int, hydfsConn *SafeConn) {
	string_val := "ALIVE" + "$" + strconv.Itoa(incarnationNum)
	//type  10.193.209.248:8081-1.0
	membershipList.Store(nodeHash, string_val)
	//send update to hydfs layer
	
	sendUpdateToHYDFS(hydfsConn, "ADD " + convertNodeHashForHYDFS(nodeHash))
}
func AddToMembershipListWithStatus(membershipList *sync.Map, nodeHash string, status string, incarnationNum int, hydfsConn *SafeConn) {
	string_val := status + "$" + strconv.Itoa(incarnationNum)
	fmt.Println("Adding to membership list ", string_val)
	membershipList.Store(nodeHash, string_val)
	//send update to hydfs layer
	sendUpdateToHYDFS(hydfsConn, "ADD " + convertNodeHashForHYDFS(nodeHash))
}

func DeleteFromMembershipList(membershipList *sync.Map, nodeHash string, hydfsConn *SafeConn) {
	membershipList.Delete(nodeHash)
	//send update to hydfs layer
	sendUpdateToHYDFS(hydfsConn, "REMOVE " + convertNodeHashForHYDFS(nodeHash))
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
	k := (totalNodes*2)/3
	if(totalNodes<=5){
		k = totalNodes - 1
	}
	fmt.Println("K -> ", k)
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
