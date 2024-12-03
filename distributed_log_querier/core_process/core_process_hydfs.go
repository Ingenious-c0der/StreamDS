package distributed_log_querier

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)


var self_id int //node id of the current node

func getSelf_id() int{
	return self_id 
}

func handleHyDFS(lc *LamportClock,conn net.Conn, keyTable *sync.Map, connTable *sync.Map, fileNameMap *sync.Map,  wg *sync.WaitGroup, m int){
	//read the message from the connection
	defer wg.Done()
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		msg, err := readMultilineMessage(reader, "END_OF_MSG")
		if err != nil {
			fmt.Println("Connection closed unexpectedly with " + conn.RemoteAddr().String())
			conn.Close()
			return 
		}
		msg = strings.Trim(msg, "\n")
		//fmt.Println("Received message: " + msg + " from " + conn.RemoteAddr().String())
		lc.Increment()
		go func(msg string){
			if strings.Contains(msg, "REPEXIST"){
				//check if a replica exists on this node
				file_ID := strings.Split(msg, " ")[1]
				fileName := "replica_" + file_ID
				res := checkFileExists("ReplicaBay",fileName)
				if res{
					sendHyDFSMessage(lc, conn, "REPRESP " + file_ID + " TRUE")
				}else{
					sendHyDFSMessage(lc, conn, "REPRESP " + file_ID + " FALSE")
				}
			}else if strings.Contains(msg, "MAPSFILE: ") {
				//MAPSFILE file_ID original_file_name
				fileNameMap.Store(strings.Split(msg, " ")[1], strings.Split(msg, " ")[2])
				
				}else if strings.Contains(msg, "REPRESP"){
				file_ID := strings.Split(msg, " ")[1]
				response := strings.Split(msg, " ")[2]
				//fmt.Println("Replication response for file " + file_ID + " is " + response + " from " + conn.RemoteAddr().String())
				if response == "FALSE"{
					lc.Increment()
					//replicate the file on that node
					
					fmt.Println("Replicating file " + file_ID + " to " + conn.RemoteAddr().String())
					forwardReplica(lc, conn, file_ID, self_id ) //handles forward of replica and appends both
				}
			}else if strings.Contains(msg, "ISCOOD"){
				// check if you are the COOD for the given file_ID 
				file_ID := strings.Split(msg, " ")[1]
				fileName:= "original_" + file_ID
				res := checkFileExists("FileBay",fileName)
				if res{
					sendHyDFSMessage(lc, conn, "COODRESP " + file_ID + " "+ strconv.Itoa(self_id) +" TRUE")
				}else{
					sendHyDFSMessage(lc, conn, "COODRESP " + file_ID + strconv.Itoa(self_id) + " FALSE")
				}
			}else if strings.Contains(msg, "COODRESP"){
				//COODRESP fileID SelfID response
				//coordinate for that file has left or crashed.
				//you should only take over if you are the next node in line
				//else ignore
				tokens:= strings.Split(msg, " ")
				file_ID,err := strconv.Atoi(tokens[1])
				if err != nil{
					fmt.Println("Error converting file_ID to int")
					return 
				}
				
				reject_cood_id,err := strconv.Atoi(tokens[2])
				if err != nil{
			
					fmt.Println("Error converting reject_cood_id to int")
					fmt.Println("Reject id for coodresp :" + tokens[2])
					fmt.Println("Received message : " + msg)
					return
				}
				response := tokens[3]
				if response == "FALSE"{
					//find the correct cood for this file
					next_cood_id:= GetHyDFSCoordinatorID(keyTable, file_ID)
					if next_cood_id == -1{
						fmt.Println("Not enough keys while checking for COOD")
						return
					}
					if next_cood_id!= reject_cood_id{
						fmt.Println("IMP! Slack detected, INFO: Contacted a node that should not be contacted to check if they are COOD, this can happen if there are delays in KeyTable updates")
					}
					if next_cood_id == self_id{
						//take over as the COOD
						//move file from replicabay to filebay
						//remove local replica for this file
						dir := GetDistributedLogQuerierDir()
						replica_file_name := "replica_" + strconv.Itoa(file_ID)
						original_file_name := "original_" + strconv.Itoa(file_ID)
						err := copyFileContents(filepath.Join(dir, "ReplicaBay", replica_file_name), filepath.Join(dir, "FileBay", original_file_name))
						if err != nil{
							fmt.Println("Error copying file from ReplicaBay to FileBay")
						}else{
							removeFile(filepath.Join(dir, "ReplicaBay", replica_file_name))
						}
						fmt.Println("I am the COOD for file " + strconv.Itoa(file_ID))
					}else{
						fmt.Println("Cood for file " + strconv.Itoa(file_ID) + " was lost, but its " + strconv.Itoa(next_cood_id) + " duty to handle that mess, not me!")
					}

				}
			}else if strings.Contains(msg, "GETFILE:"){
				//GETFILE: fileID requestorID
				file_ID := strings.Split(msg, " ")[1]
				requestor_ID := strings.Split(msg, " ")[2]
				requestor_ID_int,err := strconv.Atoi(requestor_ID)
				if err != nil{
					fmt.Println("Error converting requestor_ID to int")
					return
				}
				res:= getFile(lc, file_ID, connTable,keyTable, self_id, requestor_ID_int)
				if !res{
					//send the file not found message to requestor
					sendHyDFSMessage(lc, conn, "NOTFOUND: " + file_ID + " " + requestor_ID)
				}
			}else if strings.HasPrefix(msg, "FILE:") && !strings.Contains(msg, "GETFILE:"){
				// Pass the entire message to receiveHyDFSFile for further handling
				receiveHyDFSFile(lc, msg)
				
			}else if strings.Contains(msg, "CHECKEXIST"){
				//mostly to make sure duplicate files are not created
				//check if the file exists on this node
				//CHECKEXIST file_ID original_file_name hydfs_file_name
				file_ID := strings.Split(msg, " ")[1]
				original_file_name := strings.Split(msg, " ")[2]
				hydfs_file_name := strings.Split(msg, " ")[3]
				res := checkFileExists("FileBay", "original_" + file_ID)
				if res{
					sendHyDFSMessage(lc, conn, "CHECKRESP " + file_ID + " TRUE " + original_file_name + " " + hydfs_file_name)
				}else{
					sendHyDFSMessage(lc, conn, "CHECKRESP " + file_ID + " FALSE " + original_file_name + " " + hydfs_file_name)
				}
			}else if strings.Contains(msg, "CHECKRESP"){
				tokens := strings.Split(msg, " ")
				file_ID := tokens[1]
				response := tokens[2]
				original_file_name := tokens[3]
				hydfs_file_name := tokens[4]
				fmt.Println("Check response for file " + file_ID + " is " + response)
				if response == "FALSE"{
					//send the file to that node
					fileName := "original_" + file_ID + ".txt" +"+" + hydfs_file_name
					sendHyDFSFile(lc, conn, "business", original_file_name, fileName)
					fmt.Println("File " + hydfs_file_name + " is now being created in HYDFS at " + conn.RemoteAddr().String())
				}else{
					fmt.Println("File " + hydfs_file_name + " already exists in HYDFS!")
				}
			}else if strings.Contains(msg, "MERGEFILE"){
				//merge the file on this node
				file_ID := strings.Split(msg, " ")[1]
				fmt.Println("Now Merging file " + file_ID)
				//check if the current node is the COOD for this file
				//if it is, then merge the file, AND forward the MERGE message to successors
				file_ID_int,err := strconv.Atoi(file_ID)
				if err != nil{
					fmt.Println("Error converting file_ID to int")
					return
				}
				file_cood := GetHyDFSCoordinatorID(keyTable, file_ID_int)
				if file_cood == -1{
					fmt.Println("Not enough keys while checking for COOD")
					return
				}
				mergeFile(file_ID)
				//remove self cache for this file
				removeFile(filepath.Join(GetDistributedLogQuerierDir(), "CacheBay", "cache_" + file_ID + ".txt"))
				if file_cood == self_id{
					//forward the merge message to the successors
					fmt.Println("Forwarding merge to replicas")
					x,y := GetHYDFSSuccessorIDs(file_cood, keyTable)
					if x == -1 || y == -1{
						fmt.Println("Error getting the successors for file " + file_ID)
						return
					}
					fmt.Println("Successors for file " + file_ID + " are " + strconv.Itoa(x) + " and " + strconv.Itoa(y))
					//get the conn for these nodes
					conn_x, okx := connTable.Load(x)
					if okx{
						sendHyDFSMessage(lc, conn_x.(net.Conn), "MERGEFILE " + file_ID)
					}
					conn_y, oky := connTable.Load(y)
					if oky{
						sendHyDFSMessage(lc, conn_y.(net.Conn), "MERGEFILE " + file_ID)
					}
					//broadcast merge message to all the nodes just for cache invalidation
					broadcastHyDFSMessage(lc, connTable, "MERGED " + file_ID)
				}
			}else if strings.Contains(msg, "MERGED"){
				//remove the cache for this file
				file_ID := strings.Split(msg, " ")[1]
				//check if exists
				if checkFileExists("CacheBay", "cache_" + file_ID){
					removeFile(filepath.Join(GetDistributedLogQuerierDir(), "CacheBay", "cache_" + file_ID))
				}
			}else if strings.Contains(msg, "NOTFOUND:"){
				file_ID := strings.Split(msg, " ")[1]
				fmt.Println("Requested file does not exist in HYDFS " + file_ID)
			}else if strings.Contains(msg, "GETINFO:"){
				//GETINFO: fileID 
				file_ID := strings.Split(msg, " ")[1]
				support := GetLocalSupportForFile(file_ID)
				sendHyDFSMessage(lc, conn, "INFORESP: " + file_ID + "_" + support)
			}else if strings.Contains(msg, "INFORESP:"){
				//INFORESP: fileID_support
				tokens := strings.Split(msg, "_")
				if len(tokens[1:])> 1 {
					fmt.Println(tokens[1:])
				}
			
			}else if strings.HasPrefix(msg, "REQUESTREPLICA:"){
				//REQUESTREPLICA: fileID
				file_ID := strings.Split(msg, " ")[1]
				//check if the file exists on this node
				res := checkFileExists("ReplicaBay", "replica_" + file_ID)
				if res{
					//send the file to the requestor
					sendHyDFSFile(lc, conn, "replica", "replica_"+ file_ID, "cache_replica_" + file_ID)
				}
			}else if strings.HasPrefix(msg, "LAUNCHAPPEND:"){
				//LAUNCHAPPEND: fileID local_filename hyDFSFileName
				//read from local filename and append to the fileID 
				fmt.Println("Received multiappend request")
				tokens := strings.Split(msg, " ")
				file_ID := tokens[1]
				local_filename := tokens[2]
				hydfs_file_name := tokens[3]
				fmt.Println("Appending to file " + file_ID + " from " + strconv.Itoa(self_id)+"@"+local_filename)
				appendFile(lc, connTable, keyTable, self_id, local_filename, hydfs_file_name, m)

			}
	}(msg)
}}

func createTCPConn(lc *LamportClock,address string, keyTable *sync.Map,connTable *sync.Map, fileNameMap *sync.Map, wg *sync.WaitGroup, m int) (bool, net.Conn){
	//make sure its not self connect, for local check if it is the same address AND same port
	fmt.Println(address, GetOutboundIP().String() + ":" + os.Getenv("HGP"))
	if address == GetOutboundIP().String() + ":" + os.Getenv("HGP"){
		fmt.Println("Error: Cannot connect to self")
		return false, nil
	}
	
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Println("Error dialing", err.Error())
		return false, conn 
	}
	fmt.Println("Connected to new node at " + address)
	wg.Add(1)
	go handleHyDFS(lc, conn, keyTable, connTable,fileNameMap, wg,m)
	return true, conn
}

func verifyCloseTCPConn(conn net.Conn, wg *sync.WaitGroup){
	defer wg.Done()
	defer conn.Close()
}

//also handles MP4 i.e. stream DS pipeline
func handleHyDFSMeta(lc *LamportClock,conn net.Conn, keyTable *sync.Map, connTable *sync.Map, fileNameMap *sync.Map,token string, metaConnMap *sync.Map, wg *sync.WaitGroup,hyDFSGlobalPort string, m int ){
	defer wg.Done()
	defer conn.Close()
	reader := bufio.NewReader(conn)
	if getSyncMapLength(keyTable) == 10{
		fmt.Println("Relaying current state of connections to StreamDS Layer")
		//the case where mp4 connects after all the joins are done. 
		//send all the connections to the MP4
		//iterate over conn table to find remote addr and send it to MP4
		connTable.Range(func(key, value interface{}) bool {
			fmt.Println(value.(net.Conn).RemoteAddr().String())
			sendHyDFSMessage(lc, conn, "ADD " + convertNodeHashForStreamDS(value.(net.Conn).RemoteAddr().String()))
			return true
		})
		fmt.Println("Relayed current state of connections to StreamDS Layer")

	}
	for {
		msg, err := readMultilineMessage(reader, "END_OF_MSG")
		if err != nil {
			fmt.Println("Connection closed unexpectedly with " + conn.RemoteAddr().String())
			fmt.Println("Self TCP Pipe Broke! HyDFS Can no longer receive updates")
			return 
		}
		msg = strings.Trim(msg, "\n")
		go func(msg string){
			if strings.Contains(msg, "ADD"){
				//lands in as ADD address:PORT
				msg = strings.Split(msg, " ")[1]
				//expecting to receive address:UDPport here as token 	
				//VM MARKER
				// address := strings.Split(msg, ":")[0] 
				// address +=":"+ hyDFSGlobalPort 
				// msg = address
				//VM MARKER END
				nodeID := GetPeerID(msg, m)
				fmt.Println("Adding node " + strconv.Itoa(nodeID) + " at " + msg)
				KeyTableAdd(keyTable, nodeID)
				if succ, conn := createTCPConn(lc,msg, keyTable, connTable, fileNameMap, wg,m); succ {
					ConnTableAdd(connTable, nodeID, conn)
					fmt.Printf("Connected to %s - %s successfully\n", conn.RemoteAddr().String(), msg)
				} else {
					fmt.Printf("Failed to connect to %s", msg)
				}
				//TODO implement ringRepair function here
				//relay the add message to StreamDS layer
				metaConnMap.Range(func(key, value interface{}) bool {
					if key != token{
						sendHyDFSMessage(lc, value.(net.Conn), "ADD " + convertNodeHashForStreamDS(msg))
					}
					return true
				})
			}else if strings.Contains(msg, "REMOVE"){
				msg = strings.Split(msg, " ")[1]
				//VM MARKER
				// address := strings.Split(msg, ":")[0]
				// address += ":"+ hyDFSGlobalPort //TODO: make sure this works out
				// msg = address
				//VM MARKER END
				nodeID := GetPeerID(msg, m)
				KeyTableRemove(keyTable, nodeID)
				//get the conn for the removed node
				conn_remove, ok := connTable.Load(nodeID)
				if ok{
					verifyCloseTCPConn(conn_remove.(net.Conn), wg)
				}
				ConnTableRemove(connTable, nodeID)
				fmt.Println("Removed node " + strconv.Itoa(nodeID) + " at " + msg)
				//relay the remove message to StreamDS layer
				//use metaConnMap to distinguish between failure detection connection and streamDS connection
				metaConnMap.Range(func(key, value interface{}) bool {
					if key != token{
						sendHyDFSMessage(lc, value.(net.Conn), "REMOVE " + convertNodeHashForStreamDS(msg))
					}
					return true
				})
			
			}else if strings.Contains(msg, "TASKAPPEND: "){
				//TASKAPPEND: local_filename hydfs_file_name
				tokens := strings.Split(msg, " ")
				local_filename := tokens[1]
				hyDFSFileName := tokens[2]
				appendFile(lc, connTable, keyTable, self_id,local_filename, hyDFSFileName, m)
			}else if strings.Contains(msg, "TASKGET: "){
				//TASKGET: hydfs_file_name local_filename
				tokens := strings.Split(msg, " ")
				hyDFSFileName := tokens[1]
				local_filename := tokens[2]
				//trim the newline character
				hyDFSFileName = strings.Trim(hyDFSFileName, "\n")
				local_filename = strings.Trim(local_filename, "\n")
				fileID:= GetFileID(hyDFSFileName,m)
				getFile(lc, strconv.Itoa(fileID), connTable, keyTable, self_id, self_id)
				FetchCache(strconv.Itoa(fileID), local_filename, fileNameMap)
				//notify the connection that the file is now available
				//check if this works
				sendHyDFSMessage(lc, conn, "TASKGETRESP: " + hyDFSFileName + " " + local_filename)
			}else if strings.Contains(msg, "CREATEFILE: "){
				//CREATEFILE: hydfs_file_name
				//use an empty file ALWAYS present locally to create the file 
				tokens := strings.Split(msg, " ")
				hyDFSFileName := tokens[1]
				local_filename := "empty.txt" // hardcoded empty file
				createFile(lc,local_filename, hyDFSFileName, connTable, keyTable, fileNameMap, self_id,  m)
				fmt.Println("Created file " + hyDFSFileName)
			}else if strings.HasPrefix(msg, "TASKMERGE: "){
				//format TASKMERGE: file_name
				tokens := strings.Split(msg, " ")
				fileName := tokens[1]
				fileID := GetFileID(fileName,m)
							//check if the current node is the COOD for this file
			cood := GetHyDFSCoordinatorID(keyTable, fileID)
			if cood == -1{
				fmt.Println("Not enough keys while checking for COOD")
				return
			}
			if cood == self_id{
				mergeFile(strconv.Itoa(fileID))
				//remove self cache for this file
				removeFile(filepath.Join(GetDistributedLogQuerierDir(), "CacheBay", "cache_" + strconv.Itoa(fileID) + ".txt"))
				//forward the merge message to the successors
				x,y := GetHYDFSSuccessorIDs(cood, keyTable)
				if x == -1 || y == -1{
					fmt.Println("Error getting the successors for file " + strconv.Itoa(fileID))
					return
				}
				fmt.Println("Successors for file " + strconv.Itoa(fileID) + " are " + strconv.Itoa(x) + " and " + strconv.Itoa(y))
				//get the conn for these nodes
				conn_x, okx := connTable.Load(x)
				if okx{
					sendHyDFSMessage(lc, conn_x.(net.Conn), "MERGEFILE " + strconv.Itoa(fileID))
				}
				conn_y, oky := connTable.Load(y)
				if oky{
					sendHyDFSMessage(lc, conn_y.(net.Conn), "MERGEFILE " + strconv.Itoa(fileID))
				}
				//broadcast merge message to all the nodes just for cache invalidation
				broadcastHyDFSMessage(lc, connTable, "MERGED " + strconv.Itoa(fileID))
			}else{
				//send the merge message to the COOD
				cood_conn, ok := connTable.Load(cood)
				if ok{
					sendHyDFSMessage(lc, cood_conn.(net.Conn), "MERGEFILE " + strconv.Itoa(fileID))
				}
			}

			}
			//TODO: could do this where we start HyDFS Global operations only once we are connected
			// to MP2 pipe
			// else if strings.Contains(msg, "INIT"){
			// 	go StartHyDFSListener()
			// }
		}(msg)
}
}

func StartPipe(lc *LamportClock,pipe_port string, hyDFSGlobalPort string, keyTable *sync.Map, connTable *sync.Map, fileNameMap *sync.Map, metaConnMap *sync.Map ,wg *sync.WaitGroup, m int){
	listener, err := net.Listen("tcp", ":"+pipe_port)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	defer listener.Close()
	fmt.Println("Listening on Self Pipe " + listener.Addr().String())
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		metaConnMap.Store(conn.RemoteAddr().String(), conn)
		token:= strconv.Itoa(getSyncMapLength(metaConnMap))
		go handleHyDFSMeta(lc, conn, keyTable, connTable, fileNameMap, token ,metaConnMap, wg, hyDFSGlobalPort, m)
	}
}

func StartHyDFSListener(lc *LamportClock, hyDFSGlobalPort string, keyTable *sync.Map, connTable *sync.Map, fileNameMap *sync.Map, wg *sync.WaitGroup, m int){
	wg.Add(1)
	listener, err := net.Listen("tcp", ":"+hyDFSGlobalPort)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	defer listener.Close()
	fmt.Println("Listening Globally on " + listener.Addr().String())
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		go handleHyDFS(lc, conn, keyTable, connTable,fileNameMap, wg,m)
	}
}

func SetupHyDFSCommTerminal(lc *LamportClock, hyDFSGlobalPort string, keyTable *sync.Map, connTable *sync.Map, fileNameMap *sync.Map,  wg *sync.WaitGroup, m int,stopchan chan struct{}){
	wg.Add(1)
	defer wg.Done()
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println(">>>")
		text, _ := reader.ReadString('\n')
		if strings.HasPrefix(text, "create"){
			tokens:= strings.Split(text, " ")
			//check if the command is valid
			if len(tokens) != 3{
				fmt.Println("Invalid create command")
				continue
			}
			local_filename := tokens[1]
			hyDFSFileName := tokens[2]
			//trim the newline character
			hyDFSFileName = strings.Trim(hyDFSFileName, "\n")
			createFile(lc,local_filename, hyDFSFileName, connTable, keyTable, fileNameMap, self_id,  m)

		}else if strings.HasPrefix(text, "append"){
			//append to a file
			//check if the command is valid
			tokens:= strings.Split(text, " ")
			if len(tokens) != 3{
				fmt.Println("Invalid append command")
				continue
			}
			local_filename := tokens[1]
			hyDFSFileName := tokens[2]
			//trim the newline character
			hyDFSFileName = strings.Trim(hyDFSFileName, "\n")
			appendFile(lc, connTable, keyTable, self_id,local_filename, hyDFSFileName, m)
		}else if strings.HasPrefix(text, "get") && !strings.Contains(text, "getfromreplica"){
			//get a file
			tokens:= strings.Split(text, " ")
			if len(tokens) != 3{
				fmt.Println("Invalid get command")
				continue
			}
			hyDFSFileName := tokens[1]
			local_filename := tokens[2]
			//trim the newline character
			hyDFSFileName = strings.Trim(hyDFSFileName, "\n")
			local_filename = strings.Trim(local_filename, "\n")
			fileID:= GetFileID(hyDFSFileName,m)
			getFile(lc, strconv.Itoa(fileID), connTable, keyTable, self_id, self_id)
			//add the local filename construct later, can add a fetched dir
			go FetchCache(strconv.Itoa(fileID), local_filename, fileNameMap)
		}else if strings.HasPrefix(text, "merge"){
			tokens:= strings.Split(text, " ")
			if len(tokens) != 2{
				fmt.Println("Invalid merge command")
				continue
			}
			//trim the newline character
			fileName:= tokens[1]
			fileName = strings.Trim(fileName, "\n")
			fileID := GetFileID(fileName,m)
			//check if the current node is the COOD for this file
			cood := GetHyDFSCoordinatorID(keyTable, fileID)
			if cood == -1{
				fmt.Println("Not enough keys while checking for COOD")
				return
			}
			if cood == self_id{
				mergeFile(strconv.Itoa(fileID))
				//remove self cache for this file
				removeFile(filepath.Join(GetDistributedLogQuerierDir(), "CacheBay", "cache_" + strconv.Itoa(fileID) + ".txt"))
				//forward the merge message to the successors
				x,y := GetHYDFSSuccessorIDs(cood, keyTable)
				if x == -1 || y == -1{
					fmt.Println("Error getting the successors for file " + strconv.Itoa(fileID))
					return
				}
				fmt.Println("Successors for file " + strconv.Itoa(fileID) + " are " + strconv.Itoa(x) + " and " + strconv.Itoa(y))
				//get the conn for these nodes
				conn_x, okx := connTable.Load(x)
				if okx{
					sendHyDFSMessage(lc, conn_x.(net.Conn), "MERGEFILE " + strconv.Itoa(fileID))
				}
				conn_y, oky := connTable.Load(y)
				if oky{
					sendHyDFSMessage(lc, conn_y.(net.Conn), "MERGEFILE " + strconv.Itoa(fileID))
				}
				//broadcast merge message to all the nodes just for cache invalidation
				broadcastHyDFSMessage(lc, connTable, "MERGED " + strconv.Itoa(fileID))
			}else{
				//send the merge message to the COOD
				cood_conn, ok := connTable.Load(cood)
				if ok{
					sendHyDFSMessage(lc, cood_conn.(net.Conn), "MERGEFILE " + strconv.Itoa(fileID))
				}
			}

		}else if strings.HasPrefix(text, "exit"){
			//stop the ping channel and exit
			//close the listener
			close(stopchan)
			break
		}else if strings.HasPrefix(text, "listmemids"){
			//display the keytable
			fmt.Println("Total : " + strconv.Itoa(getSyncMapLength(keyTable)))
			displayKeyTable(keyTable)
		}else if strings.HasPrefix(text, "showfilemap"){
			//fileName map is a sync mapo
			fileNameMap.Range(func(key, value interface{}) bool {
				fmt.Println("File ID: ", key, " File Name: ", value)
				return true
			})
		}else if strings.HasPrefix(text, "self"){
			fmt.Println("Self ID: ", self_id)
		}else if strings.HasPrefix(text, "store"){
			ShowLocalFiles(fileNameMap)
		}else if strings.HasPrefix(text,"ls"){
			//broadcast the 
			//trim the newline character
			text = strings.Trim(text, "\n")
			//check if it contains 2 tokens
			if len(strings.Split(text, " ")) != 2{
				fmt.Println("Invalid ls command")
				continue
			}
			fileName := strings.Split(text, " ")[1]
			file_ID := GetFileID(fileName,m)
			//broadcast to all nodes for information
			broadcastHyDFSMessage(lc, connTable, "GETINFO: " + strconv.Itoa(file_ID) + " " + strconv.Itoa(self_id))
			supportlocal := GetLocalSupportForFile(strconv.Itoa(file_ID))
			if supportlocal!=""{
				fmt.Println(supportlocal)
			}
			time.Sleep(1 * time.Second)
		}else if strings.HasPrefix(text, "getfromreplica"){
			//getfromreplica NodeID filename localfilename
			tokens:= strings.Split(text, " ")
			if len(tokens) != 4{
				fmt.Println("Invalid getfromreplica command")
				continue
			}
			nodeID := tokens[1]
			fileName := tokens[2]
			localFileName := tokens[3]
			//trim the newline character
			localFileName = strings.Trim(localFileName, "\n")
			//get the conn for the node
			nodeID_int,err := strconv.Atoi(nodeID)
			fileID := GetFileID(fileName,m)
			if err != nil{
				fmt.Println("Error converting nodeID to int")
				return
			}
			conn, ok := connTable.Load(nodeID_int)
			if ok{
				sendHyDFSMessage(lc, conn.(net.Conn), "REQUESTREPLICA: " + strconv.Itoa(fileID))
				go FetchCache("replica_"+ strconv.Itoa(fileID) , localFileName, fileNameMap)
			}

		}else if strings.HasPrefix(text, "multiappend"){
			//multiappend hydfsFileName NodeID1 NodeID2 NodeID3.. + localFileName1 localFileName2 localFileName3..
			//trim the newline character
			text = strings.Trim(text, "\n")
			//check if it contains + 
			if !strings.Contains(text, "+"){
				fmt.Println("Error: Invalid multiappend command")
				continue
			}
			split_command := strings.Split(text, "+")
			first_split_tokens:= strings.Split(split_command[0], " ") // contains all the node ids
			hydfsFileName := first_split_tokens[1]
			//fmt.Println("HyDFS File Name: ", hydfsFileName)
			//fmt.Println("Node IDs: ", first_split_tokens[2:])
			//fmt.Println("Local File Names: ", split_command[1])
			i:=0
			second_split_tokens := strings.Split(split_command[1], " ") //contains all the file names
			//ensure that vm id and filenames match up
			//fmt.Println("Number of node IDs: ", len(first_split_tokens[2:]))
			//fmt.Println("Number of local file names: ", len(second_split_tokens))
			if len(first_split_tokens[2:]) != len(second_split_tokens){
				fmt.Println("Error: Number of node IDs and local file names do not match")
				continue
			}
			for _,nodeID := range first_split_tokens[2:]{
				//fmt.Println("Node ID: ", nodeID)
				if strconv.Itoa(self_id) == nodeID{
					appendFile(lc, connTable, keyTable, self_id, second_split_tokens[i], hydfsFileName, m)
					i++
					continue
				}
				node_id_int ,err := strconv.Atoi(nodeID)
				if err != nil{
					fmt.Println("Error converting nodeID to int in multiappend")
					return
				}
				conn, ok := connTable.Load(node_id_int)
				fileID := GetFileID(hydfsFileName,m)
				if ok{
					//fmt.Println("Sending multiappend request to node " + nodeID)
					sendHyDFSMessage(lc, conn.(net.Conn), "LAUNCHAPPEND: " + strconv.Itoa(fileID) + " " + second_split_tokens[i]+ " " + hydfsFileName)
				}
				fmt.Println("Sent multiappend request to node " + nodeID)
				i++
			}


		}
	}
}

func StartHyDFS(hyDFSSelfPort string, hyDFSGlobalPort string, selfAddress  string, wg *sync.WaitGroup){
	keyTable := sync.Map{}
	connTable := sync.Map{}
	lc := LamportClock{}
	m:= 10 
	self_id = GetPeerID(selfAddress,m)
	fileNameMap :=sync.Map{}
	metaConnMap := sync.Map{} // used to distinguish between failure detection connection and streamDS connection
	//start the routines for HyDFS
	stopRoutineChan := make(chan struct{})
	wg.Add(6)
	go FileBayHandlerRoutine(wg, stopRoutineChan, &keyTable, &connTable, &lc)
	go CacheBayHandlerRoutine(wg, stopRoutineChan)
	go ReplicaBayHandlerRoutine(&lc ,&connTable, &keyTable, wg, stopRoutineChan)
	go ArrivalBayHandlerRoutine(&lc, &connTable, &keyTable,&fileNameMap, wg, stopRoutineChan)
	go StartPipe(&lc, hyDFSSelfPort,hyDFSGlobalPort, &keyTable, &connTable,&fileNameMap, &metaConnMap,wg, m)
	go StartHyDFSListener(&lc, hyDFSGlobalPort, &keyTable, &connTable,&fileNameMap,  wg,m)
	go SetupHyDFSCommTerminal(&lc, hyDFSGlobalPort, &keyTable, &connTable,&fileNameMap, wg, m,stopRoutineChan)
	wg.Wait()
	//send a signal to stop the ping routine

	close(stopRoutineChan)

}