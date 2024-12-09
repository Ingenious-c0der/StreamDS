package distributed_log_querier

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
    "net"
)

//This routine checks if the files currently in the Filebay have sufficient replicas, if not it will send the replication message
//it will also check if the file currently in the Filebay should actually be here, otherwise it will move it to the correct location
func FileBayHandlerRoutine(wg *sync.WaitGroup, stopChan <-chan struct{}, keytable *sync.Map, connTable *sync.Map, lc *LamportClock) {
    defer wg.Done()
    dir := GetDistributedLogQuerierDir()
    fileBayDir := filepath.Join(dir, "FileBay") // Target directory
    for {
        select {
        case <-stopChan:
            //fmt.Println("FileBayHandlerRoutine stopped")
            return
        default:
            // Perform routine tasks
            //fmt.Println("Running FileBayHandlerRoutine...")
            files, err := os.ReadDir(fileBayDir)
            if err != nil {
                fmt.Println("Error reading fileBay directory:", err)
                time.Sleep(10 * time.Second) // Adjust sleep duration as needed
                continue
            }
            for _, file := range files {
                //call the successors to check if enough replicas exist
                //the actual firefighting is done in with the message echos, the
                // routine is just a way to trigger it 
                //get the fileID
                file_base:= strings.Split(file.Name(), ".")[0]
                fileID, err:= strconv.Atoi(strings.Split(file_base, "_")[1])
                //fmt.Println("Checking for fileID: ", fileID)
                if err != nil {
                    fmt.Println("Error converting fileID to int in filebay routine")
                    continue
                }
                x,y := GetHYDFSSuccessorIDs(self_id, keytable)
                if x == -1 || y == -1{
                    fmt.Println("Error getting successors for fileID: ", fileID)
                    continue
                }
               
                //send REPEXIST message to the successors
                x_conn, x_ok := connTable.Load(x)
                y_conn, y_ok := connTable.Load(y)
                //fmt.Println("Checking for fileID: ", fileID)
                if x_ok && y_ok{
                    sendHyDFSMessage(lc, x_conn.(net.Conn), "REPEXIST " + strconv.Itoa(fileID))
                    sendHyDFSMessage(lc, y_conn.(net.Conn), "REPEXIST " + strconv.Itoa(fileID))
                }
            }
            time.Sleep(100 * time.Millisecond) // Adjust sleep duration as needed
        }
    }
}

//to first download the contents of a file over TCP, once a file is accessible with acquire lock, move it to fileBay
//and then delete it locally
func ArrivalBayHandlerRoutine(lc* LamportClock, connTable *sync.Map, keyTable *sync.Map, fileNameMap *sync.Map, wg *sync.WaitGroup, stopChan <-chan struct{}) {
    defer wg.Done()
	dir := GetDistributedLogQuerierDir()
	arrivalBayDir := filepath.Join(dir, "ArrivalBay")
    fileBayDir := filepath.Join(dir, "FileBay") // Target directory
	appendBayDir := filepath.Join(dir, "appendBay") // Target directory
    replicaBayDir := filepath.Join(dir, "ReplicaBay") // Target directory
    cacheBayDir := filepath.Join(dir, "CacheBay") // Target directory
    fmt.Println("Arrival Bay Dir: " + arrivalBayDir)
    fmt.Println("File Bay Dir: " + fileBayDir)
    fmt.Println("Append Bay Dir: " + appendBayDir)
    fmt.Println("Replica Bay Dir: " + replicaBayDir)
    fmt.Println("Cache Bay Dir: " + cacheBayDir)
	for {
        select {
        case <-stopChan:
            fmt.Println("ArrivalBayHandlerRoutine stopped")
            return
        default:
           // fmt.Println("Running ArrivalBayHandlerRoutine...")

            // Check for files in the arrival bay directory
            files, err := os.ReadDir(arrivalBayDir)
            if err != nil {
                fmt.Println("Error reading arrival bay directory:", err)
                time.Sleep(3 * time.Second)
                continue
            }

            // Iterate over files and move them to file bay
            for _, file := range files {
                if file.IsDir() {
                    continue
                }
                sourcePath := filepath.Join(arrivalBayDir, file.Name())
                destinationPath := filepath.Join(fileBayDir, file.Name())

                // Attempt to move the file with lock
                fileLock, err := os.OpenFile(sourcePath, os.O_RDWR, 0666)
                if err != nil {
                    fmt.Printf("Error opening file %s: %v\n", file.Name(), err)
                    continue
                }
                //based on the sendFileName move the file to the correct bay
                if strings.Contains(file.Name(), "append"){
                    //send the file name to the correct directory inside the filebay depending on the NodeID
                    NodeID := strings.Split(file.Name(), "_")[1]
                    //check if the directory exists
                    appendBayDirtmp := filepath.Join(appendBayDir, NodeID)
                    //fmt.Println("AppendBayDir: ", appendBayDirtmp)
                    if _, err := os.Stat(appendBayDirtmp); os.IsNotExist(err) {
                        os.Mkdir(appendBayDirtmp, 0755)
                    }
                    destinationPath = filepath.Join(appendBayDirtmp, file.Name())

                }else if strings.Contains(file.Name(), "replica") && !strings.Contains(file.Name(), "cache"){
                    destinationPath = filepath.Join(replicaBayDir, file.Name())
                }else if strings.Contains(file.Name(), "cache"){
                    destinationPath = filepath.Join(cacheBayDir, file.Name())
                }else if strings.Contains(file.Name(), "original"){
                    // remove the + and split the file name to get the fileID
                    //name arrives as original_fileID.txt+hydfsfilename.txt
                    fileBase := strings.Split(file.Name(), "+")[0]
                    fileSplitOne:= strings.Split(fileBase, "_")[1]
                    fileID := strings.Split(fileSplitOne, ".")[0]
                    hydfsFileName:= strings.Split(file.Name(), "+")[1]
                    destinationPath = filepath.Join(fileBayDir, fileBase)
                    //broadcast the fileID and hydfsFileName mapping 
                    broadcastHyDFSMessage(lc, connTable, "MAPSFILE: " + fileID + " " + hydfsFileName)
                    fileNameMap.Store(fileID, hydfsFileName)
                }
                // Ensure file is copied to destination
                err = copyFileContents(sourcePath, destinationPath)
                if err != nil {
                    fmt.Printf("Error moving file %s to fileBay: %v\n", file.Name(), err)
                    fileLock.Close()
                    continue
                }
                if strings.Contains(file.Name(), "append"){
                    //if this is the cood for that file ID, then need to forward the append to replicas
                    //filename like append_NodeID_fileID.txt
                    //make sure to store appends only for the right files. 
                    //if current node is the cood for this file and if it does not contain the file in fileBay, drop this append else accept it
                    fileID, err:= strconv.Atoi(strings.Split(file.Name(), "_")[2])
                    //conv to int
                    if err != nil {
                        fmt.Println("Error converting fileID to int in append routine")
                        fileLock.Close()
                        continue
                    }
                    cood_ID := GetHyDFSCoordinatorID(keyTable, fileID)
                    if cood_ID == self_id{
                        if !checkFileExists("FileBay", "original_" + strconv.Itoa(fileID) + ".txt"){
                            fmt.Println("File "+file.Name()+ " is a stray append (file does not exist on local (cood) node)")
                            fmt.Println("Storing anyway, optimistic replication")
                            //wait for 3 seconds before deleting the file
                            //if checkFileExists("FileBay", "original_" + strconv.Itoa(fileID) + ".txt"){
                                //fmt.Println("File "+file.Name()+ " is no longer a stray append")
                                fileLock.Close()
                                //make sure the file is in the append bay and is visible 
                                forwardAppendToReplica(lc, connTable, keyTable,self_id, destinationPath, file.Name())
                            // }else{
                            //     fileLock.Close()
                            //     fmt.Println("Removed stray append file "+file.Name() +" after waiting for 5 seconds")
                            //     //remove the file
                            //     err = os.Remove(destinationPath)
                            //     //also remove the file from arrival bay
                            //     err2 := os.Remove(sourcePath)
                            //     if err != nil {
                            //         fmt.Printf("Error deleting file %s from arrival bay: %v\n", file.Name(), err)
                            //     }
                            //     if err2 != nil {
                            //         fmt.Printf("Error deleting file %s from arrival bay: %v\n", file.Name(), err2)
                            //     }
                            //     continue
                            // }
                        }else{
                            //release lock before forwarding the append
                            fileLock.Close()
                            //make sure the file is in the append bay and is visible 
                            forwardAppendToReplica(lc, connTable, keyTable,self_id, destinationPath, file.Name())
                            }
                        }
                }
                if strings.Contains(file.Name(), "cache"){
                    fmt.Println("File "+file.Name()+ " successfully fetched and now can be read")
                }

                // Remove file from arrival bay after successful copy
                err = os.Remove(sourcePath)
                if err != nil {
                    fmt.Printf("Error deleting file %s from arrival bay: %v\n", file.Name(), err)
                } else {
                   // fmt.Printf("File %s moved to internals and deleted from arrival bay\n", file.Name())
                }

                fileLock.Close() // Release lock
            }

            time.Sleep(10 * time.Millisecond) // Adjust sleep duration as needed
        }
    }
}

//cache is invalidated in the following 3 scenarios
//1.there is an append from the current node to that file
//2.file was merged
//3.File freshness timeout of 1 minute reached with an error margin of 20 seconds
//only 3rd is handled by this routine
func CacheBayHandlerRoutine(wg *sync.WaitGroup, stopChan <-chan struct{}) {
    defer wg.Done()
    dir:= GetDistributedLogQuerierDir()
    cacheBayDir := filepath.Join(dir, "CacheBay") //routine home   
    for {
        select {
        case <-stopChan:
            fmt.Println("CacheBayHandlerRoutine stopped")
            return
        default:
            // Perform routine tasks
            //fmt.Println("Running CacheBayHandlerRoutine...")
            
            files, err := os.ReadDir(cacheBayDir)
            if err != nil {
               fmt.Println("Error reading cacheBay directory:", err)
               time.Sleep(5 * time.Second) // Adjust sleep duration as needed
                continue
            }

            // Get the current time
            currentTime := time.Now()

            // Iterate over each file in the cacheBay directory
            for _, file := range files {
                fileInfo, err := file.Info()
                if err != nil {
                    fmt.Println("Error getting file info:", err)
                    continue
                }
                // Check if file is older than 10 seconds
                //fmt.Println("Checking file:", file.Name())
                if currentTime.Sub(fileInfo.ModTime()) > 10*time.Second {
                    // File is stale, remove it
                    err := os.Remove(filepath.Join(cacheBayDir, file.Name()))
                    if err != nil {
                        fmt.Println("Error removing file:", err)
                    } else {
                        fmt.Println("Removed stale cache file:", file.Name())
                    }
                }
            }
            time.Sleep(2 * time.Second) // Adjust sleep duration as needed
        }
    }
}

//check if the files in replica bay 1)has an alive coordinator, 2) has the correct number of replicas
func ReplicaBayHandlerRoutine(lc *LamportClock, connTable *sync.Map, keyTable *sync.Map, wg *sync.WaitGroup, stopChan <-chan struct{}) {
    defer wg.Done()
    dir:= GetDistributedLogQuerierDir()
    replicaBayDir := filepath.Join(dir, "ReplicaBay") //routine home
    for {
        select {
        case <-stopChan:
            fmt.Println("ReplicaBayHandlerRoutine stopped")
            return
        default:
            // Perform routine tasks
            //fmt.Println("Running ReplicaBayHandlerRoutine...")
            files, err := os.ReadDir(replicaBayDir)
            if err != nil {
                fmt.Println("Error reading replicaBay directory:", err)
                time.Sleep(5 * time.Second) // Adjust sleep duration as needed
                continue
            }
            for _, file := range files {
                //get the fileID
                fileBase := strings.Split(file.Name(), ".")[0]
                fileID, err:= strconv.Atoi(strings.Split(fileBase, "_")[1])
                if err != nil {
                    fmt.Println("Error converting fileID to int in replica routine")
                    continue
                }
                //get the cood for this file ID
                cood := GetHyDFSCoordinatorID(keyTable, fileID)
                if cood == -1{
                    fmt.Println("Not enough keys while checking for COOD")
                    return
                }
                //send the ISCOOD message to the cood to poke it
                cood_conn, cood_ok := connTable.Load(cood)
                if cood_ok{
                    sendHyDFSMessage(lc, cood_conn.(net.Conn), "ISCOOD " + strconv.Itoa(fileID))
                }else{
                    if self_id == cood{
                         //case where you need to become the cood for this file since we do not store the cood for self
                         dir := GetDistributedLogQuerierDir()
                         replica_file_name := "replica_" + strconv.Itoa(fileID) + ".txt"
                         original_file_name := "original_" + strconv.Itoa(fileID) + ".txt"
                         err := copyFileContents(filepath.Join(dir, "ReplicaBay", replica_file_name), filepath.Join(dir, "FileBay", original_file_name))
                         if err != nil{
                             fmt.Println("Error copying file from ReplicaBay to FileBay")
                         }else{
                             removeFile(filepath.Join(dir, "ReplicaBay", replica_file_name))
                         }
                         fmt.Println("Taking over as COOD for file " + strconv.Itoa(fileID))
                    }else{
                        fmt.Println("This should never happen, cood not found for fileID: ", fileID)
                    }
                }
            }

            time.Sleep(100 * time.Millisecond) // Adjust sleep duration as needed
        }
    }
}


//routine which does not run infinitely but waits on a cache file to be present and then moves it to fetched
//times out after 20 seconds
func FetchCache(fileID string, downloadName string, fileNameMap *sync.Map) {
    if !strings.Contains(downloadName, ".txt") {
        downloadName = downloadName + ".txt"
    }
    
    dir := GetDistributedLogQuerierDir()
    cacheBayDir := filepath.Join(dir, "CacheBay")
    fetchedDir := filepath.Join(dir, "Fetched") 
    cache_file_ID := "cache_" + fileID + ".txt"
    cacheFile := filepath.Join(cacheBayDir, cache_file_ID)
    fetchedFile := filepath.Join(fetchedDir, downloadName)

    // Set a timeout duration (20 seconds)
    timeout := time.After(20 * time.Second)
    ticker := time.NewTicker(20 * time.Millisecond) // Check every second

    defer ticker.Stop()

    for {
        select {
        case <-timeout:
            // Timeout expired, stop the function
            fmt.Println("Timeout expired! Cache file not found in time.")
            return
        case <-ticker.C:
            // Check if the cache file exists
            _, err := os.Stat(cacheFile)
            if err == nil {
                // Cache file exists, move it to fetched directory
                // Copy contents
                err = copyFileContents(cacheFile, fetchedFile)
                if err != nil {
                    fmt.Printf("Error moving file %s to fetched: %v\n", downloadName, err)
                } else {
                    //add a \n to the end of fetched file if it is not already there
                    content, err := os.ReadFile(fetchedFile)
                    if err == nil && len(content) > 0 {
                        if content[len(content)-1] != '\n' {
                            file, err := os.OpenFile(fetchedFile, os.O_APPEND|os.O_WRONLY, 0644)
                            if err == nil {
                                defer file.Close()
                                _, err = file.WriteString("\n")
                                if err != nil {
                                    fmt.Println("nonfatal : Error writing newline to fetched file" + err.Error())
                                }
                            }
                        }
                    }
                    hydfs_name,ok := fileNameMap.Load(fileID)
                    if ok{
                        fmt.Println("File "+ hydfs_name.(string) +" fetched as "+downloadName)
                    }else{
                        fmt.Println("File "+downloadName +" fetched and is now ready to be read")
                    }
                }
                return // Exit once the file has been moved
            }
        }
    }
}