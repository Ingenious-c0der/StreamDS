package distributed_log_querier

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var self_stream_id int = 0
var stateful_operator_state_file = "word_count_state.txt"
var stage_1_batch_size = 100
var stage_2_batch_size = 50
//only to be run on the leader node
func planStreamDSTasks(hydfsConn *SafeConn, hydfsSrcFileName string, op1_name string, op1_state string, op2_name string, op2_state string, hydfsDestFileName string,num_tasks int, streamTaskTable *sync.Map) map[string]*Task{
	// a way to find VMs with least number of tasks running on them 
	//plan the source stage
	//get the hydfs src file size, divide it by num_tasks to get the partition size
	//send get to hydfs layer
	_, ok  := streamTaskTable.Load(self_stream_id)
	if !ok {
		fmt.Println("Leader node not found in the streamTaskTable, sane check passed")
	}else{
		fmt.Println("Leader node found in the streamTaskTable, this should never happen")
		return nil
	}
	fmt.Println("")
	taskMap := make(map[string]*Task) // nodeID -> Task mapper
	success := GetFileFromHydfs(hydfsConn, hydfsSrcFileName, 10)
	if !success {
		fmt.Println("Error in getting the file from hydfs")
		return nil
	}
	//get the total lines in the file 
	total_lines, err := CountLines(hydfsSrcFileName)
	if err != nil {
		fmt.Println("Error in counting lines in the file")
		return nil
	}
	//get the partition size
	partitions := GetFairPartitions(total_lines, num_tasks)
	fmt.Println("Partitions: ", partitions)
	//first schedule stage 2 tasks
	for i := 0; i < num_tasks; i++ {
		nodeID_selected := GetNodeWithLeastTasks(streamTaskTable)
		if nodeID_selected == -1 {
			fmt.Println("No node available to schedule the task")
			return nil
		}
		stage_2_task := NewTask(
			i, 
			nodeID_selected,
			"operator",
			"active",
			"second",
			"multi",
			"file",
			[]string{},
			hydfsDestFileName,
			"None",
			"None",
			"None",
			op2_state,
			op2_name,
			false,
			false,
			[]int{},
			num_tasks,
		)
		taskMap[strconv.Itoa(nodeID_selected)] = stage_2_task
		pre_tasks, ok  := streamTaskTable.Load(nodeID_selected)
		if ok {
			pre_tasks = append(pre_tasks.([]*Task), stage_2_task)
			streamTaskTable.Store(nodeID_selected, pre_tasks)
		} else {
			streamTaskTable.Store(nodeID_selected, []*Task{stage_2_task})
		}
	}
	stage_two_nodes := []string {}
	for k, _ := range taskMap {
		stage_two_nodes = append(stage_two_nodes, k)
	}
	stage_two_task_ids := make([]int, 0)
	for _, node_itr := range stage_two_nodes{
		stage_two_task_ids = append(stage_two_task_ids, taskMap[node_itr].TaskID)
	}

	//next schedule stage 1 tasks (utilize the taskMap keys as output nodes)
	stage_one_nodes := []string{}
	for i := num_tasks; i < 2 * num_tasks; i++ {
		nodeID_selected := GetNodeWithLeastTasks(streamTaskTable)
		if nodeID_selected == -1 {
			fmt.Println("No node available to schedule the task")
			return nil
		}
		//this could be parameterized further 
		stage_1_task := NewTask(
			i,
			nodeID_selected,
			"operator",
			"active",
			"first",
			"single",
			"multi",
			stage_two_nodes,
			"None",
			"None",
			"None",
			"None",
			op1_state,
			op1_name,
			true,
			false,
			stage_two_task_ids, 
			num_tasks,
		)
		taskMap[strconv.Itoa(nodeID_selected)] = stage_1_task
		pre_tasks, ok  := streamTaskTable.Load(nodeID_selected)
		if ok {
			pre_tasks = append(pre_tasks.([]*Task), stage_1_task)
			streamTaskTable.Store(nodeID_selected, pre_tasks)
		} else {
			streamTaskTable.Store(nodeID_selected, []*Task{stage_1_task})
		}
		stage_one_nodes = append(stage_one_nodes, strconv.Itoa(nodeID_selected))
	}
	//use the stage_1_task nodes as output nodes for the source tasks
	//last schedule the source tasks
	//num_stages are hardcoded to 3
	for i := 2*num_tasks; i < 3* num_tasks; i++ {
		nodeID_selected := GetNodeWithLeastTasks(streamTaskTable)
		//get one of the stage 1 nodes as the output node and remove it from the list
		output_node := stage_one_nodes[0]
		//get the task id of the output node
		task_output, ok := taskMap[output_node]
		if !ok {
			fmt.Println("Output node not found in the task map")
			return nil
		}
		stage_one_nodes = stage_one_nodes[1:]
		if nodeID_selected == -1 {
			fmt.Println("No node available to schedule the task")
			return nil
		}
		current_partition := partitions[i - 2*num_tasks]
		//schedule the source task
		source_task := NewTask(
			i,
			nodeID_selected,
			"source",
			"active",
			"zero",
			"file",
			"single",
			[]string{output_node},
			"None",
			hydfsSrcFileName,
			strconv.Itoa(current_partition[0]) + ":"+ strconv.Itoa(current_partition[1]),
			"None",
			"Stateless",
			"None",
			true,
			false,
			[]int{task_output.TaskID},
			num_tasks,
			)
		taskMap[strconv.Itoa(nodeID_selected)] = source_task
		pre_tasks, ok  := streamTaskTable.Load(nodeID_selected)
		if ok {
			pre_tasks = append(pre_tasks.([]*Task), source_task)
			streamTaskTable.Store(nodeID_selected, pre_tasks)
		} else {
			streamTaskTable.Store(nodeID_selected, []*Task{source_task})
		}
	}
	//create the tasklogFiles for each of the tasks
	fmt.Println("Creating task log files and output files")
	for _, task := range taskMap {
		taskLogFile := task.TaskLogFile
		hydfsConn.SafeWrite([]byte("CREATEFILE: "+ taskLogFile + "END_OF_MSG\n"))
	}
	//create the hydfsDestFile
	hydfsConn.SafeWrite([]byte("CREATEFILE: "+ hydfsDestFileName + "END_OF_MSG\n"))
	fmt.Println("Task planning completed")
	//return the taskMap
	return taskMap
}
//only to be run on the leader node
func rescheduleStreamDSTaskOnFailure(hydfsConn *SafeConn,FailedNodeID int, streamConnTable *sync.Map, streamTaskTable *sync.Map ){
	//first find the affected tasks
	affected_tasks, ok := streamTaskTable.Load(FailedNodeID)
	if !ok {
		fmt.Println("Failed node not found in the streamTaskTable")
		return
	}
	//remove the failed node from the streamTaskTable
	streamTaskTable.Delete(FailedNodeID)
	for _, task := range affected_tasks.([]*Task){
		if task.TaskType == "source" {
			//steps
			//1. execute merge on the output file
			//2. get a node with least tasks after removing the failed node
			//3. update the streamTaskTable
			//4. send the task to the new node
			ExecuteMergeOnHydfs(hydfsConn, task.TaskLogFile)
			task.DoReplay = true
			task.TaskAssignedNode = GetNodeWithLeastTasks(streamTaskTable)
			pre_tasks, ok  := streamTaskTable.Load(task.TaskAssignedNode)
			if ok {
				pre_tasks = append(pre_tasks.([]*Task), task)
				streamTaskTable.Store(task.TaskAssignedNode, pre_tasks)
			} else {
				streamTaskTable.Store(task.TaskAssignedNode, []*Task{task})
			}
			//get the new node's conn and send the task
			nodeConn, ok := streamConnTable.Load(task.TaskAssignedNode)
			if !ok {
				fmt.Println("Node connection not found")
				return
			}
			sendTask(nodeConn.(net.Conn), *task)
			fmt.Println("Task "+ strconv.Itoa(task.TaskID) +" rescheduled on node "+ strconv.Itoa(task.TaskAssignedNode))
		}else if task.TaskType == "operator" {

			if task.TaskStage == "first" {
				//steps
				//1. execute merge on the tasklogfile
				//2. get a node with least tasks after removing the failed node
				//3. update the streamTaskTable
				//4. send the task to the new node
				//5. send the resume message to the inputNode of the task
				ExecuteMergeOnHydfs(hydfsConn, task.TaskLogFile)
				task.DoReplay = true
				task.TaskAssignedNode = GetNodeWithLeastTasks(streamTaskTable)
				pre_tasks, ok  := streamTaskTable.Load(task.TaskAssignedNode)
				if ok {
					pre_tasks = append(pre_tasks.([]*Task), task)
					streamTaskTable.Store(task.TaskAssignedNode, pre_tasks)
				} else {
					streamTaskTable.Store(task.TaskAssignedNode, []*Task{task})
				}
				//get the new node's conn and send the task
				nodeConn, ok := streamConnTable.Load(task.TaskAssignedNode)
				if !ok {
					fmt.Println("Node connection not found")
					return
				}
				sendTask(nodeConn.(net.Conn), *task)
				fmt.Println("Task "+ strconv.Itoa(task.TaskID) +" rescheduled on node "+ strconv.Itoa(task.TaskAssignedNode))
				//send the resume message to the input node
				inputNodeID := task.TaskInputNode
				inputNodeID_int, err := strconv.Atoi(inputNodeID)
				if err != nil {
					fmt.Println("Error in parsing the input node ID")
					return
				}
				inputNodeConn, ok := streamConnTable.Load(inputNodeID_int)
				if !ok {
					fmt.Println("Input node connection not found")
					return
				}
				//RESUME send format RESUME: targetNodeID targettaskID+new_output_node_id/s (PS. the targetNodeID is filtered out once receipt)
				resume_msg := "RESUME: "+ strconv.Itoa(task.TaskAssignedNode) + " "+ strconv.Itoa(task.TaskID) + "+"+ strings.Join(task.TaskOutputNodes, " ")
				time.Sleep(1 * time.Second)
				inputNodeConn.(net.Conn).Write([]byte(resume_msg + "END_OF_MSG\n"))
			}else if task.TaskStage == "second" {
				//steps
				//1. execute merge on the tasklogfile
				//2. execute merge on the hydfs output file
				//3. get a node with least tasks after removing the failed node
				//4. update the streamTaskTable
				//5. send the task to the new node
				//6. send the resume message to all the nodes that sent input to the task (i.e. all the stage1 tasks)
				//todo new: check if the 6th point here actually is okay . i.e. sending resume message when the task never paused, to replay
				//the buffer. or some other mechanism is needed
				ExecuteMergeOnHydfs(hydfsConn, task.TaskLogFile)
				ExecuteMergeOnHydfs(hydfsConn, task.TaskOutputFile)
				task.DoReplay = true
				task.TaskAssignedNode = GetNodeWithLeastTasks(streamTaskTable)
				pre_tasks, ok  := streamTaskTable.Load(task.TaskAssignedNode)
				if ok {
					pre_tasks = append(pre_tasks.([]*Task), task)
					streamTaskTable.Store(task.TaskAssignedNode, pre_tasks)
				} else {
					streamTaskTable.Store(task.TaskAssignedNode, []*Task{task})
				}
				//get the new node's conn and send the task
				nodeConn, ok := streamConnTable.Load(task.TaskAssignedNode)
				if !ok {
					fmt.Println("Node connection not found")
					return
				}
				sendTask(nodeConn.(net.Conn), *task)
				fmt.Println("Task "+ strconv.Itoa(task.TaskID) +" rescheduled on node "+ strconv.Itoa(task.TaskAssignedNode))
				//send the resume message to all the nodes that sent input to the task
				//iterate over all the tasks in the streamTaskTable and send the resume message to the ones that are stage 1 tasks
				new_output_nodes := []string{}
				new_output_nodes = append(new_output_nodes, strconv.Itoa(task.TaskAssignedNode))
				failed_node_str := strconv.Itoa(FailedNodeID)
				streamTaskTable.Range(func(key, value interface{}) bool {
					tasks := value.([]*Task)
					for _, taskitr := range tasks {
						if taskitr.TaskType == "operator" && taskitr.TaskStage == "first" {
							//send the resume message to the input node
							taskNodeID := taskitr.TaskAssignedNode
							taskNodeConn, ok := streamConnTable.Load(taskNodeID)
							if !ok {
								fmt.Println("Input node connection not found")
								return true
							}
							//update the output nodes of the task
							for _, output_node := range taskitr.TaskOutputNodes {
								if output_node != failed_node_str {
									new_output_nodes = append(new_output_nodes, output_node)
								}
							}
							taskitr.TaskOutputNodes = new_output_nodes
							//RESUME send format RESUME: targetNodeID targettaskID+new_output_node_id/s (PS. the targetNodeID is filtered out once receipt)
							resume_msg := "RESUME: "+ strconv.Itoa(taskitr.TaskAssignedNode) + " "+ strconv.Itoa(taskitr.TaskID) + "+"+ strings.Join(taskitr.TaskOutputNodes, " ")
							time.Sleep(200 * time.Millisecond)
							taskNodeConn.(net.Conn).Write([]byte(resume_msg + "END_OF_MSG\n"))
						}
					}
					return true
				})
			}
		}

	}
}

func runStreamDSTask(hydfsConn * SafeConn, task *Task, streamConnTable *sync.Map,chord chan string, m int){
	//first carve out the structure of the task 
	//get the nodeID of the task for validation
	nodeID:= task.TaskAssignedNode 
	//check if its itself 
	if nodeID!= self_stream_id {
		fmt.Println("Task wasn't supposed to run on this VM ", nodeID, self_stream_id)
		return
	}
	//get the task type
	if task.TaskType == "source" {
		//get the source file name
		source_file_name := task.TaskInputFile 
		if source_file_name == "None" {
			fmt.Println("Source file name is None")
			return
		}
		success := GetFileFromHydfs(hydfsConn, source_file_name, 10)
		if !success {
			fmt.Println("Error in getting the file from hydfs, retrying.. ")
			//retry
			for i:=0; i<5; i++ {
				success = GetFileFromHydfs(hydfsConn, source_file_name, 10)
				if !success {
					fmt.Println("Error in getting the file from hydfs max retry")
				}
			}
			if !success {
				fmt.Println("Error in getting the file from hydfs after max retries")
				return
			}
		
		}
		partition := strings.Split(task.TaskPartitionRange, ":")
		start_line, err1 := strconv.Atoi(partition[0])
		end_line, err2 := strconv.Atoi(partition[1])
		if err1 != nil || err2 != nil {
			fmt.Println("Error in parsing the partition range")
			return
		}
		tuples := []LineInfo{}
		var err error
		//check if the task is a replay task
		if task.DoReplay {
			//resume the task from the last sent tuple before the crash
			//steps 
			// 1. fetch the buffer
			// 2. read the last buffer state
			// 3. start the partition from the last smallest unacked tuple 
			success_ret := GetFileFromHydfs(hydfsConn, task.TaskLogFile, 10)
			if !success_ret {
				fmt.Println("Error in getting the file from hydfs")
				return
			}
			bufferMap, err := ReadLastBuffer(task.TaskLogFile)
			if err != nil {
				fmt.Println("Error in reading the last buffer")
				return
			}
			//get the smallest unacked tuple
			smallest_unacked_tuple := math.MaxInt
			for fileLineID := range bufferMap {
				line_num_int := GetSourceLineNumberFromKey(fileLineID)
				if line_num_int == -1{
					fmt.Println("Source key does not contain line num, replaying entire partition")
					continue
				}
				if line_num_int < smallest_unacked_tuple {
					smallest_unacked_tuple = line_num_int
				}
			}
			if smallest_unacked_tuple == math.MaxInt {
				fmt.Println("No unacked tuple found, replaying entire partition")
				smallest_unacked_tuple = start_line
			}
			//start the partition from the smallest unacked tuple
			tuples, err = ReadFilePartition(source_file_name, smallest_unacked_tuple, end_line)
			if err != nil {
				fmt.Println("Error in reading the file partition during replay")
				return
			}
		}else{
			//get tuples from the file partition as (filename:lineNumber,line)
			tuples, err = ReadFilePartition(source_file_name, start_line, end_line)
			if err != nil {
				fmt.Println("Error in reading the file partition")
				return
			}

		}
		//start the send of tuples and receive of ack on the chord channel
		//also maintain persistent buffer for the tuples
		batch_size := stage_1_batch_size
		batch := make([]LineInfo, 0) 
		bufferMap := make(map[string]string)
		output_node_ID_string := task.TaskOutputNodes[0]
		output_node_ID_int, err := strconv.Atoi(output_node_ID_string)
		if err != nil {
			fmt.Println("Error in parsing the output node ID")
			return
		}
		output_node_conn,ok := streamConnTable.Load(output_node_ID_int)
		if !ok {
			fmt.Println("Output node connection not found")
			return
		}
		lastAckTime := time.Now()
		lastSentTime := time.Now()
		paused := false
		tupleIndex := 0 
		for tupleIndex < len(tuples) || len(bufferMap) > 0{
			select {
			case msg := <- chord:
				if strings.HasPrefix(msg, "ACK: ") {
					//Format ACK: targetNodeId TargetTaskID \n <body>
					//this will be a batch ack
					//remove all the tuples from the buffer that have been acked
					//fmt.Println("Rack")
					parts := strings.SplitN(msg, "\n", 2)
					if len(parts) != 2 {
						fmt.Println("Error in parsing the ack message")
						fmt.Println(msg)
						continue
					}
					header := parts[0]
					body := parts[1]
					header_parts := strings.Split(header, " ")
					if len(header_parts) != 3 {
						fmt.Println("Error in parsing the ack header")
						continue
					}
					acked_tuples, err := deserializeLineInfoArray([]byte(body))

					if err != nil {
						fmt.Println("Error in deserializing the acked tuples")
						return
					}
					//fmt.Println("Ack tuple sample :", acked_tuples[0])
					for _, acked_tuple := range acked_tuples {
						delete(bufferMap, acked_tuple.FileLineID)
					}
					StoreBufferOnHydfs(bufferMap, task.TaskLogFile, hydfsConn)
					lastAckTime = time.Now()
				}else if strings.HasPrefix(msg, "RESUME: ") {
					//format RESUME: targettaskID+new_output_node_id
					//example RESUME: 6+145
					//parse the new output node id
					content:= strings.Split(msg, " ")[1]
					output_node_ID_string = strings.Split(content, "+")[1]
					output_node_ID_int, err := strconv.Atoi(output_node_ID_string)
					if err != nil {
						fmt.Println("Error in parsing the output node ID")
						return
					}
					output_node_conn,ok = streamConnTable.Load(output_node_ID_int)
					if !ok {
						fmt.Println("Output node connection not found")
						return
					}
					fmt.Println("Resuming the task with new output node ", output_node_ID_string)
					paused = false
					batchSize := 10
					lastSentTime = time.Now()
					lastAckTime = time.Now()
					//resend the bufferMap in batches
					batch := make([]LineInfo, 0, batchSize)
					for fileLineID, content := range bufferMap {
						batch = append(batch, LineInfo{FileLineID: fileLineID, Content: content})
						if len(batch) == batchSize {
							sendLineInfoArray(output_node_conn.(net.Conn), batch, output_node_ID_string, strconv.Itoa(task.OutputTaskIDs[0]),strconv.Itoa(task.TaskID), strconv.Itoa(self_stream_id))
							batch = batch[:0] // Clear the batch
						}
					}
					// Send any remaining tuples
					if len(batch) > 0 {
						sendLineInfoArray(output_node_conn.(net.Conn), batch, output_node_ID_string, strconv.Itoa(task.OutputTaskIDs[0]),strconv.Itoa(task.TaskID),strconv.Itoa(self_stream_id))
					}

				}
			default:
				if paused {
					time.Sleep(300 * time.Millisecond)
					//fmt.Println("Task ", task.TaskID, " paused")
					continue
				}
				//fmt.Println(time.Since(lastAckTime), time.Since(lastSentTime))
				if time.Since(lastAckTime) > 5*time.Second && time.Since(lastSentTime) < 5*time.Second {
					paused = true
					fmt.Println("Task paused ", lastAckTime, time.Now())
					fmt.Println(lastSentTime)
					continue
				}
				//send the tuple to the next stage
				if tupleIndex < len(tuples){
					tuple := tuples[tupleIndex]
					batch = append(batch, tuple)
					bufferMap[tuple.FileLineID] = tuple.Content
					tupleIndex++
					// fmt.Println(tupleIndex, tuple.FileLineID)
					if len(batch) == batch_size {
						//send the batch to the next stage
						StoreBufferOnHydfs(bufferMap, task.TaskLogFile, hydfsConn)
						sendLineInfoArray(output_node_conn.(net.Conn), batch, output_node_ID_string, strconv.Itoa(task.OutputTaskIDs[0]), strconv.Itoa(task.TaskID),strconv.Itoa(self_stream_id))
						batch = make([]LineInfo, 0)
						lastSentTime = time.Now()
					}
				}else if len(tuples) == tupleIndex{
					//send the completed message to the next stage
					completed_line := LineInfo{FileLineID: "*COMPLETED*", Content: "1"}
					sendLineInfoArray(output_node_conn.(net.Conn), []LineInfo{completed_line}, output_node_ID_string, strconv.Itoa(task.OutputTaskIDs[0]),strconv.Itoa(task.TaskID),strconv.Itoa(self_stream_id))
					tupleIndex++ // just so that we never enter this block again
				}else if len(batch) > 0 {
					//send the remaining tuples in the batch
					StoreBufferOnHydfs(bufferMap, task.TaskLogFile, hydfsConn)
					sendLineInfoArray(output_node_conn.(net.Conn), batch, output_node_ID_string, strconv.Itoa(task.OutputTaskIDs[0]),strconv.Itoa(task.TaskID),strconv.Itoa(self_stream_id))
					batch = make([]LineInfo, 0)
					lastSentTime = time.Now()
				}else {
					//all tuples have been sent, wait for ACKs to come in 
					time.Sleep(1 * time.Second)
					fmt.Println("Waiting on Acks")
					fmt.Println("BufferMap size: ", len(bufferMap))
					//print the entries in the buffer map
					smallest_unacked_tuple := math.MaxInt
					largest_unacked_tuple := -1
					for fileLineID := range bufferMap {
						line_num := strings.Split(fileLineID, ":")[1]
						line_num_int, err := strconv.Atoi(line_num)
						if err != nil {
							fmt.Println("Error in parsing the line number")
							return
						}
						if line_num_int < smallest_unacked_tuple {
							smallest_unacked_tuple = line_num_int
						}
						if line_num_int > largest_unacked_tuple {
							largest_unacked_tuple = line_num_int
						}
					}
					fmt.Println("Smallest unacked tuple: ", smallest_unacked_tuple)
					fmt.Println("Largest unacked tuple: ", largest_unacked_tuple)
					fmt.Println("*********")
				}
			}
		}
		fmt.Println("Source Task "+ strconv.Itoa(task.TaskID) +" completed")
		//send a completed message to the next task maybe?

	}else if task.TaskType == "operator" {
		if task.TaskStage == "first" {
			//setup the message listener
			bufferMap := make(map[string]string)
			//ackTimer := time.NewTimer(5 * time.Second)
			lastSentTime := time.Now()
			lastAckTime := time.Now()
			paused := false
			complete := false
			birth_time := time.Now()
			output_nodes := task.TaskOutputNodes
			completion_message_sent := false
			output_nodes_int := make([]int, 0)
			for _, node := range output_nodes {
				node_int, err := strconv.Atoi(node)
				if err != nil {
					fmt.Println("Error in converting the output node to int")
					return
				}
				output_nodes_int = append(output_nodes_int, node_int)
			}
			seen_storage_map := make(map[string]string)
			if task.DoReplay{
				//fetch the task.log file and the buffer
				success:= GetFileFromHydfs(hydfsConn, task.TaskLogFile, 10)
				if !success {
					fmt.Println("Error in getting the file from hydfs")
					return
				}
				bufferMap, err := ReadLastBuffer(task.TaskLogFile)
				//fill the seen_storage_map
				for fileLineID := range bufferMap {
					seen_storage_map[fileLineID] = "1"
				}
				if err != nil {
					fmt.Println("Error in reading the last buffer")
					return
				}
				//resend all the tuples in the buffer
				send_toBatch := make([]LineInfo, 0)
				for fileLineID := range bufferMap {
					send_toBatch = append(send_toBatch, LineInfo{FileLineID: fileLineID, Content: "1"})
				}
				for _, line := range send_toBatch {
					hashable := GetHashableStage1(line)
					//manip end key -> usable entity to hash
					selected_node,selected_task := MapHashableToNodeAndTask(hashable,m, output_nodes_int, task.OutputTaskIDs)
					
					output_node_conn, ok  := streamConnTable.Load(selected_node)
					if !ok {
						fmt.Println("Output node not found")
						return
					}
					sendLineInfoArray(output_node_conn.(net.Conn), []LineInfo{line}, strconv.Itoa(selected_node), strconv.Itoa(selected_task) , strconv.Itoa(task.TaskID) ,strconv.Itoa(self_stream_id))
				}
				lastSentTime = time.Now()
				fmt.Println("Task replay successful, now resuming the task")
			}
			for {
				select{
				case msg := <- chord:
					if strings.HasPrefix(msg, "ACK: ") {
						//this will be a batch ack
						//remove all the tuples from the buffer that have been acked
						parts := strings.SplitN(msg, "\n", 2)
						if len(parts) != 2 {
							fmt.Println("Error in parsing the ack message")
							fmt.Println(msg)
							continue
						}
						header := parts[0]
						body := parts[1]
						header_parts := strings.Split(header, " ")
						if len(header_parts) != 3 {
							fmt.Println("Error in parsing the ack header")
							continue
						}
						acked_tuples, err := deserializeLineInfoArray([]byte(body))
						if err != nil {
							fmt.Println("Error in deserializing the acked tuples")
							return
						}
						//fmt.Println("Ack tuple sample for stage 1 :", acked_tuples[0])
						for _, acked_tuple := range acked_tuples {
							delete(bufferMap, acked_tuple.FileLineID)
						}
						StoreBufferOnHydfs(bufferMap, task.TaskLogFile, hydfsConn)
						lastAckTime = time.Now()
					}else if strings.HasPrefix(msg, "INPUTBATCH: "){
						//message format INPUTBATCH: targetnodeID targettaskID inputNodeID inputtaskID
						header := strings.SplitN(msg, "\n", 2)[0]
						inputtaskID := strings.Split(header, " ")[4]
						input_node_ID := strings.Split(header, " ")[3]
						//fmt.Println(header)
						input_node_ID_int, err := strconv.Atoi(input_node_ID)
						if err != nil {
							fmt.Println("Error in parsing the input node ID: ", input_node_ID)
							return
						}
						input_node_conn, ok := streamConnTable.Load(input_node_ID_int)
						if !ok {
							fmt.Println("Input node connection not found", input_node_ID_int)
							return
						}
						input := strings.SplitN(msg, "\n", 2)[1]
						//fmt.Println("Received input batch: ", input)
						//deserialize the input
						input_batch,err := deserializeLineInfoArray([]byte (input))
						if err != nil {
							fmt.Println("Error in deserializing the input batch")
							return
						}
						if len(input_batch) == 1 {
							if input_batch[0].FileLineID == "*COMPLETED*" {
								complete = true
							}
							continue
						}
						fmt.Println(input_batch[0], " - ", input_batch[len(input_batch) - 1])
						currentBatch := make([]LineInfo, 0)
						for _, line := range input_batch {
							//VM MARKER START
							processed_output := RunOperator(task.TaskOperatorName, line.Content)
							//processed_output := RunOperatorlocal(task.TaskOperatorName, line.Content, task.TaskID)
							//VM MARKER END
							output_list := GetOutputFromOperatorStage1(processed_output)
							for _, output := range output_list {
								//manip from source key -> first key
								key_stage_1 := GetStage1Key(line.FileLineID, output)
								if key_stage_1 == "" {
									//empty output case 
									continue
								}
								if _, ok := seen_storage_map[key_stage_1]; ok {
									//send direct ack to the input node but drop the tuple here since it is a duplicate
									sendAckInfoArray(input_node_conn.(net.Conn), []LineInfo{line}, input_node_ID, inputtaskID)
									//fmt.Println("Sack1")
									fmt.Println("Sent Dupe Ack for  ", key_stage_1)
									continue
								}
								bufferMap[key_stage_1] = "1" //just a placeholder
								seen_storage_map[key_stage_1] = "1"
								currentBatch = append(currentBatch, LineInfo{FileLineID: key_stage_1, Content: "1"})
							}
						}
						//write the buffer map to the hydfs
						StoreBufferOnHydfs(bufferMap, task.TaskLogFile, hydfsConn)
						//send ack to the input node
						sendAckInfoArray(input_node_conn.(net.Conn), input_batch, input_node_ID, inputtaskID)
						//fmt.Println("Sack2")
						//send the batch to the output nodes
						//hash the word to get the output node
						//first convert the output nodes to int
						if paused{
							//fmt.Println("Task paused")
							//do not send the batch forward
							continue
						}
						for _, line := range currentBatch {
							//hash the word to get the output node
							//fileLineID or the key here is filename:linenumber:word-index
							//manip_key_op_2 
							//hashable is the entity you want to extract from the key to hash on if at all.
							hashable := GetHashableStage1(line)
							//manip end key -> usable entity to hash
							selected_node,selected_task := MapHashableToNodeAndTask(hashable,m, output_nodes_int, task.OutputTaskIDs)
							output_node_conn, ok  := streamConnTable.Load(selected_node)
							if !ok {
								fmt.Println("Output node not found")
								return
							}
							sendLineInfoArray(output_node_conn.(net.Conn), []LineInfo{line}, strconv.Itoa(selected_node), strconv.Itoa(selected_task), strconv.Itoa(task.TaskID) ,strconv.Itoa(self_stream_id))
							lastSentTime = time.Now()
						}
						
					}else if strings.HasPrefix(msg, "RESUME: "){
						//format RESUME: targettaskID+new_output_node_ids 
						//example RESUME: 6+145 146 147
						content:= strings.Split(msg, " ")[1]
						nodes_string := strings.Split(content, "+")[1]
						task.TaskOutputNodes = strings.Split(nodes_string, " ")
						output_nodes = task.TaskOutputNodes
						output_nodes_int = make([]int, 0)
						for _, node := range output_nodes {
							node_int, err := strconv.Atoi(node)
							if err != nil {
								fmt.Println("Error in converting the output node to int")
								return
							}
							output_nodes_int = append(output_nodes_int, node_int)
						}
						send_toBatch := make([]LineInfo, 0)
						for fileLineID := range bufferMap {
							send_toBatch = append(send_toBatch, LineInfo{FileLineID: fileLineID, Content: "1"})
						}
						for _, line := range send_toBatch {
							hashable := GetHashableStage1(line)
							selected_node,selected_task := MapHashableToNodeAndTask(hashable,m, output_nodes_int, task.OutputTaskIDs)
							output_node_conn, ok  := streamConnTable.Load(selected_node)
							if !ok {
								fmt.Println("Output node not found")
								return
							}
							sendLineInfoArray(output_node_conn.(net.Conn), []LineInfo{line}, strconv.Itoa(selected_node), strconv.Itoa(selected_task), strconv.Itoa(task.TaskID),strconv.Itoa(self_stream_id))
						}
						fmt.Println("Resuming the task with new output nodes ", output_nodes) 
						paused = false
						lastAckTime = time.Now()
						lastSentTime = time.Now()
					}
				default:
					//wait for unpause
					//fmt.Println("Task "+strconv.Itoa(task.TaskID)+" paused")
					if paused {
						if len(bufferMap) == 0 && complete {
							fmt.Println("First Stage task "+ strconv.Itoa(task.TaskID) +" completed")
							return
						}
						time.Sleep(300 * time.Millisecond)
					}else {
						if complete{
							if !completion_message_sent{
								//send completion message to all the stage 2 output nodes
									completed_line := LineInfo{FileLineID: "*COMPLETED*", Content: "1"}
									for i, output_node := range output_nodes_int {
										output_node_conn, ok  := streamConnTable.Load(output_node)
										if !ok {
											fmt.Println("Output node not found")
											continue
										}
										sendLineInfoArray(output_node_conn.(net.Conn), []LineInfo{completed_line}, strconv.Itoa(output_node), strconv.Itoa(task.OutputTaskIDs[i]),strconv.Itoa(task.TaskID),strconv.Itoa(self_stream_id))
									}
									completion_message_sent = true
							}
						}
						if len(bufferMap) == 0 && complete {
							fmt.Println("First Stage task "+ strconv.Itoa(task.TaskID) +" completed")
							return
						}
						if completion_message_sent {
							fmt.Println("Waiting for acks on ", len(bufferMap), " tuples")
							time.Sleep(1 * time.Second)
						}
						if (time.Since(lastAckTime) > 5*time.Second && time.Since(lastSentTime) < 5*time.Second) || 
							(complete && time.Since(lastAckTime) > 10*time.Second){
							paused = true
							//format as 12:05:06
							fmt.Println("Task paused at ", time.Now().Format("15:04:05"))
							fmt.Println("Last Ack time ", lastAckTime.Format("15:04:05"))
							fmt.Println("Last Sent time ", lastSentTime.Format("15:04:05"))
							fmt.Println("Birth time ", birth_time.Format("15:04:05"))
							continue
						}
					}
					
					
				}
			}
		}else if task.TaskStage == "second" {
			seen_storage_map := make(map[string]string)
			var bufferMap map[string]string
			bufferMap_int := make(map[string]int)
			completed_count := 0 // used to check if all the first stages are completed
			birth_time := time.Now()
			err := error(nil)
			if task.DoReplay{
				//read into the seen storage map the task.txt file 
				success:= GetFileFromHydfs(hydfsConn, task.TaskLogFile, 10)
				if !success {
					fmt.Println("Error in getting the file from hydfs")
					
					return
				}
				//assuming that the merge has been executed previously
				success_ret := GetFileFromHydfs(hydfsConn, task.TaskOutputFile, 10)
				if !success_ret {
					fmt.Println("Error in getting the file from hydfs")
					return
				}
				bufferMap, err = ReadLastBufferForTask(task.TaskLogFile, task.TaskID)
				if err != nil {
					fmt.Println("Error in reading the last buffer with given taskID ", err)
					return
				}
				for key, value := range bufferMap {
					bufferMap_int[key], err = strconv.Atoi(value)
					if err != nil {
						fmt.Println("Error in converting the buffer map value to int")
						return
					}
				}
				PopulateStatefulOperatorFile(bufferMap_int, stateful_operator_state_file)
				seen_storage_map, err = ReadLastBuffer(task.TaskLogFile)
				if err != nil {
					fmt.Println("Error in reading the last buffer")
					return
				}
				fmt.Println("Task replay successful, now resuming the task")

			}
			counter:= 0
			//ret_ack_map key: NodeID-taskID value: []LineInfo (tuples that need to be batch acked)
			ret_ack_map := make(map[string][]LineInfo)
			total_tuples_processed := 0
			output_map_global := make(map[string]string)
			for msg := range chord {
					if strings.HasPrefix(msg, "INPUTBATCH: "){
						//input isnt actually a batch but a single tuple since there is no way to batch the input to stateful ops 
						// format 
						//INPUTBATCH: targetnodeID targettaskID inputNodeID inputtaskID
						//fmt.Println("Received message on channel ", msg)
						input := strings.SplitN(msg, "\n", 2)[1]
						header := strings.SplitN(msg, "\n", 2)[0]
						inputTaskID := strings.Split(header, " ")[4]
						inputNodeID := strings.Split(header, " ")[3]
						
						//fmt.Println("Input node ID ", inputNodeID)
						inputNodeID_int, err := strconv.Atoi(inputNodeID)
						if err != nil {
							fmt.Println("Error in parsing the input node ID")
							return
						}
						input_node_conn, ok := streamConnTable.Load(inputNodeID_int)
						if !ok {
							fmt.Println("Input node connection not found")
							return
						}
						//fmt.Println("Received input batch: ", input)
						//deserialize the input
						input_batch,err := deserializeLineInfoArray([]byte (input))
						if err != nil {
							fmt.Println("Error in deserializing the input batch")
							return
						}
						if input_batch[0].FileLineID == "*COMPLETED*" {
							completed_count++
							fmt.Println("Completed count ", completed_count)
							fmt.Println("Num tasks ", task.NumTasks)
							if completed_count == task.NumTasks{
								//inputs are completed, now clear the buffer and write to the hydfs
								PrintMapToConsole(output_map_global)
								StoreOutputOnHydfs(output_map_global, task.TaskOutputFile, hydfsConn, task.TaskID)
								StoreBufferOnHydfs(seen_storage_map, task.TaskLogFile, hydfsConn)
								//send ack to the input node
								ResolveStoredAcks(&ret_ack_map, streamConnTable)
								//sendAckInfoArray(input_node_conn.(net.Conn), batch, inputNodeID, inputTaskID)
								//fmt.Println("Sacked")
								fmt.Println("Second Stage Task "+ strconv.Itoa(task.TaskID) +" completed")
								fmt.Println("Clocked @ ", time.Since(birth_time).Milliseconds() ,"ms")
								fmt.Println("Total tuples processed ", total_tuples_processed)
								return 
							}
							continue 
						}
						//add the input batch to the ret_ack_map 
						if _, ok := ret_ack_map[inputNodeID+"-"+inputTaskID]; ok {
							ret_ack_map[inputNodeID+"-"+inputTaskID] = append(ret_ack_map[inputNodeID+"-"+inputTaskID], input_batch...)
						}else{
							ret_ack_map[inputNodeID+"-"+inputTaskID] = input_batch
						}
						var last_output string
						for _, line := range input_batch {
							//check if the word has been seen before
							key_stage_2 := GetStage2Key(line.FileLineID)
							if _, ok := seen_storage_map[key_stage_2]; ok {
								//send direct ack to the input node but drop the tuple here since it is a duplicate
								sendAckInfoArray(input_node_conn.(net.Conn), []LineInfo{line}, inputNodeID, inputTaskID)
								fmt.Println("Send Dupe Ack for  ", key_stage_2)
								continue
							}
							input_stage_2 := GetInputForStage2(line)
							//VM MARKER START
							last_output = RunOperator(task.TaskOperatorName, input_stage_2)
							//last_output = RunOperatorlocal(task.TaskOperatorName, input_stage_2, task.TaskID)
							//VM MARKER END
							total_tuples_processed++
							//buffer here is stored as fileLineID:word to maintain uniqueness
							seen_storage_map[key_stage_2] = "1"
						}
						//deserialize the the last output
						output_int_map := make(map[string]int)
						json.Unmarshal([]byte(last_output), &output_int_map)
						//convert int values to string values
						output_string_map := make(map[string]string)
						for k, v := range output_int_map {
							output_string_map[k] = strconv.Itoa(v)
						}
						output_map_global = output_string_map
						counter++
						if counter == stage_2_batch_size {
							//PrintMapToConsole(output_string_map)
							StoreOutputOnHydfs(output_string_map, task.TaskOutputFile, hydfsConn, task.TaskID)
							StoreBufferOnHydfs(seen_storage_map, task.TaskLogFile, hydfsConn)
							//send ack to the input node
							ResolveStoredAcks(&ret_ack_map, streamConnTable)
							//sendAckInfoArray(input_node_conn.(net.Conn), batch, inputNodeID, inputTaskID)
							//batch = make([]LineInfo, 0)
							//fmt.Println("Sacked")
							counter = 0
						}
						//StoreOutputOnHydfs(last_output, task.TaskOutputFile, hydfsConn, task.TaskID)
					}else if strings.Contains(msg, "STOPTASK: "){
						//exit the task
						fmt.Println("Task "+ strconv.Itoa(task.TaskID) +" completed")
					}
			}
		}else{
			fmt.Println("Invalid task stage")
		}
	}
}

func handleStreamDSConnection(isLeader bool, hydfsConn *SafeConn, conn net.Conn, taskChannelTable *sync.Map, streamTaskTable *sync.Map, streamConnTable *sync.Map, wg * sync.WaitGroup, m int){
	defer wg.Done()
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		msg, err := readMultilineMessage(reader, "END_OF_MSG")
		if err != nil {
			fmt.Println("Error in reading the message : ", err)
			return
		}
		msg = strings.Trim(msg, "\n")
		//fmt.Println("Message received from node: ", msg)
		go func (msg string){
			if strings.HasPrefix(msg, "TASK: "){
				//leader has sent a task to run 
				if isLeader{
					fmt.Println("Warning : Task received on Leader node, Should never happen")
					return
				}
				//parse the task
				taskJson := strings.Split(msg, " ")[1]
				task,err  := DeserializeTask([]byte(taskJson))
				if err != nil {
					fmt.Println("Error in deserializing the task")
					return
				}
				fmt.Println("Task received ", task)
				//create a channel for the task
				taskChannel := make(chan string, 30000)
				taskChannelTable.Store(task.TaskID, taskChannel)
				streamTaskTable.Store(self_id, []*Task{&task})
				//run the task
				go runStreamDSTask(hydfsConn, &task, streamConnTable,taskChannel, m)
			}else if strings.HasPrefix(msg, "ACK: "){
				//format ACK: targetNodeID targetTaskID
				//route the ack to target task 
				header := strings.SplitN(msg, "\n", 2)[0]

				targetNodeID := strings.Split(header, " ")[1]
				targetNodeID_int, err := strconv.Atoi(targetNodeID)
				if err != nil {
					fmt.Println("Error in parsing the target node ID")
					return
				}
				if targetNodeID_int != self_stream_id {
					fmt.Println("Ack wasn't supposed to come to this VM ", targetNodeID_int, self_stream_id)
					return
				}
				targetTaskID := strings.Split(header, " ")[2]
				targetTaskID_int, err := strconv.Atoi(targetTaskID)
				if err != nil {
					fmt.Println("Error in parsing the target task ID", 948, targetTaskID)
					return
				}
				//get the channel for the task
				taskChannel, ok := taskChannelTable.Load(targetTaskID_int)
				if !ok {
					fmt.Println("Task channel not found")
					return
				}
				taskChannel.(chan string) <- msg
			}else if strings.HasPrefix(msg, "RESUME: "){
				//format RESUME: targetNodeID targettaskID+content
				tokens:= strings.Split(msg, " ")
				targetNodeID := tokens[1]
				targetNodeID_int, err := strconv.Atoi(targetNodeID)
				if err != nil {
					fmt.Println("Error in parsing the target node ID")
					return
				}
				if targetNodeID_int != self_stream_id {
					fmt.Println("Resume wasn't supposed to come to this VM ", targetNodeID_int, self_stream_id)
					return
				}
				targetTaskID := strings.Split(tokens[2], "+")[0]
				targetTaskID_int, err := strconv.Atoi(targetTaskID)
				if err != nil {
					fmt.Println("Error in parsing the target task ID", 974, targetTaskID)
					return
				}
				//get the channel for the task
				taskChannel, ok := taskChannelTable.Load(targetTaskID_int)
				if !ok {
					fmt.Println("Task channel not found")
					return
				}
				msg = "RESUME: "+ tokens[2]
				taskChannel.(chan string) <- msg
			}else if strings.HasPrefix(msg, "COMPLETE: "){
				//format COMPLETE: targetTaskID
				tokens:= strings.Split(msg, " ")
				targetTaskID := tokens[1]
				targetTaskID_int, err := strconv.Atoi(targetTaskID)
				if err != nil {
					fmt.Println("Error in parsing the target task ID", 990, targetTaskID)
					return
				}
				//get the channel for the task
				taskChannel, ok := taskChannelTable.Load(targetTaskID_int)
				if !ok {
					fmt.Println("Task channel not found")
					return
				}
				taskChannel.(chan string) <- msg
			}else if strings.HasPrefix(msg, "INPUTBATCH: "){
				//format INPUTBATCH: targetnodeID targettaskID inputNodeID inputtaskID \n <serialized batch of input>
				header := strings.SplitN(msg, "\n", 2)[0]
				targettaskID := strings.Split(header, " ")[2]
				targettaskID_int, err := strconv.Atoi(targettaskID)
				if err != nil {
					fmt.Println("Error in parsing the target task ID", 1004, targettaskID)
					return
				}
				//get the channel for the task
				taskChannel, ok := taskChannelTable.Load(targettaskID_int)
				if !ok {
					fmt.Println("Task channel not found")
					return
				}
				taskChannel.(chan string) <- msg
			}else if strings.HasPrefix(msg, "STOPTASK: "){
				//format STOPTASK: targetTaskID
				tokens:= strings.Split(msg, " ")
				targetTaskID := tokens[1]
				targetTaskID_int, err := strconv.Atoi(targetTaskID)
				if err != nil {
					fmt.Println("Error in parsing the target task ID", 1019, targetTaskID)
					return
				}
				//get the channel for the task
				taskChannel, ok := taskChannelTable.Load(targetTaskID_int)
				if !ok {
					fmt.Println("Task channel not found")
					return
				}
				taskChannel.(chan string) <- msg
			}
		}(msg)
	}
}

func createStreamDsTCPConn(isLeader bool, hydfsConn* SafeConn, address string, taskChannelTable *sync.Map,streamTaskTable *sync.Map, streamConnTable *sync.Map,  wg * sync.WaitGroup, m int)(bool, net.Conn){
	fmt.Println(address, GetOutboundIP().String() + ":" + os.Getenv("SGP"))
	if address == GetOutboundIP().String() + ":" + os.Getenv("SGP") {
		fmt.Println("Cannot connect to self")
		return false, nil
	}
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Println("Error in connecting to node in streamDS")
		return false, nil
	}
	fmt.Println("Connected to node in streamDS at "+ address)
	wg.Add(1)
	go handleStreamDSConnection(isLeader, hydfsConn, conn, taskChannelTable, streamTaskTable, streamConnTable, wg, m)
	return true, conn
}

//handles connection between streamDS and HyDFS layer
func handleStreamDSConnectionMeta(isLeader bool,hydfsConn *SafeConn, taskChannelTable *sync.Map, StreamDSGlobalPort string, streamConnTable *sync.Map, streamTaskTable *sync.Map,wg * sync.WaitGroup, m int){
	defer wg.Done()
	defer hydfsConn.conn.Close()
	reader := bufio.NewReader(hydfsConn.conn)
	for{
		msg, err := readMultilineMessage(reader, "END_OF_MSG")
		if err != nil {
			fmt.Println("Self TCP Pipe with HYDFS Layer Broke! Cannot receive failure updates anymore")
			return
		}
		msg = strings.Trim(msg, "\n")
		fmt.Println("Message received from HyDFS layer: ", msg)
		go func (msg string){
			if strings.Contains(msg, "ADD"){
				//lands in as ADD address:PORT
				//port is HYDFSGLOBALPORT on VMs
				//need to still connect to StreamDSGlobalPort
				//but need to calculate nodeID using the HydfsGlobalPort
				msg = strings.Split(msg, " ")[1]
					
				//VM MARKER
				nodeID := GetPeerID(msg, m)
				address := strings.Split(msg, ":")[0] 
				address +=":"+ StreamDSGlobalPort
				msg = address
				//VM MARKER END

				//VM MARKER CHECK ?
				// port := strings.Split(msg, ":")[1]
				// address := strings.Split(msg, ":")[0]
				// manip_port := subtractStrings(port, 3030)
				// manip_msg := address + ":" + manip_port
				// nodeID := GetPeerID(manip_msg, m)
				//VM MARKER CHECK END
				fmt.Println("Adding node " + strconv.Itoa(nodeID) + " at " + msg)
				if succ, conn := createStreamDsTCPConn(isLeader, hydfsConn, msg, taskChannelTable, streamTaskTable,streamConnTable,wg,m); succ {
					ConnTableAdd(streamConnTable, nodeID, conn)
					streamTaskTable.Store(nodeID, []*Task{})
					fmt.Printf("Connected to %s - %s successfully\n", conn.RemoteAddr().String(), msg)
				} else {
					fmt.Printf("Failed to connect to %s", msg)
				}

			}else if strings.Contains(msg, "REMOVE"){
				msg = strings.Split(msg, " ")[1]
				//VM MARKER
				nodeID := GetPeerID(msg, m)
				address := strings.Split(msg, ":")[0] 
				address +=":"+ StreamDSGlobalPort
				msg = address
				//VM MARKER END
				//VM MARKER CHECK ?
				// port := strings.Split(msg, ":")[1]
				// address := strings.Split(msg, ":")[0]
				// manip_port := subtractStrings(port, 3030)
				// manip_msg := address + ":" + manip_port
				// nodeID := GetPeerID(manip_msg, m)
				//VM MARKER CHECK END
				//get the conn for the removed node
				conn_remove, ok := streamConnTable.Load(nodeID)
				if ok{
					verifyCloseTCPConn(conn_remove.(net.Conn), wg)
				}
				ConnTableRemove(streamConnTable, nodeID)
				
				fmt.Println("Removed node " + strconv.Itoa(nodeID) + " at " + msg)
				if isLeader{
					//reschedule the tasks
					rescheduleStreamDSTaskOnFailure(hydfsConn, nodeID, streamConnTable, streamTaskTable)
				}
			}
		}(msg)
	}

}

func startStreamDSListener(isLeader bool, hydfsConn *SafeConn, taskChannelTable *sync.Map, streamDSGlobalPort string, streamTaskTable *sync.Map, streamConnTable *sync.Map, wg *sync.WaitGroup, m int){
	ln, err := net.Listen("tcp", ":"+streamDSGlobalPort)
	if err != nil {
		fmt.Println("Error in starting the streamDS listener", err)
		return
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error in accepting the connection")
			return
		}
		go handleStreamDSConnection(isLeader, hydfsConn, conn, taskChannelTable, streamTaskTable, streamConnTable, wg, m)
	}
}
// connects to the HyDFS layer using the self pipe
func StartStreamDSPipe(hydfsSelfPort string) *SafeConn {
	conn, err := net.Dial("tcp", "localhost:"+hydfsSelfPort)
	if err != nil {
		fmt.Println("Error in connecting to hydfs layer")
		return nil
	}
	safeConn := &SafeConn{conn: conn}
	return safeConn
}

func setupStreamDSCommTerminal(isleader bool, hydfsConn *SafeConn, taskChannelTable *sync.Map,streamConnTable *sync.Map, streamTaskTable *sync.Map, wg *sync.WaitGroup){
	wg.Add(1)
	defer wg.Done()
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println(">>>")
		text, _ := reader.ReadString('\n')
		if strings.HasPrefix(text, "RainStorm "){
			//RainStorm operator_name1 operator_state1 operator_name2 operator_state2 input_file output_file num_tasks
			tokens:= strings.Split(text, " ")
			if len(tokens) != 8 {
				fmt.Println("Invalid command")
				continue
			}
			op1_name := tokens[1]
			op1_state := tokens[2]
			op2_name := tokens[3]
			op2_state := tokens[4]
			hydfsSrcFileName := tokens[5]
			hydfsDestFileName := tokens[6]
			num_tasks, err := strconv.Atoi(strings.Trim(tokens[7], "\n"))
			if err != nil {
				fmt.Println("Error in parsing the number of tasks")
				continue
			}
			//plan the tasks
			if !isleader {
				fmt.Println("Only the leader can plan the tasks")
				continue
			}
			taskMap := planStreamDSTasks(hydfsConn, hydfsSrcFileName, op1_name, op1_state, op2_name, op2_state, hydfsDestFileName, num_tasks, streamTaskTable)
			if taskMap == nil {
				fmt.Println("Error in planning the tasks")
				continue
			}
			//send the tasks to the nodes
			for nodeID, task := range taskMap {
				//convert nodeID to int 
				nodeID_int, err := strconv.Atoi(nodeID)
				if err != nil {
					fmt.Println("Error in converting the node ID to int")
					continue
				}
				conn, ok := streamConnTable.Load(nodeID_int)
				if !ok {
					fmt.Println("Connection to node not found")
					continue
				}
				sendTask(conn.(net.Conn), *task)
				fmt.Println("Task "+ strconv.Itoa(task.TaskID) +"sent to node ", nodeID)
			}
		}else if strings.HasPrefix(text, "STOPTASK: "){
				//format STOPTASK: targetTaskID
				text  = strings.Trim(text, "\n")
				tokens:= strings.Split(text, " ")
				targetTaskID := tokens[1]
				targetTaskID_int, err := strconv.Atoi(targetTaskID)
				if err != nil {
					fmt.Println("Error in parsing the target task ID", 1210, targetTaskID)
					continue
				}
				//get the channel for the task
				taskChannel, ok := taskChannelTable.Load(targetTaskID_int)
				if !ok {
					fmt.Println("Task channel not found")
					return
				}
				taskChannel.(chan string) <- text

		}else if strings.HasPrefix(text, "showSST"){
			//show the stream task table
			streamTaskTable.Range(func(key, value interface{}) bool {
				fmt.Println("Node ", key)
				tasks := value.([]*Task)
				for _, task := range tasks {
					fmt.Println("Task: ", task)
				}
				return true
			})
		}else if strings.HasPrefix(text, "showSCT"){
			//show the stream connection table
			streamConnTable.Range(func(key, value interface{}) bool {
				fmt.Println("Node ", key)
				fmt.Println("Conn: ", value)
				return true
			})
		}else if strings.HasPrefix(text, "quantify"){
			// quantifies the result of given hydfsFile as one, only to be run on leader
			//format quantify hydfsdestFile numtasks
			text = strings.Trim(text, "\n")
			if len(strings.Split(text, " ")) != 3 {
				fmt.Println("Invalid command")
				continue
			}
			num_tasks := strings.Split(text, " ")[2]
			hydfsDestFileName := strings.Split(text, " ")[1]
			num_tasks_int, err := strconv.Atoi(num_tasks)
			if err != nil {
				fmt.Println("Error in parsing the number of tasks")
				continue
			}
			output, err := QuantifyHydfsFile(hydfsConn, hydfsDestFileName, num_tasks_int)
			if err != nil {
				fmt.Println("Error in quantifying the hydfs file")
				continue
			}
			fmt.Println("Quantified output: \n", output)
		}
	}
}

func StartStreamDS(isLeader bool, safeConn *SafeConn, selfAddress string,hyDFSSelfPort string,streamDSGlobalPort string, wg *sync.WaitGroup){
	m := 10 
	self_stream_id = GetPeerID(selfAddress,m)
	fmt.Println("self_address received", selfAddress)
	self_id = self_stream_id
	streamConnTable := sync.Map{}
	streamTaskTable := sync.Map{} // nodeID -> list of reference tasks (*Task)
	taskChannelTable := sync.Map{} // taskID -> channel for task for current node
	//start the streamDS listener
	go handleStreamDSConnectionMeta(isLeader, safeConn, &taskChannelTable,streamDSGlobalPort, &streamConnTable, &streamTaskTable, wg, m)
	go startStreamDSListener(isLeader, safeConn, &taskChannelTable, streamDSGlobalPort, &streamTaskTable, &streamConnTable, wg, m)
	//start the streamDS comm terminal
	go setupStreamDSCommTerminal(isLeader, safeConn, &taskChannelTable ,&streamConnTable, &streamTaskTable, wg)
	wg.Wait()
	//wg.Done()
}
