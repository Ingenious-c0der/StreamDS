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
	GetFileFromHydfs(hydfsConn, hydfsSrcFileName, 10)
	//get the total lines in the file 
	total_lines, err := CountLines(hydfsSrcFileName)
	if err != nil {
		fmt.Println("Error in counting lines in the file")
		return nil
	}
	//get the partition size
	partitions := GetFairPartitions(total_lines, num_tasks)
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
			-1,
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
			-1, 
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
			"first",
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
			task_output.OutputTaskID, 
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
	for _, task := range taskMap {
		taskLogFile := task.TaskLogFile
		hydfsConn.SafeWrite([]byte("CREATEFILE: "+ taskLogFile))
	}
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
			//TODO new: check secondary effects on firstStage tasks since they might ack timeout if the source is lost for a while
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
				inputNodeConn, ok := streamConnTable.Load(inputNodeID)
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

func runStreamDSTask(hydfsConn * SafeConn, task *Task, streamTaskTable *sync.Map, streamConnTable *sync.Map,chord chan string, m int){
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
		GetFileFromHydfs(hydfsConn, source_file_name, 10)
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
			GetFileFromHydfs(hydfsConn, task.TaskLogFile, 10)
			bufferMap, err := ReadLastBuffer(task.TaskLogFile)
			if err != nil {
				fmt.Println("Error in reading the last buffer")
				return
			}
			//get the smallest unacked tuple
			smallest_unacked_tuple := math.MaxInt
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
		batch_size := 10
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
		ackTimer := time.NewTimer(5 * time.Second)
		paused := false
		tupleIndex := 0 
		for tupleIndex < len(tuples) || len(bufferMap) > 0{
			select {
			case msg := <- chord:
				if strings.HasPrefix(msg, "ACK: ") {
					//this will be a batch ack
					//remove all the tuples from the buffer that have been acked
					if !ackTimer.Stop() {
						<-ackTimer.C
					}
					ackTimer.Reset(5 * time.Second)
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
					fmt.Println("Ack tuple sample :", acked_tuples[0])
					for _, acked_tuple := range acked_tuples {
						delete(bufferMap, acked_tuple.FileLineID)
					}
					//Todo new: there is some slack here, what happens if the ack is not recorded i.e. the process crashes
					//here.
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
					ackTimer.Reset(5 * time.Second)
					lastAckTime = time.Now()
					//resend the bufferMap in batches
					batch := make([]LineInfo, 0, batchSize)
					for fileLineID, content := range bufferMap {
						batch = append(batch, LineInfo{FileLineID: fileLineID, Content: content})
						if len(batch) == batchSize {
							sendLineInfoArray(output_node_conn.(net.Conn), batch, output_node_ID_string, strconv.Itoa(task.OutputTaskID),strconv.Itoa(task.TaskID), strconv.Itoa(self_stream_id))
							batch = batch[:0] // Clear the batch
						}
					}
					// Send any remaining tuples
					if len(batch) > 0 {
						sendLineInfoArray(output_node_conn.(net.Conn), batch, output_node_ID_string, strconv.Itoa(task.OutputTaskID),strconv.Itoa(task.TaskID),strconv.Itoa(self_stream_id))
					}

				}
			case <- ackTimer.C:
				//its been 5 seconds since the last ack
				//stop the task momentarily till the reset message is received from the leader
				paused = true
				fmt.Println("Task paused ", lastAckTime, time.Now())
			default:
				if paused {
					time.Sleep(300 * time.Millisecond)
					fmt.Println("Task ", task.TaskID, " paused")
					continue
				}
				//send the tuple to the next stage
				if tupleIndex < len(tuples){
					tuple := tuples[tupleIndex]
					batch = append(batch, tuple)
					bufferMap[tuple.FileLineID] = tuple.Content
					tupleIndex++
					if len(batch) == batch_size {
						//send the batch to the next stage
						StoreBufferOnHydfs(bufferMap, task.TaskLogFile, hydfsConn)
						sendLineInfoArray(output_node_conn.(net.Conn), batch, output_node_ID_string, strconv.Itoa(task.OutputTaskID), strconv.Itoa(task.TaskID),strconv.Itoa(self_stream_id))
						batch = make([]LineInfo, 0)
					}
				}else if len(batch) > 0 {
					//send the remaining tuples in the batch
					StoreBufferOnHydfs(bufferMap, task.TaskLogFile, hydfsConn)
					sendLineInfoArray(output_node_conn.(net.Conn), batch, output_node_ID_string, strconv.Itoa(task.OutputTaskID),strconv.Itoa(task.TaskID),strconv.Itoa(self_stream_id))
					batch = make([]LineInfo, 0)
				}else {
					//all tuples have been sent, wait for ACKs to come in 
					time.Sleep(100 * time.Millisecond)
				}
			}
		}
		fmt.Println("Source Task "+ strconv.Itoa(task.TaskID) +" completed")
		//send a completed message to the next task maybe?

	}else if task.TaskType == "operator" {
		if task.TaskStage == "first" {
			//setup the message listener
			bufferMap := make(map[string]string)
			ackTimer := time.NewTimer(5 * time.Second)
			lastAckTime := time.Now()
			paused := false
			output_nodes := task.TaskOutputNodes
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
			input_node_ID := task.TaskInputNode
			input_node_ID_int, err := strconv.Atoi(input_node_ID)
			if err != nil {
				fmt.Println("Error in parsing the input node ID")
				return
			}
			input_node_conn, ok := streamConnTable.Load(input_node_ID_int)
			if !ok {
				fmt.Println("Input node connection not found")
				return
			}
			if task.DoReplay{
				//fetch the task.log file and the buffer
				GetFileFromHydfs(hydfsConn, task.TaskLogFile, 10)
				bufferMap, err = ReadLastBuffer(task.TaskLogFile)
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
					//hash the word to get the output node
					word := strings.Split(line.FileLineID, ":")[2]
					selected_node := MapWordToNode(word,m, output_nodes_int)
					output_node_conn, ok  := streamConnTable.Load(selected_node)
					if !ok {
						fmt.Println("Output node not found")
						return
					}
					sendLineInfoArray(output_node_conn.(net.Conn), []LineInfo{line}, strconv.Itoa(selected_node), strconv.Itoa(task.OutputTaskID), input_node_ID,strconv.Itoa(self_stream_id))
				}
				fmt.Println("Task replay successful, now resuming the task")
			}
			for {
				select{
				case msg := <- chord:
					if strings.HasPrefix(msg, "ACK: ") {
						//this will be a batch ack
						//remove all the tuples from the buffer that have been acked
						if !ackTimer.Stop() {
							<-ackTimer.C
						}
						ackTimer.Reset(5 * time.Second)
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
						fmt.Println("Ack tuple sample for stage 1 :", acked_tuples[0])
						for _, acked_tuple := range acked_tuples {
							delete(bufferMap, acked_tuple.FileLineID)
						}
						//Todo new: there is some slack here, what happens if the ack is not recorded i.e. the process crashes
						//here.
						StoreBufferOnHydfs(bufferMap, task.TaskLogFile, hydfsConn)
						lastAckTime = time.Now()
					}else if strings.HasPrefix(msg, "INPUTBATCH: "){
						//message format INPUTBATCH: targetnodeID targettaskID inputNodeID inputtaskID
						header := strings.SplitN(msg, "\n", 2)[0]
						inputtaskID := strings.Split(header, " ")[4]
						inputNodeID := strings.Split(header, " ")[3]
						//check if the input node has changed most likely due to a failure
						if inputNodeID != input_node_ID {
							input_node_ID = inputNodeID
							input_node_ID_int, err := strconv.Atoi(input_node_ID)
							if err != nil {
								fmt.Println("Error in parsing the input node ID")
								return
							}
							input_node_conn, ok = streamConnTable.Load(input_node_ID_int)
							if !ok {
								fmt.Println("Input node connection not found")
								return
							}
						}
						//<newline> <serialized batch of input>
						//remove the first line till \n in the message

						input := strings.SplitN(msg, "\n", 2)[1]
						fmt.Println("Received input batch: ", input)
						//deserialize the input
						input_batch,err := deserializeLineInfoArray([]byte (input))
						if err != nil {
							fmt.Println("Error in deserializing the input batch")
							return
						}
						currentBatch := make([]LineInfo, 0)
						for _, line := range input_batch {
							processed_output := RunOperator(task.TaskOperatorName, line.Content)
							var word_list []string
							err := json.Unmarshal([]byte(processed_output), &word_list)
							if err != nil {
								fmt.Println("Error in unmarshalling the processed output")
								return
							}
							//buffer here is stored as fileLineID:word to maintain uniqueness 
							for _, word := range word_list {
								if _, ok := seen_storage_map[line.FileLineID+":"+word]; ok {
									//send direct ack to the input node but drop the tuple here since it is a duplicate
									sendAckInfoArray(input_node_conn.(net.Conn), []LineInfo{line}, input_node_ID, strconv.Itoa(task.TaskID))
									fmt.Println("Sent Dupe Ack for  ", line.FileLineID+":"+word)
									continue
								}
								bufferMap[line.FileLineID+":"+word] = "1" //just a placeholder
								seen_storage_map[line.FileLineID+":"+word] = "1"
								currentBatch = append(currentBatch, LineInfo{FileLineID: line.FileLineID+":"+word, Content: "1"})
							}
						}
						//write the buffer map to the hydfs
						StoreBufferOnHydfs(bufferMap, task.TaskLogFile, hydfsConn)
						//send ack to the input node
						sendAckInfoArray(input_node_conn.(net.Conn), input_batch, input_node_ID, strconv.Itoa(task.TaskID))
						//send the batch to the output nodes
						//hash the word to get the output node
						//first convert the output nodes to int
						if !paused{

							for _, line := range currentBatch {
								//hash the word to get the output node
								word := strings.Split(line.FileLineID, ":")[2]
								selected_node := MapWordToNode(word,m, output_nodes_int)
								output_node_conn, ok  := streamConnTable.Load(selected_node)
								if !ok {
									fmt.Println("Output node not found")
									return
								}
								sendLineInfoArray(output_node_conn.(net.Conn), []LineInfo{line}, strconv.Itoa(selected_node), strconv.Itoa(task.OutputTaskID), inputtaskID,strconv.Itoa(self_stream_id))
							}
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
						//buffermap has entries like filename:linenumber:word
						if err != nil {
							fmt.Println("Error in parsing the output node ID")
							return
						}
						send_toBatch := make([]LineInfo, 0)
						for fileLineID := range bufferMap {
							send_toBatch = append(send_toBatch, LineInfo{FileLineID: fileLineID, Content: "1"})
						}
						for _, line := range send_toBatch {
							//hash the word to get the output node
							word := strings.Split(line.FileLineID, ":")[2]
							selected_node := MapWordToNode(word,m, output_nodes_int)
							output_node_conn, ok  := streamConnTable.Load(selected_node)
							if !ok {
								fmt.Println("Output node not found")
								return
							}
							sendLineInfoArray(output_node_conn.(net.Conn), []LineInfo{line}, strconv.Itoa(selected_node), strconv.Itoa(task.OutputTaskID), strconv.Itoa(task.TaskID),strconv.Itoa(self_stream_id))
						}
						fmt.Println("Resuming the task with new output nodes ", output_nodes) 
						paused = false
						ackTimer.Reset(5 * time.Second)
						lastAckTime = time.Now()
					}else if strings.HasPrefix(msg, "COMPLETE: "){
						//you are done with the task 
						fmt.Println("Task "+ strconv.Itoa(task.TaskID) +" completed")
						return
					}
				case <- ackTimer.C:
					//its been 5 seconds since the last ack
					//stop the task momentarily till the resume message is received from the leader
					paused = true
					fmt.Println("Task paused ", lastAckTime, time.Now())
				default:
					//wait for unpause
					fmt.Println("Task "+strconv.Itoa(task.TaskID)+" paused")
					time.Sleep(300 * time.Millisecond)
				}
			}
		}else if task.TaskStage == "second" {
			seen_storage_map := make(map[string]string)
			var bufferMap map[string]string
			bufferMap_int := make(map[string]int)
			err := error(nil)
			if task.DoReplay{
				//read into the seen storage map the task.txt file 
				GetFileFromHydfs(hydfsConn, task.TaskLogFile, 10)
				//execute merge to get the latest output from another node but same task
				//TODOnew : Execute merge from leader before resuming the task
				//ExecuteMergeOnHydfs(hydfsConn, task.TaskOutputFile)
				//assuming that the merge has been executed previously
				GetFileFromHydfs(hydfsConn, task.TaskOutputFile, 10)
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
				//hardcoded filename for now
				PopulateStatefulOperatorFile(bufferMap_int, "word_count_state.txt")
				seen_storage_map, err = ReadLastBuffer(task.TaskLogFile)
				if err != nil {
					fmt.Println("Error in reading the last buffer")
					return
				}
				fmt.Println("Task replay successful, now resuming the task")

			}
			for msg := range chord {
				
					if strings.HasPrefix(msg, "INPUTBATCH: "){
						// format 
						//INPUTBATCH: targetnodeID targettaskID inputNodeID inputtaskID
						input := strings.SplitN(msg, "\n", 2)[1]
						header := strings.SplitN(msg, "\n", 2)[0]
						inputNodeID := strings.Split(header, " ")[3]
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
						fmt.Println("Received input batch: ", input)
						//deserialize the input
						input_batch,err := deserializeLineInfoArray([]byte (input))
						//line of the format filename:lineNumber:word
						if err != nil {
							fmt.Println("Error in deserializing the input batch")
							return
						}
						var last_output string
						for _, line := range input_batch {
							//check if the word has been seen before
							if _, ok := seen_storage_map[line.FileLineID]; ok {
								//send direct ack to the input node but drop the tuple here since it is a duplicate
								sendAckInfoArray(input_node_conn.(net.Conn), []LineInfo{line}, inputNodeID, strconv.Itoa(task.TaskID))
								fmt.Println("Send Dupe Ack for  ", line.FileLineID)
								continue
							}
							word := strings.Split(line.FileLineID, ":")[2]
							last_output = RunOperator(task.TaskOperatorName, word)
							//buffer here is stored as fileLineID:word to maintain uniqueness
							seen_storage_map[line.FileLineID] = "1"
						}
						//deserialize the the last output
						output_int_map := make(map[string]int)
						json.Unmarshal([]byte(last_output), &output_int_map)
						//convert int values to string values
						output_string_map := make(map[string]string)
						for k, v := range output_int_map {
							output_string_map[k] = strconv.Itoa(v)
						}
						StoreOutputOnHydfs(output_string_map, task.TaskOutputFile, hydfsConn, task.TaskID)
						StoreBufferOnHydfs(seen_storage_map, task.TaskLogFile, hydfsConn)
						PrintMapToConsole(output_string_map)
						//send ack to the input node
						sendAckInfoArray(input_node_conn.(net.Conn), input_batch, inputNodeID, strconv.Itoa(task.TaskID))
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
			fmt.Println("Error in reading the message")
			return
		}
		msg = strings.Trim(msg, "\n")
		fmt.Println("Message received from node: ", msg)
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
				//create a channel for the task
				taskChannel := make(chan string)
				taskChannelTable.Store(task.TaskID, taskChannel)
				streamTaskTable.Store(task.TaskID, task)
				//run the task
				go runStreamDSTask(hydfsConn, &task, streamTaskTable, streamConnTable,taskChannel, m)
			}else if strings.HasPrefix(msg, "ACK: "){
				//format ACK: targetNodeID targetTaskID
				//route the ack to target task 
				tokens:= strings.Split(msg, " ")
				targetNodeID := tokens[1]
				targetNodeID_int, err := strconv.Atoi(targetNodeID)
				if err != nil {
					fmt.Println("Error in parsing the target node ID")
					return
				}
				if targetNodeID_int != self_stream_id {
					fmt.Println("Ack wasn't supposed to come to this VM ", targetNodeID_int, self_stream_id)
					return
				}
				targetTaskID := tokens[2]
				targetTaskID_int, err := strconv.Atoi(targetTaskID)
				if err != nil {
					fmt.Println("Error in parsing the target task ID")
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
					fmt.Println("Error in parsing the target task ID")
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
					fmt.Println("Error in parsing the target task ID")
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
					fmt.Println("Error in parsing the target task ID")
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
					fmt.Println("Error in parsing the target task ID")
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
				msg = strings.Split(msg, " ")[1]
				//expecting to receive address:UDPport here as token 	
				//VM MARKER
				address := strings.Split(msg, ":")[0] 
				address +=":"+ StreamDSGlobalPort
				msg = address
				//VM MARKER END
				nodeID := GetPeerID(msg, m)
				fmt.Println("Adding node " + strconv.Itoa(nodeID) + " at " + msg)
				if succ, conn := createStreamDsTCPConn(isLeader, hydfsConn, address, taskChannelTable, streamTaskTable,streamConnTable,wg,m); succ {
					ConnTableAdd(streamConnTable, nodeID, conn)
					streamTaskTable.Store(nodeID, []*Task{})
					fmt.Printf("Connected to %s - %s successfully\n", conn.RemoteAddr().String(), msg)
				} else {
					fmt.Printf("Failed to connect to %s", msg)
				}

			}else if strings.Contains(msg, "REMOVE"){
				msg = strings.Split(msg, " ")[1]
				//VM MARKER
				address := strings.Split(msg, ":")[0]
				address += ":"+ StreamDSGlobalPort //TODO: make sure this works out
				msg = address
				//VM MARKER END
				nodeID := GetPeerID(msg, m)
				//get the conn for the removed node
				conn_remove, ok := streamConnTable.Load(nodeID)
				if ok{
					verifyCloseTCPConn(conn_remove.(net.Conn), wg)
				}
				ConnTableRemove(streamConnTable, nodeID)
				
				fmt.Println("Removed node " + strconv.Itoa(nodeID) + " at " + msg)
				//TODO: check if any tasks were running on the failed node and restart that task if you are the leader
				//and in the end streamTaskTable.Delete(nodeID)
			}
		}(msg)
	}

}

func startStreamDSListener(isLeader bool, hydfsConn *SafeConn, taskChannelTable *sync.Map, streamDSGlobalPort string, streamTaskTable *sync.Map, streamConnTable *sync.Map, wg *sync.WaitGroup, m int){
	ln, err := net.Listen("tcp", ":"+streamDSGlobalPort)
	if err != nil {
		fmt.Println("Error in starting the streamDS listener")
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
func startStreamDSPipe(hydfsSelfPort string) *SafeConn {
	conn, err := net.Dial("tcp", "localhost:"+hydfsSelfPort)
	if err != nil {
		fmt.Println("Error in connecting to hydfs layer")
		return nil
	}
	safeConn := &SafeConn{conn: conn}
	return safeConn
}

func setupStreamDSCommTerminal(isleader bool, hydfsConn *SafeConn, streamConnTable *sync.Map, streamTaskTable *sync.Map, wg *sync.WaitGroup){
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
				conn, ok := streamConnTable.Load(nodeID)
				if !ok {
					fmt.Println("Connection to node not found")
					continue
				}
				sendTask(conn.(net.Conn), *task)
				fmt.Println("Task "+ strconv.Itoa(task.TaskID) +"sent to node ", nodeID)
			}
		}else if strings.HasPrefix(text, "STOP: "){

		}
	}
}

func StartStreamDS(isLeader bool, selfAddress string,hyDFSSelfPort string,streamDSGlobalPort string, wg *sync.WaitGroup){
	m := 10 
	self_stream_id = GetPeerID(selfAddress,m)
	streamConnTable := sync.Map{}
	streamTaskTable := sync.Map{} // nodeID -> list of reference tasks (*Task)
	taskChannelTable := sync.Map{} // taskID -> channel for task for current node
	safeConn:= startStreamDSPipe(hyDFSSelfPort)
	//start the streamDS listener
	go handleStreamDSConnectionMeta(isLeader, safeConn, &taskChannelTable,streamDSGlobalPort, &streamConnTable, &streamTaskTable, wg, m)
	go startStreamDSListener(isLeader, safeConn, &taskChannelTable, streamDSGlobalPort, &streamTaskTable, &streamConnTable, wg, m)
	//start the streamDS comm terminal
	go setupStreamDSCommTerminal(isLeader, safeConn, &streamConnTable, &streamTaskTable, wg)
	//wg.Done()
}
