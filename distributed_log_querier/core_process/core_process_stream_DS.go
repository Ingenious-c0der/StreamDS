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
var stage_1_batch_size = 300
var stage_2_batch_size = 250
var num_task_base_lim = 0

// only to be run on the leader node
func planStreamDSTasks(hydfsConn *SafeConn, hydfsSrcFileName string, op1_name string, op1_state string, op2_name string, op2_state string, hydfsDestFileName string, num_tasks int, streamTaskTable *sync.Map, op1_input string, op2_input string) map[string]*Task {
	// a way to find VMs with least number of tasks running on them
	//plan the source stage
	//get the hydfs src file size, divide it by num_tasks to get the partition size
	//send get to hydfs layer
	_, ok := streamTaskTable.Load(self_stream_id)
	if !ok {
		fmt.Println("Leader node not found in the streamTaskTable, sane check passed")
	} else {
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
	fmt.Println("File fetched from hydfs ", hydfsSrcFileName)
	//get the total lines in the file
	time.Sleep(1 * time.Second)
	total_lines, err := CountLines(hydfsSrcFileName)
	if err != nil {
		fmt.Println("Error in counting lines in the file")
		return nil
	}
	//get the partition size
	if num_tasks == 0 || total_lines == 0 {
		fmt.Println("No Op on num_tasks or total_lines")
		fmt.Println("Num_tasks: ", num_tasks)
		fmt.Println("Total_lines: ", total_lines)
		return nil
	}
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
			i+num_task_base_lim,
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
			[]string{op2_input},
			-1,
		)
		taskMap[strconv.Itoa(nodeID_selected)] = stage_2_task
		pre_tasks, ok := streamTaskTable.Load(nodeID_selected)
		if ok {
			pre_tasks = append(pre_tasks.([]*Task), stage_2_task)
			streamTaskTable.Store(nodeID_selected, pre_tasks)
		} else {
			streamTaskTable.Store(nodeID_selected, []*Task{stage_2_task})
		}
	}
	stage_two_nodes := []string{}
	for k := range taskMap {
		stage_two_nodes = append(stage_two_nodes, k)
	}
	stage_two_task_ids := make([]int, 0)
	for _, node_itr := range stage_two_nodes {
		stage_two_task_ids = append(stage_two_task_ids, taskMap[node_itr].TaskID)
	}
	stage_two_nodes, stage_two_task_ids = SortNodesByTaskIDs(stage_two_nodes, stage_two_task_ids) // always serialize the nodes by task ids
	num_task_base_lim += num_tasks
	//next schedule stage 1 tasks (utilize the taskMap keys as output nodes)
	stage_one_nodes := []string{}
	for i := 0; i < num_tasks; i++ {
		nodeID_selected := GetNodeWithLeastTasks(streamTaskTable)
		if nodeID_selected == -1 {
			fmt.Println("No node available to schedule the task")
			return nil
		}
		//this could be parameterized further
		stage_1_task := NewTask(
			i+num_task_base_lim,
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
			[]string{op1_input},
			-1,
		)
		taskMap[strconv.Itoa(nodeID_selected)] = stage_1_task
		pre_tasks, ok := streamTaskTable.Load(nodeID_selected)
		if ok {
			pre_tasks = append(pre_tasks.([]*Task), stage_1_task)
			streamTaskTable.Store(nodeID_selected, pre_tasks)
		} else {
			streamTaskTable.Store(nodeID_selected, []*Task{stage_1_task})
		}
		stage_one_nodes = append(stage_one_nodes, strconv.Itoa(nodeID_selected))
	}
	num_task_base_lim += num_tasks
	//use the stage_1_task nodes as output nodes for the source tasks
	//last schedule the source tasks
	//num_stages are hardcoded to 3
	for i := 0; i < num_tasks; i++ {
		nodeID_selected := GetNodeWithLeastTasks(streamTaskTable)
		//get one of the stage 1 nodes as the output node and remove it from the list
		output_node := stage_one_nodes[0]
		taskMap[output_node].InputTaskID = i + num_task_base_lim
		taskMap[output_node].TaskInputNode = strconv.Itoa(nodeID_selected)
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
		current_partition := partitions[i]
		fmt.Println("Partition: ", i+num_task_base_lim, current_partition)
		//schedule the source task
		source_task := NewTask(
			i+num_task_base_lim,
			nodeID_selected,
			"source",
			"active",
			"zero",
			"file",
			"single",
			[]string{output_node},
			"None",
			hydfsSrcFileName,
			strconv.Itoa(current_partition[0])+":"+strconv.Itoa(current_partition[1]),
			"None",
			"Stateless",
			"None",
			true,
			false,
			[]int{task_output.TaskID},
			num_tasks,
			[]string{"None"},
			-1,
		)
		taskMap[strconv.Itoa(nodeID_selected)] = source_task
		pre_tasks, ok := streamTaskTable.Load(nodeID_selected)
		if ok {
			pre_tasks = append(pre_tasks.([]*Task), source_task)
			streamTaskTable.Store(nodeID_selected, pre_tasks)
		} else {
			streamTaskTable.Store(nodeID_selected, []*Task{source_task})
		}
	}
	num_task_base_lim += num_tasks
	//create the tasklogFiles for each of the tasks
	fmt.Println("Creating task log files and output files")
	for _, task := range taskMap {
		taskLogFile := task.TaskLogFile
		hydfsConn.SafeWrite([]byte("CREATEFILE: " + taskLogFile + "END_OF_MSG\n"))
	}
	//create the hydfsDestFile
	hydfsConn.SafeWrite([]byte("CREATEFILE: " + hydfsDestFileName + "END_OF_MSG\n"))
	fmt.Println("Task planning completed")
	//return the taskMap
	return taskMap
}

// only to be run on the leader node
func rescheduleStreamDSTaskOnFailure(hydfsConn *SafeConn, FailedNodeIDMap *sync.Map, streamConnTable *sync.Map, streamTaskTable *sync.Map) {
	//first find the affected tasks
	FailedNodeIDs := []int{}
	FailedNodeIDMap.Range(func(key, value interface{}) bool {
		FailedNodeIDs = append(FailedNodeIDs, key.(int))
		return true
	})
	//empty the failed node map
	FailedNodeIDMap = &sync.Map{}
	fmt.Println("Failed Node IDs: ", FailedNodeIDs)
	fmt.Println("Streamtasktable before deletion")
	printStreamTaskTable(streamTaskTable)
	resume_message_map := make(map[int][]string)
	temp_stage_2_tasks := make(map[int]*[]Task)
	temp_stage_1_tasks := make(map[int]*[]Task)
	stage_2_new_nodes := []int{}
	affected_tasks := []*Task{}
	for _, FailedNodeID := range FailedNodeIDs {
		affected_tasks_itr, ok := streamTaskTable.Load(FailedNodeID)
		if !ok {
			fmt.Println("Failed node not found in the streamTaskTable")
			return
		}
		affected_tasks = append(affected_tasks, affected_tasks_itr.([]*Task)...)
		//remove the failed node from the streamTaskTable
		streamTaskTable.Delete(FailedNodeID)
	}

	for _, task := range affected_tasks {
		fmt.Println("Affected task : ", task.TaskID, task.TaskType, task.TaskStage)
		if task.TaskType == "source" {
			//steps
			//1. execute merge on the output file
			//2. get a node with least tasks after removing the failed node
			//3. update the streamTaskTable
			//4. send the task to the new node
			ExecuteMergeOnHydfs(hydfsConn, task.TaskLogFile)
			task.DoReplay = true
			task.TaskAssignedNode = GetNodeWithLeastTasks(streamTaskTable)
			pre_tasks, ok := streamTaskTable.Load(task.TaskAssignedNode)
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
			fmt.Println("Task " + strconv.Itoa(task.TaskID) + " rescheduled on node " + strconv.Itoa(task.TaskAssignedNode))
		} else if task.TaskType == "operator" {
			if task.TaskStage == "first" {
				//steps
				//1. execute merge on the tasklogfile
				//2. get a node with least tasks after removing the failed node
				//3. update the streamTaskTable
				//4. send the task to the new node
				//5. send the resume message to the inputNode of the task
				ExecuteMergeOnHydfs(hydfsConn, task.TaskLogFile)
				time.Sleep(3 * time.Second) //to let the hydfs layer stabilize
				task.DoReplay = true
				task.TaskAssignedNode = GetNodeWithLeastTasks(streamTaskTable)
				pre_tasks, ok := streamTaskTable.Load(task.TaskAssignedNode)
				if ok {
					pre_tasks = append(pre_tasks.([]*Task), task)
					streamTaskTable.Store(task.TaskAssignedNode, pre_tasks)
				} else {
					streamTaskTable.Store(task.TaskAssignedNode, []*Task{task})
				}
				if _, ok := temp_stage_1_tasks[task.TaskAssignedNode]; ok {
					*temp_stage_1_tasks[task.TaskAssignedNode] = append(*temp_stage_1_tasks[task.TaskAssignedNode], *task)
				} else {
					temp_stage_1_tasks[task.TaskAssignedNode] = &[]Task{*task}
				}
				//store the resume message to the input node
				inputNodeID := task.TaskInputNode
				inputNodeID_int, err := strconv.Atoi(inputNodeID)
				if err != nil {
					fmt.Println("Error in parsing the input node ID when rescheduling")
					return
				}
				//RESUME send format RESUME: targetNodeID targettaskID+new_output_node_id/s (PS. the targetNodeID is filtered out once receipt)
				task_output_nodes := []string{}
				task_output_nodes = append(task_output_nodes, strconv.Itoa(task.TaskAssignedNode))
				resume_msg := "RESUME: " + inputNodeID + " " + strconv.Itoa(task.InputTaskID) + "+" + strings.Join(task_output_nodes, " ")
				resume_message_map[inputNodeID_int] = append(resume_message_map[inputNodeID_int], resume_msg)
			} else if task.TaskStage == "second" {
				//steps
				//1. execute merge on the tasklogfile
				//2. execute merge on the hydfs output file
				//3. get a node with least tasks after removing the failed node
				//4. update the streamTaskTable
				//5. send the task to the new node
				//6. send the resume message to all the nodes that sent input to the task (i.e. all the stage1 tasks)
				//todo new: check if the 6th point here actually is okay . i.e. sending resume message when the task never paused, to replay
				//the buffer. or some other mechanism is needed
				//answer : every task in stage1 is paused when a task in stage2 fails. so this is okay
				ExecuteMergeOnHydfs(hydfsConn, task.TaskLogFile)
				time.Sleep(3 * time.Second) //to let the hydfs layer stabilize
				ExecuteMergeOnHydfs(hydfsConn, task.TaskOutputFile)
				time.Sleep(3 * time.Second) //to let the hydfs layer stabilize
				task.DoReplay = true
				task.TaskAssignedNode = GetNodeWithLeastTasks(streamTaskTable)
				pre_tasks, ok := streamTaskTable.Load(task.TaskAssignedNode)
				if ok {
					pre_tasks = append(pre_tasks.([]*Task), task)
					streamTaskTable.Store(task.TaskAssignedNode, pre_tasks)
				} else {
					streamTaskTable.Store(task.TaskAssignedNode, []*Task{task})
				}
				temp_stage_2_tasks[task.TaskAssignedNode] = &[]Task{*task}
				stage_2_new_nodes = append(stage_2_new_nodes, task.TaskAssignedNode)
			}
		}
	}
	fmt.Println(temp_stage_1_tasks)
	fmt.Println(temp_stage_2_tasks)
	fmt.Println(resume_message_map)
	fmt.Println("Stage 2 new nodes: ", stage_2_new_nodes)
	fmt.Println("Streamtasktable after deletion")
	printStreamTaskTable(streamTaskTable)
	if len(temp_stage_2_tasks) > 1 {
		//case where both failed nodes were stage 2 nodes
		//first reschedule the stage 2 tasks
		fmt.Println("Case where both failed nodes were stage 2 nodes")
		for nodeID, task_list := range temp_stage_2_tasks {
			nodeConn, ok := streamConnTable.Load(nodeID)
			if !ok {
				fmt.Println("Node connection not found")
				return
			}
			for _, task := range *task_list {
				sendTask(nodeConn.(net.Conn), task)
				fmt.Println("Task " + strconv.Itoa(task.TaskID) + " rescheduled on node " + strconv.Itoa(task.TaskAssignedNode))
			}
		}
		old_output_nodes := GetSampleOutputNodes(streamTaskTable)
		neo_output_nodes := mergeTaskList(old_output_nodes, stage_2_new_nodes, FailedNodeIDs)
		serialized_output_nodes := serializeOutputNodes(neo_output_nodes, streamTaskTable)
		//unpause all the stage 1 tasks
		fmt.Println("Resuming all the stage 1 tasks")
		fmt.Println("Sample output nodes: ", serialized_output_nodes)
		sendResumeMessageToAllStage1Nodes(streamTaskTable, streamConnTable, serialized_output_nodes)

	} else if len(temp_stage_2_tasks) == 1 {
		//case where one was stage 1 and the other was stage 2
		//first reschedule the stage 2 task
		fmt.Println("Case where one was stage 1 and the other was stage 2")
		for nodeID, task_list := range temp_stage_2_tasks {
			nodeConn, ok := streamConnTable.Load(nodeID)
			if !ok {
				fmt.Println("Node connection not found")
				return
			}
			for _, task := range *task_list {
				sendTask(nodeConn.(net.Conn), task)
				fmt.Println("Task " + strconv.Itoa(task.TaskID) + " rescheduled on node " + strconv.Itoa(task.TaskAssignedNode))
			}
		}
		//next reschedule the stage 1 task
		sample_output_nodes := []string{}
		for nodeID, task_list := range temp_stage_1_tasks {
			nodeConn, ok := streamConnTable.Load(nodeID)
			if !ok {
				fmt.Println("Node connection not found")
				return
			}
			for _, task := range *task_list {
				//update the output nodes of the task
				merged_list := mergeTaskList(task.TaskOutputNodes, stage_2_new_nodes, FailedNodeIDs)
				serialized_output_nodes := serializeOutputNodes(merged_list, streamTaskTable)
				task.TaskOutputNodes = serialized_output_nodes
				sample_output_nodes = serialized_output_nodes
				sendTask(nodeConn.(net.Conn), task)
				fmt.Println("Task " + strconv.Itoa(task.TaskID) + " rescheduled on node " + strconv.Itoa(task.TaskAssignedNode))
			}
		}
		//unpause all the stage 1 tasks
		fmt.Println("Resuming all the stage 1 tasks")
		fmt.Println("Sample output nodes: ", sample_output_nodes)
		sendResumeMessageToAllStage1Nodes(streamTaskTable, streamConnTable, sample_output_nodes)
		for nodeID, resume_msgs := range resume_message_map {
			nodeConn, ok := streamConnTable.Load(nodeID)
			if !ok {
				fmt.Println("Node connection not found")
				return
			}
			for _, resume_msg := range resume_msgs {
				nodeConn.(net.Conn).Write([]byte(resume_msg + "END_OF_MSG\n"))
			}
		}
		//unpause the source tasks as well

	} else if len(temp_stage_2_tasks) == 0 {
		//case where both failed nodes were stage 1 nodes
		fmt.Println("Case where both failed nodes were stage 1 nodes")
		for nodeID, task_list := range temp_stage_1_tasks {
			nodeConn, ok := streamConnTable.Load(nodeID)
			if !ok {
				fmt.Println("Node connection not found")
				return
			}
			for _, task := range *task_list {
				sendTask(nodeConn.(net.Conn), task)
				fmt.Println("Task " + strconv.Itoa(task.TaskID) + " rescheduled on node " + strconv.Itoa(task.TaskAssignedNode))
			}
		}
		for nodeID, resume_msgs := range resume_message_map {
			nodeConn, ok := streamConnTable.Load(nodeID)
			if !ok {
				fmt.Println("Node connection not found")
				return
			}
			for _, resume_msg := range resume_msgs {
				nodeConn.(net.Conn).Write([]byte(resume_msg + "END_OF_MSG\n"))
			}
		}

	}
}

func runStreamDSTask(hydfsConn *SafeConn, leaderConn net.Conn, task *Task, taskChannelTable *sync.Map, streamTaskTable *sync.Map, streamConnTable *sync.Map, chord chan string, m int) {
	//first carve out the structure of the task
	//get the nodeID of the task for validation
	defer close(chord)
	nodeID := task.TaskAssignedNode
	//check if its itself
	if nodeID != self_stream_id {
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
			//retry infinite times
			for {
				success = GetFileFromHydfs(hydfsConn, source_file_name, 10)
				if success {
					break
				}
			}
		}
		fmt.Println("File fetched successfully from hydfs ", source_file_name)
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
			fmt.Println("File fetched successfully from hydfs ", task.TaskLogFile)
			bufferMap, err := ReadLastBuffer(task.TaskLogFile)
			if err != nil {
				fmt.Println("Error in reading the last buffer")
				return
			}

			//get the smallest unacked tuple
			smallest_unacked_tuple := math.MaxInt
			for fileLineID := range bufferMap {
				line_num_int := GetSourceLineNumberFromKey(fileLineID)
				if line_num_int == -1 {
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
		} else {
			//get tuples from the file partition as (filename:lineNumber,line)
			total_tuples_to_read := end_line - start_line + 1
			tuples, err = ReadFilePartition(source_file_name, start_line, end_line)
			if err != nil {
				fmt.Println("Error in reading the file partition ", err)
				tuples = []LineInfo{}
			}
			if len(tuples) < total_tuples_to_read {
				fmt.Println(len(tuples), total_tuples_to_read)
				fmt.Println("All tuples not found in the partition")
				//retry reading the partition till tuples are found
				for {
					success:= GetFileFromHydfs(hydfsConn, source_file_name, 10)
					if !success {
						fmt.Println("Error in getting the file from hydfs, retrying.. ")
						//retry infinite times
						for {
							success = GetFileFromHydfs(hydfsConn, source_file_name, 10)
							if success {
								break
							}
						}
					}
					fmt.Println("Retrying reading the file partition")
					tuples, err = ReadFilePartition(source_file_name, start_line, end_line)
					if err != nil {
						fmt.Println("Error in reading the file partition again", err, start_line, end_line)
						continue
					}
					if len(tuples) >= total_tuples_to_read {
						break
					}
				}
			}
		}
		//start the send of tuples and receive of ack on the chord channel
		//also maintain persistent buffer for the tuples
		batch_size := stage_1_batch_size
		batch := make([]LineInfo, 0)
		bufferMap := make(map[string]string)
		output_node_ID_string := task.TaskOutputNodes[0]
		output_node_ID_int, err := strconv.Atoi(output_node_ID_string)
		paused := false
		if err != nil {
			fmt.Println("Error in parsing the output node ID")
			return
		}
		output_node_conn, ok := streamConnTable.Load(output_node_ID_int)
		if !ok {
			fmt.Println("Output node connection not found", output_node_ID_int)
			paused = true
		}
		
		tupleIndex := 0
		for tupleIndex < len(tuples) || len(bufferMap) > 0 {
			select {
			case msg := <-chord:
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
				} else if strings.HasPrefix(msg, "RESUME: ") {
					//format RESUME: targettaskID+new_output_node_id
					//example RESUME: 6+145
					//parse the new output node id
					output_node_ID_string := strings.Split(msg, "+")[1]
					output_node_ID_int, err := strconv.Atoi(output_node_ID_string)
					if err != nil {
						fmt.Println("Error in parsing the output node ID")
						return
					}
					task.TaskOutputNodes = []string{output_node_ID_string}
					output_node_conn, ok = streamConnTable.Load(output_node_ID_int)
					if !ok {
						fmt.Println("Output node connection not found")
						return
					}
					fmt.Println("Resuming the task with new output node ", output_node_ID_string)
					paused = false
					batchSize := stage_1_batch_size
					//resend the bufferMap in batches
					batch := make([]LineInfo, 0)
					for fileLineID, content := range bufferMap {
						batch = append(batch, LineInfo{FileLineID: fileLineID, Content: content})
						if len(batch) == batchSize {
							sendLineInfoArray(output_node_conn.(net.Conn), batch, output_node_ID_string, strconv.Itoa(task.OutputTaskIDs[0]), strconv.Itoa(task.TaskID), strconv.Itoa(self_stream_id))
							batch = make([]LineInfo, 0)
						}
					}
					// Send any remaining tuples
					if len(batch) > 0 {
						for _, line := range batch {
							batch = append(batch, LineInfo{FileLineID: line.FileLineID, Content: line.Content})
						}
						sendLineInfoArray(output_node_conn.(net.Conn), batch, output_node_ID_string, strconv.Itoa(task.OutputTaskIDs[0]), strconv.Itoa(task.TaskID), strconv.Itoa(self_stream_id))
					}

				} else if strings.HasPrefix(msg, "FAILURE: ") {
					//format FAILURE: nodeID
					//check if you were relaying to the failed node
					failed_node := strings.Split(msg, " ")[1]
					if failed_node == output_node_ID_string {
						//pause the task
						paused = true
						fmt.Println("Task paused due to failure of the output node", failed_node)
					} else {
						//stop for a few seconds to let the hydfs layer stabilize
						time.Sleep(4 * time.Second)
					}
				}
			default:
				if paused {
					time.Sleep(300 * time.Millisecond)
					//fmt.Println("Task ", task.TaskID, " paused")
					continue
				}
				//fmt.Println(time.Since(lastAckTime), time.Since(lastSentTime))
				//send the tuple to the next stage
				if tupleIndex < len(tuples) {
					tuple := tuples[tupleIndex]
					batch = append(batch, tuple)
					bufferMap[tuple.FileLineID] = tuple.Content
					tupleIndex++
					//fmt.Println(tupleIndex, tuple.FileLineID)
					if len(batch) == batch_size {
						//send the batch to the next stage
						StoreBufferOnHydfs(bufferMap, task.TaskLogFile, hydfsConn)
						sendLineInfoArray(output_node_conn.(net.Conn), batch, output_node_ID_string, strconv.Itoa(task.OutputTaskIDs[0]), strconv.Itoa(task.TaskID), strconv.Itoa(self_stream_id))
						batch = make([]LineInfo, 0)
					}
				} else if len(tuples) == tupleIndex {
					//send the completed message to the next stage
					time.Sleep(1 * time.Second) // to make sure that in case of perfect batch splits the last batch is always received before the completed message
					completed_line := LineInfo{FileLineID: "*COMPLETED*", Content: "1"}
					sendLineInfoArray(output_node_conn.(net.Conn), []LineInfo{completed_line}, output_node_ID_string, strconv.Itoa(task.OutputTaskIDs[0]), strconv.Itoa(task.TaskID), strconv.Itoa(self_stream_id))
					tupleIndex++ // just so that we never enter this block again
				} else if len(batch) > 0 {
					//send the remaining tuples in the batch
					StoreBufferOnHydfs(bufferMap, task.TaskLogFile, hydfsConn)
					sendLineInfoArray(output_node_conn.(net.Conn), batch, output_node_ID_string, strconv.Itoa(task.OutputTaskIDs[0]), strconv.Itoa(task.TaskID), strconv.Itoa(self_stream_id))
					batch = make([]LineInfo, 0)
				} else {
					//all tuples have been sent, wait for ACKs to come in
					time.Sleep(300 * time.Millisecond)
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
		fmt.Println("Source Task " + strconv.Itoa(task.TaskID) + " completed")
		EndTask(leaderConn, streamTaskTable, task)
		taskChannelTable.Delete(task.TaskID)

	} else if task.TaskType == "operator" {
		if task.TaskStage == "first" {
			//setup the message listener
			bufferMap := make(map[string]string)
			paused := false
			complete := false
			birth_time := time.Now()
			output_nodes := task.TaskOutputNodes
			completion_message_sent := false
			output_nodes_int := make([]int, 0)
			for _, node := range output_nodes {
				node_int, err := strconv.Atoi(node)
				if err != nil {
					fmt.Println("Error in converting the output node to int 0 :", output_nodes)
					return
				}
				output_nodes_int = append(output_nodes_int, node_int)
			}
			seen_storage_map := make(map[string]string)
			if task.DoReplay {
				//fetch the task.log file and the buffer
				success := GetFileFromHydfs(hydfsConn, task.TaskLogFile, 10)
				if !success {
					fmt.Println("Error in getting the file from hydfs, retrying.. ")
					//retry infinite times
					for {
						success = GetFileFromHydfs(hydfsConn, task.TaskLogFile, 10)
						if success {
							break
						}
					}
				}
				fmt.Println("File fetched successfully from hydfs ", task.TaskLogFile)
				bufferMap, err := ReadLastBuffer(task.TaskLogFile)
				if len(bufferMap) == 0 || err != nil {
					fmt.Println("No buffer found in the task log file when replaying")
					bufferMap = make(map[string]string)
				}
				if bufferMap == nil {
					fmt.Println("No buffer found in the task log file when replaying")
					bufferMap = make(map[string]string)
				}
				//fill the seen_storage_map
				for fileLineID := range bufferMap {
					seen_storage_map[fileLineID] = "1"
				}
				if err != nil {
					fmt.Println("Error in reading the last buffer")
					return
				}
				//resend all the tuples in the buffer at once
				send_toBatch := make([]LineInfo, 0)
				for fileLineID := range bufferMap {
					send_toBatch = append(send_toBatch, LineInfo{FileLineID: fileLineID, Content: "1"})
				}
				for _, line := range send_toBatch {
					hashable := GetHashableStage1(line)
					//manip end key -> usable entity to hash
					selected_node, selected_task := MapHashableToNodeAndTask(hashable, m, output_nodes_int, task.OutputTaskIDs)

					output_node_conn, ok := streamConnTable.Load(selected_node)
					if !ok {
						fmt.Println("Output node not found")
						//output node crashed when this task was busy processing, go to pause stage
						continue
					}
					sendLineInfoArray(output_node_conn.(net.Conn), []LineInfo{line}, strconv.Itoa(selected_node), strconv.Itoa(selected_task), strconv.Itoa(task.TaskID), strconv.Itoa(self_stream_id))
				}

				fmt.Println("Task replay successful, now resuming the task")
			}
			for {
				select {
				case msg := <-chord:
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

					} else if strings.HasPrefix(msg, "INPUTBATCH: ") {
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
						input_batch, err := deserializeLineInfoArray([]byte(input))
						if err != nil {
							fmt.Println("Error in deserializing the input batch")
							return
						}
						if len(input_batch) == 1 {
							if input_batch[0].FileLineID == "*COMPLETED*" {
								fmt.Println("Received completed message from the input node")
								complete = true
							}
							continue
						}
						//fmt.Println(input_batch[0], " - ", input_batch[len(input_batch)-1])
						currentBatch := make([]LineInfo, 0)
						for _, line := range input_batch {		
							processed_output := RunOperatorlocal(task.TaskOperatorName, task.TaskOperatorInput[0], line.Content, task.TaskID)
							output_list := GetOutputFromOperatorStage1(processed_output)
							fmt.Println("Output list: ", output_list)
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
								content := GetStage1Content(line.Content, output)
								bufferMap[key_stage_1] = content //just a placeholder
								seen_storage_map[key_stage_1] = content
								currentBatch = append(currentBatch, LineInfo{FileLineID: key_stage_1, Content: content})
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
						if paused {
							continue
						}
						for _, line := range currentBatch {
							//hash the word to get the output node
							//fileLineID or the key here is filename:linenumber:word-index
							//manip_key_op_2
							//hashable is the entity you want to extract from the key to hash on if at all.
							hashable := GetHashableStage1(line)
							//manip end key -> usable entity to hash
							selected_node, selected_task := MapHashableToNodeAndTask(hashable, m, output_nodes_int, task.OutputTaskIDs)
							output_node_conn, ok := streamConnTable.Load(selected_node)
							if !ok {
								fmt.Println("Output node not found")
								//output node crashed when this task was busy processing, go to pause stage
								continue
							}
							sendLineInfoArray(output_node_conn.(net.Conn), []LineInfo{line}, strconv.Itoa(selected_node), strconv.Itoa(selected_task), strconv.Itoa(task.TaskID), strconv.Itoa(self_stream_id))
						}
					} else if strings.HasPrefix(msg, "RESUME: ") {
						//format RESUME: targettaskID+new_output_node_ids
						//example "RESUME: 6+145 146 147"
						fmt.Println("Received resume ", msg)
						if !paused {
							continue
						}
						content := strings.Split(msg, "+")[1]
						task.TaskOutputNodes = strings.Split(content, " ")
						output_nodes = task.TaskOutputNodes
						output_nodes_int = make([]int, 0)
						for _, node := range output_nodes {
							node_int, err := strconv.Atoi(node)
							if err != nil {
								fmt.Println("Error in converting the output node to int 1: ", output_nodes)
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
							selected_node, selected_task := MapHashableToNodeAndTask(hashable, m, output_nodes_int, task.OutputTaskIDs)
							output_node_conn, ok := streamConnTable.Load(selected_node)
							if !ok {
								fmt.Println("Output node not found")
								//output node crashed when this task was busy processing, go to pause stage
								continue
							}
							sendLineInfoArray(output_node_conn.(net.Conn), []LineInfo{line}, strconv.Itoa(selected_node), strconv.Itoa(selected_task), strconv.Itoa(task.TaskID), strconv.Itoa(self_stream_id))
						}
						fmt.Println("Resuming the task with new output nodes ", output_nodes, task.OutputTaskIDs)
						paused = false
					} else if strings.HasPrefix(msg, "FAILURE: ") {
						//format FAILURE: nodeID
						//check if you were relaying to the failed node
						failed_node := strings.Split(msg, " ")[1]
						for _, output_node := range output_nodes {
							if output_node == failed_node {
								//pause the task
								paused = true
								fmt.Println("Task paused due to failure of the output node in stage 2", failed_node)
								break
							}
						}
						if !paused {
							//stop for a few seconds to let the hydfs layer stabilize
							time.Sleep(4 * time.Second)
						}
					}else if strings.HasPrefix(msg, "stoptask"){
						//stop the task
						fmt.Println("First Stage task " + strconv.Itoa(task.TaskID) + " completed")
						fmt.Println("Clocked @ ", time.Since(birth_time).Milliseconds(), "ms")
						EndTask(leaderConn, streamTaskTable, task)
						taskChannelTable.Delete(task.TaskID)
						return
					}
				default:
					//wait for unpause
					//fmt.Println("Task "+strconv.Itoa(task.TaskID)+" paused")
					if paused {
						if len(bufferMap) == 0 && complete && time.Since(birth_time).Seconds() > 10 {
							fmt.Println("First Stage Probably completed")
						}
						time.Sleep(300 * time.Millisecond)
					} else {
						if complete {
							if !completion_message_sent {
								//send completion message to all the stage 2 output nodes
								completed_line := LineInfo{FileLineID: "*COMPLETED*", Content: "1"}
								for i, output_node := range output_nodes_int {
									output_node_conn, ok := streamConnTable.Load(output_node)
									if !ok {
										fmt.Println("Output node not found")
										continue
									}
									sendLineInfoArray(output_node_conn.(net.Conn), []LineInfo{completed_line}, strconv.Itoa(output_node), strconv.Itoa(task.OutputTaskIDs[i]), strconv.Itoa(task.TaskID), strconv.Itoa(self_stream_id))
								}
								completion_message_sent = true
							}
						}
						if len(bufferMap) == 0 && complete {
							fmt.Println("First Stage Probably completed")
							time.Sleep(2*time.Second)
							// fmt.Println("First Stage task " + strconv.Itoa(task.TaskID) + " completed")
							// fmt.Println("Clocked @ ", time.Since(birth_time).Milliseconds(), "ms")
							// EndTask(leaderConn, streamTaskTable, task)
							// taskChannelTable.Delete(task.TaskID)
							// return
						}
						if completion_message_sent && len(bufferMap) > 0 {
							fmt.Println("Waiting for acks on ", len(bufferMap), " tuples")
							time.Sleep(1 * time.Second)
						}
					}
				}
			}
		} else if task.TaskStage == "second" {
			seen_storage_map := make(map[string]string)
			var bufferMap map[string]string
			bufferMap_int := make(map[string]int)
			lastbufferClearedTime := time.Now()
			completed_count := 0 // used to check if all the first stages are completed
			//todo new : also recover completed count from the buffer
			//storing the completed count in seen_storage_map with a special key
			birth_time := time.Now()
			err := error(nil)
			if task.DoReplay {
				//read into the seen storage map the task.txt file
				success := GetFileFromHydfs(hydfsConn, task.TaskLogFile, 10)
				if !success {
					fmt.Println("Error in getting the file from hydfs, retrying.. ")
					//retry infinite times
					for {
						success = GetFileFromHydfs(hydfsConn, task.TaskLogFile, 10)
						if success {
							break
						}
					}
				}
				fmt.Println("File fetched successfully from hydfs ", task.TaskLogFile)
				//assuming that the merge has been executed previously
				success_ret := GetFileFromHydfs(hydfsConn, task.TaskOutputFile, 10)
				if !success_ret {
					fmt.Println("Error in getting the file from hydfs, retrying.. ")
					//retry
					//retry infinite times
					for {
						success = GetFileFromHydfs(hydfsConn, task.TaskOutputFile, 10)
						if success {
							break
						}
					}
				}
				fmt.Println("File fetched successfully from hydfs ", task.TaskOutputFile)
				bufferMap, err = ReadLastBufferForTask(task.TaskOutputFile, task.TaskID)
				if bufferMap == nil {
					fmt.Println("No buffer found for the task")
					bufferMap = make(map[string]string)
				}
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
				if len(bufferMap_int) == 0 {
					fmt.Println("Populating empty state operator file")
				}else{
					fmt.Println("Populating state operator file with ", len(bufferMap_int), " entries")
				}
				PopulateStatefulOperatorFile(bufferMap_int, stateful_operator_state_file, task.TaskID)
				
				//a solution to really populate the seen_storage_map could be to find all the unqiue keys in the buffer map

				seen_storage_map, err = ReadLastBuffer(task.TaskLogFile)
				if err != nil {
					fmt.Println("Error in reading the last buffer")
					seen_storage_map = make(map[string]string)
				}
				if seen_storage_map == nil {
					fmt.Println("Seen storage map is nil")
					seen_storage_map = make(map[string]string)
				}
				// completed_countStr, ok := seen_storage_map["*COMPLETED*"]
				// if ok {
				// 	completed_count, err = strconv.Atoi(completed_countStr)
				// 	if err != nil {
				// 		fmt.Println("Error in converting the completed count to int, using zero, you might have to manually stop the task on completion")
				// 	}
				// } else {
				// 	fmt.Println("Completed count not found in the buffer, using zero, you might have to manually stop the task on completion")
				// }
				fmt.Println("Task replay successful, now resuming the task")

			}
			counter := 0
			//ret_ack_map key: NodeID-taskID value: []LineInfo (tuples that need to be batch acked)
			ret_ack_map := make(map[string][]LineInfo)
			total_output_tuples_generated := 0 
			total_tuples_processed := 0
			output_map_global := make(map[string]string)
			for {
				select {
				case msg := <-chord:
				if strings.HasPrefix(msg, "INPUTBATCH: ") {
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
					input_batch, err := deserializeLineInfoArray([]byte(input))
					if err != nil {
						fmt.Println("Error in deserializing the input batch")
						return
					}
					if input_batch[0].FileLineID == "*COMPLETED*"{ //|| completed_count == task.NumTasks {
						if completed_count != task.NumTasks {
							completed_count++
							seen_storage_map["*COMPLETED*"] = strconv.Itoa(completed_count)
							StoreBufferOnHydfs(seen_storage_map, task.TaskLogFile, hydfsConn)
						}
						//fmt.Println("Completed count ", completed_count)
						// fmt.Println("Num tasks ", task.NumTasks)
						// if completed_count == task.NumTasks {
						// 	//inputs are completed, now clear the buffer and write to the hydfs
						// 	PrintMapToConsole(output_map_global)
						// 	RelayOutputToLeader(leaderConn, output_map_global, task)
						// 	StoreOutputOnHydfs(output_map_global, task.TaskOutputFile, hydfsConn, task.TaskID)
						// 	StoreBufferOnHydfs(seen_storage_map, task.TaskLogFile, hydfsConn)
						// 	//send ack to the input node
						// 	ResolveStoredAcks(&ret_ack_map, streamConnTable)
						// 	//sendAckInfoArray(input_node_conn.(net.Conn), batch, inputNodeID, inputTaskID)
						// 	//fmt.Println("Sacked")
						// 	fmt.Println("Second Stage Task " + strconv.Itoa(task.TaskID) + " completed")
						// 	fmt.Println("Clocked @ ", time.Since(birth_time).Milliseconds(), "ms")
						// 	fmt.Println("Total tuples processed ", total_tuples_processed)
						// 	if task.TaskState == "stateful" {
						// 		//clear the stateful operator state file
						// 		
						// 	}
						// 	EndTask(leaderConn, streamTaskTable, task)
						// 	taskChannelTable.Delete(task.TaskID)
						// 	return
						// }
						continue
					}
					//add the input batch to the ret_ack_map
					if _, ok := ret_ack_map[inputNodeID+"-"+inputTaskID]; ok {
						ret_ack_map[inputNodeID+"-"+inputTaskID] = append(ret_ack_map[inputNodeID+"-"+inputTaskID], input_batch...)
					} else {
						ret_ack_map[inputNodeID+"-"+inputTaskID] = input_batch
					}
					var last_output string
					//var stateless_buffer map[string]string
					for _, line := range input_batch {
						//check if the word has been seen before
						key_stage_2 := GetStage2Key(line.FileLineID)
						if _, ok := seen_storage_map[key_stage_2]; ok {
							//send direct ack to the input node but drop the tuple here since it is a duplicate
							sendAckInfoArray(input_node_conn.(net.Conn), []LineInfo{line}, inputNodeID, inputTaskID)
							fmt.Println("Send Dupe Ack for  ", key_stage_2)
							continue
						}
						var input_stage_2 string
						if task.TaskState == "stateful" {
							input_stage_2 = GetInputForStage2Stateful(line)
							//fmt.Println("Input for stage 2 ", input_stage_2)
						} else {
							input_stage_2 = GetInputForStage2Stateless(line)
						}

						last_output = RunOperatorlocal(task.TaskOperatorName, task.TaskOperatorInput[0], input_stage_2, task.TaskID)
						fmt.Println("Last output ", last_output)
						if task.TaskState == "stateless" {
							output_list := GetOutputFromOperatorStageStateless2(last_output)
							temp_map := make(map[string]string)
							if len(output_list) == 2 {
								output_map_global[output_list[0]+"-"+output_list[1]] = "1"
								temp_map[output_list[0]+"-"+output_list[1]] = "1"
								total_output_tuples_generated++
							}else if len(output_list) == 1 {
								output_map_global[output_list[0]] = "1"
								temp_map[output_list[0]] = "1"
								total_output_tuples_generated++
							}
							// for _, output := range output_list {
							// 	output_map_global[output] = "1"
							// 	total_output_tuples_generated++
							// 	temp_map[output] = "1"
							// }
							RelayOutputToLeader(leaderConn, temp_map, task)
						} else {
							delta_map := GetOutputFromOperatorStageStateful2(last_output)
							updated_count := delta_map[input_stage_2]
							total_output_tuples_generated++
							//fmt.Println("Updated count for ", input_stage_2, updated_count)
							temp_map := make(map[string]string)
							temp_map[input_stage_2] = updated_count
							RelayOutputToLeader(leaderConn, temp_map, task)
						}
						//fmt.Println("Last output ", last_output)

						total_tuples_processed++
						//buffer here is stored as fileLineID:word to maintain uniqueness
						seen_storage_map[key_stage_2] = "1"
						//fmt.Println("Seen storage map size ", len(seen_storage_map))
					}
					//deserialize the the last output
					if task.TaskState == "stateful" {
						output_map_global = GetOutputFromOperatorStageStateful2(last_output)
					}
					counter++
					if counter == stage_2_batch_size {
						//PrintMapToConsole(output_string_map)
						StoreOutputOnHydfs(output_map_global, task.TaskOutputFile, hydfsConn, task.TaskID)
						StoreBufferOnHydfs(seen_storage_map, task.TaskLogFile, hydfsConn)
						//send ack to the input node
						lastbufferClearedTime = time.Now()
						ResolveStoredAcks(&ret_ack_map, streamConnTable)
						//sendAckInfoArray(input_node_conn.(net.Conn), batch, inputNodeID, inputTaskID)
						//batch = make([]LineInfo, 0)
						//fmt.Println("Sacked")
						counter = 0
					}
				} else if strings.Contains(msg, "stoptask") {
					//exit the task
					PrintMapToConsole(output_map_global)
					RelayOutputToLeader(leaderConn, output_map_global, task)
					StoreOutputOnHydfs(output_map_global, task.TaskOutputFile, hydfsConn, task.TaskID)
					StoreBufferOnHydfs(seen_storage_map, task.TaskLogFile, hydfsConn)
					//send ack to the input node
					ResolveStoredAcks(&ret_ack_map, streamConnTable)
					//sendAckInfoArray(input_node_conn.(net.Conn), batch, inputNodeID, inputTaskID)
					//fmt.Println("Sacked")
					if task.TaskState == "stateful" {
						//clear the stateful operator state file			
						ClearOperatorStatelocal(stateful_operator_state_file, task.TaskID)
					}
					fmt.Println("Task " + strconv.Itoa(task.TaskID) + " completed")
					fmt.Println("Clocked @ ", time.Since(birth_time).Milliseconds(), "ms")
					fmt.Println("Total tuples processed ", total_tuples_processed)
					if task.TaskState == "stateless" {
						fmt.Println("Total output tuples generated ", total_output_tuples_generated)
					}
					EndTask(leaderConn, streamTaskTable, task)
					taskChannelTable.Delete(task.TaskID)
					return
				} else if strings.HasPrefix(msg, "FAILURE: ") {
					//stop for a few seconds to let the hydfs layer stabilize
					time.Sleep(4 * time.Second)
				}
			
			default:
				if time.Since(lastbufferClearedTime).Seconds() > 8 && (len(ret_ack_map) > 0 || time.Since(lastbufferClearedTime).Seconds() > 20 ) {
					PrintMapToConsole(output_map_global)
					RelayOutputToLeader(leaderConn, output_map_global, task)
					StoreOutputOnHydfs(output_map_global, task.TaskOutputFile, hydfsConn, task.TaskID)
					StoreBufferOnHydfs(seen_storage_map, task.TaskLogFile, hydfsConn)
					//send ack to the input node
					ResolveStoredAcks(&ret_ack_map, streamConnTable)
					lastbufferClearedTime = time.Now()
					fmt.Println("Log From Task " + strconv.Itoa(task.TaskID))
				}else{
					time.Sleep(300 * time.Millisecond)
				}
			}
		}

		} else {
			fmt.Println("Invalid task stage")
		}
	}
}

func handleStreamDSConnection(isLeader bool, hydfsConn *SafeConn, conn net.Conn, taskChannelTable *sync.Map, streamTaskTable *sync.Map, streamConnTable *sync.Map, wg *sync.WaitGroup, m int) {
	defer wg.Done()
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		msg, err := readMultilineMessage(reader, "END_OF_MSG")
		if err != nil {
			fmt.Println("Connection closed with: ", conn.RemoteAddr())
			conn.Close()
			return
		}
		msg = strings.Trim(msg, "\n")
		//fmt.Println("Message received from node: ", msg)
		go func(msg string) {
			if strings.HasPrefix(msg, "TASK:+:") {
				//leader has sent a task to run
				if isLeader {
					fmt.Println("Warning : Task received on Leader node, Should never happen")
					return
				}
				//parse the task
				taskJson := strings.Split(msg, ":+:")[1]
				task, err := DeserializeTask([]byte(taskJson))
				if err != nil {
					fmt.Println("Error in deserializing the task", err, taskJson)
					return
				}
				fmt.Println("Task received ", task)
				//create a channel for the task
				taskChannel := make(chan string, 30000)
				taskChannelTable.Store(task.TaskID, taskChannel)
				streamTaskTable.Store(self_id, []*Task{&task})
				fmt.Println("Task channel created for task ", task.TaskID)
				//run the task
				go runStreamDSTask(hydfsConn, conn, &task, taskChannelTable, streamTaskTable, streamConnTable, taskChannel, m)
			} else if strings.HasPrefix(msg, "ACK: ") {
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
					fmt.Println("Task channel not found, ack for task ", targetTaskID_int)
					//print the taskChannelTableKeys
					taskChannelTable.Range(func(key, value interface{}) bool {
						fmt.Println("TaskChannelTable key ", key)
						return true
					})
					return
				}
				taskChannel.(chan string) <- msg
			} else if strings.HasPrefix(msg, "RESUME: ") {
				//format RESUME: targetNodeID targettaskID+content
				content:= strings.Split(msg, "+")[0]
				tokens := strings.Split(content, " ")
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
				targetTaskID := tokens[2]
				targetTaskID_int, err := strconv.Atoi(targetTaskID)
				if err != nil {
					fmt.Println("Error in parsing the target task ID", 974, targetTaskID)
					return
				}
				//get the channel for the task
				taskChannel, ok := taskChannelTable.Load(targetTaskID_int)
				if !ok {
					fmt.Println("Task channel not found, resume for task ", targetTaskID_int)
					//print the taskChannelTableKeys
					taskChannelTable.Range(func(key, value interface{}) bool {
						fmt.Println("TaskChannelTable key ", key)
						return true
					})
					return
				}
				msg = "RESUME: " + tokens[2] + "+" + strings.Split(msg, "+")[1]
				taskChannel.(chan string) <- msg
			} else if strings.HasPrefix(msg, "COMPLETE: ") {
				//format COMPLETE: targetTaskID
				tokens := strings.Split(msg, " ")
				targetTaskID := tokens[1]
				targetTaskID_int, err := strconv.Atoi(targetTaskID)
				if err != nil {
					fmt.Println("Error in parsing the target task ID", 990, targetTaskID)
					return
				}
				//get the channel for the task
				taskChannel, ok := taskChannelTable.Load(targetTaskID_int)
				if !ok {
					fmt.Println("Task channel not found, complete for task ", targetTaskID_int)
					//print the taskChannelTableKeys
					taskChannelTable.Range(func(key, value interface{}) bool {
						fmt.Println("TaskChannelTable key ", key)
						return true
					})
					return
				}
				taskChannel.(chan string) <- msg
			} else if strings.HasPrefix(msg, "INPUTBATCH: ") {
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
					fmt.Println("Task channel not found, input batch for task ", targettaskID_int)
					//print the taskChannelTableKeys
					taskChannelTable.Range(func(key, value interface{}) bool {
						fmt.Println("TaskChannelTable key ", key)
						return true
					})
					return
				}

				taskChannel.(chan string) <- msg
			} else if strings.HasPrefix(msg, "COMPLETEDTASK:") {
				//format COMPLETEDTASK: nodeID taskID
				if isLeader {
					tokens := strings.Split(msg, " ")
					targetTaskID := tokens[2]
					targetNodeId := tokens[1]
					targetTaskID_int, err := strconv.Atoi(targetTaskID)
					if err != nil {
						fmt.Println("Error in parsing the target task ID", 1019, targetTaskID)
						return
					}
					targetNodeID_int, err := strconv.Atoi(targetNodeId)
					if err != nil {
						fmt.Println("Error in parsing the target node ID", 1023, targetNodeId)
						return
					}
					//remove the task from the streamTaskTable
					pre_tasks, ok := streamTaskTable.Load(targetNodeID_int)
					if ok {
						tasks := pre_tasks.([]*Task)
						var new_tasks []*Task
						for _, task := range tasks {
							if task.TaskID != targetTaskID_int {
								new_tasks = append(new_tasks, task)
							}
						}
						streamTaskTable.Store(targetNodeID_int, new_tasks)
						fmt.Println("Task ", targetTaskID_int, " marked as completed on ", targetNodeID_int)
					} else {
						fmt.Println("Node not found in the streamTaskTable", targetNodeID_int)
						return
					}
				} else {
					fmt.Println("COMPLETEDTASK message received on non-leader node")
				}
			} else if strings.HasPrefix(msg, "TASKINFO:") {
				//format TASKINFO: nodeID taskID \n update
				if isLeader {
					//REMOVE BEFORE FLIGHT
					header := strings.SplitN(msg, "\n", 2)[0]
					body := strings.SplitN(msg, "\n", 2)[1]
					output_map := make(map[string]string)
					json.Unmarshal([]byte(body), &output_map)
					tokens := strings.Split(header, " ")
					targetTaskID := tokens[2]
					targetNodeId := tokens[1]
					fmt.Println("****TaskOutput ", targetTaskID, targetNodeId, "******")
					PrintMapToConsole(output_map)
					fmt.Println("****TaskOutput End******")
				} else {
					fmt.Println("TASKINFO message received on non-leader node")
				}

			} else if strings.HasPrefix(msg, "KILLSTREAMDS:") {
				fmt.Println("Killed")
				os.Exit(1)
			}else if strings.HasPrefix(msg, "RUNCOMPLETE: "){
				// "RUNCOMPLETE: " + taskID
				tokens := strings.Split(msg, " ")
				taskID := tokens[1]
				taskID_int, err := strconv.Atoi(taskID)
				if err != nil {
					fmt.Println("Error in parsing the task ID", 1067, taskID)
					return
				}
				//get the channel for the task
				taskChannel, ok := taskChannelTable.Load(taskID_int)
				if !ok {
					fmt.Println("Task channel not found, complete for task ", taskID_int)
					//print the taskChannelTableKeys
					taskChannelTable.Range(func(key, value interface{}) bool {
						fmt.Println("TaskChannelTable key ", key)
						return true
					})
					return
				}
				taskChannel.(chan string) <- "stoptask "
			}
		}(msg)
	}
}

func createStreamDsTCPConn(isLeader bool, hydfsConn *SafeConn, address string, taskChannelTable *sync.Map, streamTaskTable *sync.Map, streamConnTable *sync.Map, wg *sync.WaitGroup, m int) (bool, net.Conn) {
	fmt.Println(address, GetOutboundIP().String()+":"+os.Getenv("SGP"))
	if isLeader && (address == GetOutboundIP().String()+":"+os.Getenv("SGP")) {
		fmt.Println("Cannot connect to self")
		return false, nil
	}
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Println("Error in connecting to node in streamDS")
		return false, nil
	}
	fmt.Println("Connected to node in streamDS at " + address)
	wg.Add(1)
	go handleStreamDSConnection(isLeader, hydfsConn, conn, taskChannelTable, streamTaskTable, streamConnTable, wg, m)
	return true, conn
}

// handles connection between streamDS and HyDFS layer
func handleStreamDSConnectionMeta(isLeader bool, hydfsConn *SafeConn, taskChannelTable *sync.Map, failedNodeIDMap *sync.Map, StreamDSGlobalPort string, streamConnTable *sync.Map, streamTaskTable *sync.Map, wg *sync.WaitGroup, m int) {
	defer wg.Done()
	defer hydfsConn.conn.Close()
	reader := bufio.NewReader(hydfsConn.conn)
	for {
		msg, err := readMultilineMessage(reader, "END_OF_MSG")
		if err != nil {
			fmt.Println("Self TCP Pipe with HYDFS Layer Broke! Cannot receive failure updates anymore")
			return
		}
		msg = strings.Trim(msg, "\n")
		fmt.Println("Message received from HyDFS layer: ", msg)
		go func(msg string) {
			if strings.Contains(msg, "ADD") {
				//lands in as ADD address:PORT
				//port is HYDFSGLOBALPORT on VMs
				//need to still connect to StreamDSGlobalPort
				//but need to calculate nodeID using the HydfsGlobalPort
				msg = strings.Split(msg, " ")[1]

				//VM MARKER
				nodeID := GetPeerID(msg, m)
				address := strings.Split(msg, ":")[0]
				address += ":" + StreamDSGlobalPort
				msg = address
				//VM MARKER END

				//VM MARKER LOCAL
				// nodeID := GetPeerID(msg, m)
				// address := strings.Split(msg, ":")[0]
				// port := strings.Split(msg, ":")[1]
				// port_int, err := strconv.Atoi(port)
				// if err != nil {
				// 	fmt.Println("Error in converting the port to int in ADD")
				// 	return
				// }
				// port_int = port_int + 3030 // elevate to streamDS port range
				// msg := address + ":" + strconv.Itoa(port_int)
				//VM MARKER LOCAL END
				fmt.Println("Adding node " + strconv.Itoa(nodeID) + " at " + msg)
				if succ, conn := createStreamDsTCPConn(isLeader, hydfsConn, msg, taskChannelTable, streamTaskTable, streamConnTable, wg, m); succ {
					ConnTableAdd(streamConnTable, nodeID, conn)
					streamTaskTable.Store(nodeID, []*Task{})
					fmt.Printf("Connected to %s - %s successfully\n", conn.RemoteAddr().String(), msg)
				} else {
					fmt.Printf("Failed to connect to %s", msg)
				}

			} else if strings.Contains(msg, "REMOVE") {
				msg = strings.Split(msg, " ")[1]
				nodeID := GetPeerID(msg, m)
				//get the conn for the removed node
				conn_remove, ok := streamConnTable.Load(nodeID)
				if ok {
					verifyCloseTCPConn(conn_remove.(net.Conn), wg)
				}
				ConnTableRemove(streamConnTable, nodeID)
				//send every task an update about the failure node. The task will then pause
				//and wait for the leader to reschedule the task
				if !isLeader {
					//send the failure update to all the tasks
					//iterate over the taskChannel
					taskChannelTable.Range(func(key, value interface{}) bool {
						taskChannel := value.(chan string)
						taskChannel <- "FAILURE: " + strconv.Itoa(nodeID)
						return true
					})
				}
				fmt.Println("Removed node " + strconv.Itoa(nodeID) + " at " + msg)
				if isLeader {
					//reschedule the tasks
					failedNodeIDMap.Store(nodeID, true)
					if getSyncMapLength(failedNodeIDMap) == 2 {
						time.Sleep(5 * time.Second) //wait for the hydfs layer to stabilize
						rescheduleStreamDSTaskOnFailure(hydfsConn, failedNodeIDMap, streamConnTable, streamTaskTable)
						//clear the failedNodeIDMap
						failedNodeIDMap = &sync.Map{}
					}
				}
			}
		}(msg)
	}

}

func startStreamDSListener(isLeader bool, hydfsConn *SafeConn, taskChannelTable *sync.Map, streamDSGlobalPort string, streamTaskTable *sync.Map, streamConnTable *sync.Map, wg *sync.WaitGroup, m int) {
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

func setupStreamDSCommTerminal(isleader bool, hydfsConn *SafeConn, taskChannelTable *sync.Map, streamConnTable *sync.Map, streamTaskTable *sync.Map, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println(">>>")
		text, _ := reader.ReadString('\n')
		if strings.HasPrefix(text, "RainStorm ") {
			//RainStorm operator_name1 operator_state1 operator_name2 operator_state2 operator_1_input operator_2_input input_file output_file num_tasks dokill
			text = strings.TrimSpace(text)
			tokens := splitWithQuotes(text)
			if len(tokens) != 11 {
				fmt.Println("Invalid command")
				continue
			}
			op1_name := tokens[1]
			op1_state := tokens[2]
			op2_name := tokens[3]
			op2_state := tokens[4]
			op1_input := tokens[5]
			op2_input := tokens[6]
			hydfsSrcFileName := tokens[7]
			hydfsDestFileName := tokens[8]
			dokill := tokens[10]
			num_tasks, err := strconv.Atoi(tokens[9])
			if err != nil {
				fmt.Println("Error in parsing the number of tasks")
				continue
			}
			if op1_input != "None" && strings.Contains("\"", op1_input) {
				op1_input = strings.ReplaceAll(op1_input, "\"", "")
			}
			if op2_input != "None" && strings.Contains("\"", op2_input) {
				op2_input = strings.ReplaceAll(op2_input, "\"", "")
			}
			//plan the tasks
			if !isleader {
				fmt.Println("Only the leader can plan the tasks")
				continue
			}
			taskMap := planStreamDSTasks(hydfsConn, hydfsSrcFileName, op1_name, op1_state, op2_name, op2_state, hydfsDestFileName, num_tasks, streamTaskTable, op1_input, op2_input)
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
				fmt.Println("Task "+strconv.Itoa(task.TaskID)+"sent to node ", nodeID)
			}
			if strings.HasPrefix(dokill, "kill") {
				//kill two streamDS tasks after 1.5 seconds
				//select one stateful task and one stateless task
				range_start := num_task_base_lim - 3*num_tasks
				range_end := num_task_base_lim - num_tasks
				//fmt.Println("Killing 2 tasks from range ", range_start, " to ", range_end)
				taskIdsToCrash := []int{}
				for i := range_start; i < range_end; i++ {
					taskIdsToCrash = append(taskIdsToCrash, i)
				}
				fmt.Println("Selecting one from these tasks to crash ", taskIdsToCrash)
				//select 2 random tasks
				//for now select the first and last task to crash
				//taskID_1 := taskIdsToCrash[len(taskIdsToCrash)-2] //stateless
				taskID_1, taskID_2 := GetTasks(taskIdsToCrash, dokill)
				//taskID_2 := taskIdsToCrash[0] // stateful
				//get node ids for the tasks
				nodeID_1 := GetNodeIDForTask(taskID_1, streamTaskTable)
				nodeID_2 := GetNodeIDForTask(taskID_2, streamTaskTable)
				if nodeID_1 == -1 || nodeID_2 == -1 {
					fmt.Println("Error in getting the node ID for the task")
					continue
				}
				fmt.Println("Killing tasks ", taskID_1, " and ", taskID_2)
				fmt.Println("Node IDs ", nodeID_1, " and ", nodeID_2)
				time.Sleep(1500 * time.Millisecond)
				//send the kill message
				killStreamDSProcess(nodeID_1, hydfsConn, streamConnTable)
				killStreamDSProcess(nodeID_2, hydfsConn, streamConnTable)

			}
		} else if strings.HasPrefix(text, "stoptask ") {
			//format stoptask targetTaskID
			text = strings.Trim(text, "\n")
			tokens := strings.Split(text, " ")
			targetTaskID := tokens[1]
			targetTaskID_int, err := strconv.Atoi(targetTaskID)
			if err != nil {
				fmt.Println("Error in parsing the target task ID", 1210, targetTaskID)
				continue
			}
			//get the channel for the task
			taskChannel, ok := taskChannelTable.Load(targetTaskID_int)
			if !ok {
				fmt.Println("Task channel not found, stoptask for task ", targetTaskID_int)
				//print the taskChannelTableKeys
				taskChannelTable.Range(func(key, value interface{}) bool {
					fmt.Println("TaskChannelTable key ", key)
					return true
				})
				continue
			}
			taskChannel.(chan string) <- text

		} else if strings.HasPrefix(text, "showsst") {
			//show the stream task table
			fmt.Println("Stream Task Table : ", getSyncMapLength(streamTaskTable))
			streamTaskTable.Range(func(key, value interface{}) bool {
				fmt.Print(key, " : ")
				tasks := value.([]*Task)
				for _, task := range tasks {
					fmt.Print(task.TaskID, " ")
				}
				fmt.Println()
				return true
			})
		} else if strings.HasPrefix(text, "showsct") {
			//show the stream connection table
			fmt.Println("Stream Connection Table : ", getSyncMapLength(streamConnTable))
			streamConnTable.Range(func(key, value interface{}) bool {
				fmt.Println(key, " : ", value.(net.Conn).RemoteAddr())
				return true
			})
		} else if strings.HasPrefix(text, "quantify") {
			// quantifies the result of given hydfsFile as one, only to be run on leader
			//format quantify hydfsdestFile numtasks
			text = strings.Trim(text, "\n")
			if len(strings.Split(text, " ")) != 3 && len(strings.Split(text, " ")) != 4 {
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
			if len(strings.Split(text, " ")) == 4 {
				singleString := strings.Join(output, "")
				buffers, err := ParseBuffers(singleString)
				if err != nil {
					fmt.Println("Cannot display lengths, please view the file")
					continue
				}
				fmt.Printf("Number of parsed buffers: %d\n", len(buffers))
				total := 0
				for i, buffer := range buffers {
					fmt.Printf("Buffer %d - TaskID: %d, Data length: %d\n", 
						i+1, buffer.TaskID, len(buffer.Data))
					total += len(buffer.Data)
				}
				fmt.Println("Total Unique Records: ", total)
			}
		}else if strings.HasPrefix(text, "completerun"){
			if isleader{
				//send complete message to all the stage 2 nodes
				streamTaskTable.Range(func(key, value interface{}) bool {
					tasks := value.([]*Task)
					nodeID := key.(int)
					for _, task := range tasks {
						//send complete message
						completeMessage := "RUNCOMPLETE: " + strconv.Itoa(task.TaskID) + "END_OF_MSG\n"
						conn, ok := streamConnTable.Load(nodeID)
						if !ok {
							fmt.Println("Connection to node not found")
							continue
						}
						_, err := conn.(net.Conn).Write([]byte(completeMessage))
						if err != nil {
							fmt.Println("Error in sending the complete message")
							continue
						}
					}
					return true
				})
				fmt.Println("Complete message sent to all the nodes")
			}
		}
	}
}

func StartStreamDS(isLeader bool, safeConn *SafeConn, selfAddress string, hyDFSSelfPort string, streamDSGlobalPort string, wg *sync.WaitGroup) {
	m := 10
	self_stream_id = GetPeerID(selfAddress, m)
	fmt.Println("self_ stream id ", self_stream_id)
	fmt.Println("self_address received", selfAddress)
	self_id = self_stream_id
	streamConnTable := sync.Map{}
	streamTaskTable := sync.Map{}  // nodeID -> list of reference tasks (*Task)
	taskChannelTable := sync.Map{} // taskID -> channel for task for current node
	failedNodeIDMap := sync.Map{}  // nodeID -> true
	//start the streamDS listener
	ClearOperatorState(stateful_operator_state_file)
	go handleStreamDSConnectionMeta(isLeader, safeConn, &taskChannelTable, &failedNodeIDMap, streamDSGlobalPort, &streamConnTable, &streamTaskTable, wg, m)
	go startStreamDSListener(isLeader, safeConn, &taskChannelTable, streamDSGlobalPort, &streamTaskTable, &streamConnTable, wg, m)
	//start the streamDS comm terminal
	go setupStreamDSCommTerminal(isLeader, safeConn, &taskChannelTable, &streamConnTable, &streamTaskTable, wg)
	wg.Wait()
	//wg.Done()
}
