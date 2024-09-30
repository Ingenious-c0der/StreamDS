# G28 MP Submission
# Distributed Log Querier 
## By Sagar Abhyankar (sra9) and Aditya Kulkarni (aak14)

This document explains how to run all the codes, their functionality and the general structure of the directories. 

At first the directory might feel a bit too messy, but its really not once you get the hang of it. Please go through to this entire document before attempting to run the code. Its not spaghetti code, its just comprehensive code with a multitude of features and no bugs! All the codes are expected to run properly, if they don't the problem is likely how they were launched. Pretty sure we have made covefe proud! :) 
Anyway on to the actual explanation!

## MP2 Details (SWIM For failure detection)

To run the MP2, you need to run the `core_process_fail_detect_intf.go` in the `distributed_log_querier` folder, as simple as that. Use the following command.

Example usage for introducer (if a node is an introducer, it doesn't care about the `INTRO_ADDRESS` param value)
```bash
INTRO_ADDRESS=127.0.0.1:8081 VERSION=1.0 SELF_PORT=8081 LOG_FILE_NAME=server1.log IS_INTRODUCER=True go run core_process_fail_detect_intf.go
```
Example usage, for non introducer nodes
```bash
INTRO_ADDRESS=172.22.156.92:8081 VERSION=1.0 SELF_PORT=8082 LOG_FILE_NAME=server2.log IS_INTRODUCER= go run core_process_fail_detect_intf.go
```

#### MP2 terminal commands

`LEAVE` : Node leaves the group 

`PRNT SUBSET` : Node prints out the current ping targets

`PRNT MEMSET` : Node prints out the full membership list

`list_self` : Print out node hash of current Node (self)

`enable_sus` : Switch operation mode to SUSPECT

`disable_sus` : Switch operation mode to NONSUSPECT 

`status_sus` : Prints out the current mode of operation

`list_sus` : Prints out the list of currently suspected nodes




### MP 1 details below

##### If you are really low on time and don't want to look around how things work in the code, click [here](https://gitlab.engr.illinois.edu/aak14/g28/-/tree/main?ref_type=heads#1-running-and-verifying-demo) to strictly run and test the behaviors expected! (for MP1 only)

### Directory structure
The main directory is distributed_log_querier which is also the package distributed_log_querier, and it houses 2 directories and the following files.
#### core_process/
This directory contains core_process.go which is the main logic of our distributed log querier system, it should not be run directly even if it contains the main() (unused) function as it only contains and exports the neccesarry library functions to interfaces.
#### functions_utility/
This directory contains function_utility package which is a library of functions used for unit testing. 
#### go.mod 
This file contains all the information about the module. We used go 1.21.0 (latest 1.23.0) for compatibility with the go available on the VM


#### core_process_auto_interface.go (used for demo)
We use this file during the demo, and if you want to pass the VM environments such as the VM name, ip_address and list of peer ip addresses you should use this interface! Running this file starts a single instance on the current machine.
Example command to run it (you need to be in the distributed_log_querier directory)
##### usage
```bash
SELF_NAME=vm1 PORT="8080" AUTO_ADDRESSES="172.22.158.92:8080 172.22.94.92:8080 172.22.156.93:8080 172.22.158.93:8080 172.22.94.93:8080 172.22.156.94:8080 172.22.158.94:8080 172.22.94.94:8080 172.22.156.95:8080" go run core_process_auto_interface.go
```
- The SELF_NAME of the vm is kind of hard coded for the sake of simplicity to be same as the log filename it reads from (vm(i).log like vm1,vm2..vm10).
- The AUTO_ADDRESSES variable should be the list of addresses the vm should connect to, for local launches it can also be list like this "[::]:8080, ..., [::]:8089"
- The value for PORT can be anything as long as the peers know about it! We had kept it to 8080 on all machines, but can be anything really. While running locally, obviously one needs to make sure every machine is launched on a different port!


#### core_process_manual_interface.go
Its almost same as the automatic interface, the only difference is that instead of passing values for variables from command line, simply run the file and the terminal will ask you for inputs for the necessary variable values such as name port etc. (you need to be in the distributed_log_querier directory)
###### important! : make sure that the main() function in core_process_auto_interface.go is either commented out or its name is changed to something else otherwise it will lead to name clash in the same "package main", similarly make sure that the name of the function in core_process_manual_interface.go is main() and not main_manual()
##### usage
```bash
go run core_process_manual_interface.go
```

#### log files
These are the logs provided to us via the Piazza Post, all the testing and inferences were drawn from these. Be rest assured that during the demo in a distributed environment although the repository contains all the log files in one place, we delete all other unnecessary log files (we don't cheat in anyway) except for the log file for that particular instance. 

#### .gitignore
ignoring local results too heavy for git

### Unit Tests/ Testing Coverage & Suite
We have two types of unit testing available based on the environment in which they are run.
#### 1. Unit testing in distributed environment 
If you mean to test in a VM cluster, use the testing files starting with the word distributed which include the following, more on each one of them up ahead. (we apologize for the ugly long names)
##### distributed_main_spawner_test.go :
Tests connection setup and runs one grep command (you can crash one of the vms during this test and that will serve as the fault tolerance unit test, since just by sheer logic we are sure it will work perfectly :O )
##### distributed_different_grep_commands_test.go :
Tests connection setup and also different grep commands
##### distributed_generate_logs_and_verify_test.go :
Generates custom log files on each VM and then runs the grep command to verify the match count



###### important! Distributed tests assume that the peer VMs are already running the main program (core_process.go launched with one of the interfaces) with correct names and ports. Its okay if they are not connected to any other VM at the moment, since that really isn't needed to verify required behavior and pass the test. Also the addresses are hard coded for our VM cluster, but it can be easily changed by tweaking the contents of the auto_addresses list [here](https://gitlab.engr.illinois.edu/aak14/g28/-/blob/main/distributed_log_querier/distributed_generate_logs_and_verify_test.go?ref_type=heads#L30-L41) (and similarly in other distributed unit test files). The self_address is also hard coded (the address of the VM in the cluster which will run the tests), which can also be modified [here](https://gitlab.engr.illinois.edu/aak14/g28/-/blob/main/distributed_log_querier/distributed_generate_logs_and_verify_test.go?ref_type=heads#L42). The address for the tester should generally appear first in the list of auto_addresses
Example usage of the above given tests (assuming all the other peer VMs are already running and you should be in the distributed_log_querier directory). By default the log generation and grep commands test works only on 4 VMs, which can easily be extended to more instances by modifying the NUM_INSTANCES variable found [here](https://gitlab.engr.illinois.edu/aak14/g28/-/blob/main/distributed_log_querier/distributed_generate_logs_and_verify_test.go?ref_type=heads#L29) 

```bash

go test -v distributed_main_spawner_test.go

go test -v distributed_different_grep_commands_test.go 

CUSTOM_FILENAME="TEST_NEW.log" CUSTOM_PATTERNS="HELLO,WORLD,new,something,100,200,500" go test -v distributed_generate_logs_and_verify_test.go

```
#### How does the log generation and verification unit test work?
So as you can see there are two parameters for running that command, first is 
- CUSTOM_FILENAME : This parameter specifies the name of the log file to be generated on the distributed system. Defaults to `test_run.log` if the param is not passed
- CUSTOM_PATTERNS : This first entry in this list of words specifies the infrequent pattern that will appear exactly only 10 times in the log file. The test will show the output of the grep commands on all the patterns(words) passed in the `CUSTOM_PATTERNS` list. If it is not passed, defaults to `NEWLINE` as the pattern. 

##### important! The other contents of the generated log file are as follows, which can be used to verify the behavior 
- The matching pattern (first word passed in `CUSTOM_PATTERNS`) occurs 10 times
- The line `RANDOM TEXT` appears exactly 100 times
- The line `100 200 300` appears exactly 1000 times.
We think this is enough data to test and verify whether your grep is working accurately. 
The code for it can be found [here](https://gitlab.engr.illinois.edu/aak14/g28/-/blob/main/distributed_log_querier/core_process/core_process.go?ref_type=heads#L42-L84) if you want to tweak the number of patterns or the contents of the log file. 

#### 2. Unit testing in local environment. 
Don't have the time to spin up the cluster to verify behavior which can be simulated locally anyway? Well you have found the right section! These unit test files simulate the cluster environment by simply spawning new "VMs", each on a different port of the same machine. It is more comprehensive in the sense that each machine connects to every other machine from the start and you DO NOT need to have any other peers started and ready to go.
The test itself will spawn all the required VMs and close them gracefully after the operation is done.
Following are the unit test files that fall under that category
- main_spawner_test.go 
- generate_logs_and_verify_test.go
- different_grep_commands_test.go

The only notable differences other than the running environment for these tests vs the distributed ones is that main_spawner_test runs grep command from each VM in the simulated cluster after setting up the connections. 
Secondly, generate_logs_and_verify_test.go does not take custom parameters unlike the distributed one. (Can be changed easily if wanted)
example usage
```bash
go test main_spawner_test.go 
go test generate_logs_and_verify_test.go
go test different_grep_commands_test.go
```




### 1. Running and Verifying (DEMO)
Following are the Steps you need to follow to get a working interconnected VM cluster, the distributed log querier!
#### Step 1
Run the following command (after changing the directory to distributed_log_querier/) on each machine you want to be part of the distributed log querier system

```bash
SELF_NAME=vm1 PORT="8080" AUTO_ADDRESSES="172.22.158.92:8080 172.22.94.92:8080 172.22.156.93:8080 172.22.158.93:8080 172.22.94.93:8080 172.22.156.94:8080 172.22.158.94:8080 172.22.94.94:8080 172.22.156.95:8080" go run core_process_auto_interface.go
```
only change the AUTO_ADDRESSES list to whatever suites your cluster, but make sure its similar to the list above (white character spacing and ports) Do not move to Step 2 till all of the needed VMs start running the script

#### Step 2
After running the script on all instances, you should see the "Started Listening on (XXXX) port" Thats a good sign! that means the client is up and running, ready to take your input.
Now, type the command `CONN AUTO` as input on each instance of the cluster, this ensures that the VM automatically connects to all the other VMs as specified in `AUTO_ADDRESSES` and adds them to its Peer List. We cannot automate this! simply because we don't know if all of the other machines are up and ready to accept (Listening State) to accept connections. We can set a timeout, but why rush it :(? 

#### Step 3
After Step 2 you should now see messages like CONN PEXCG and CONN REXCG, don't worry thats just the message passing between the VMs for names and connection establishment. Give it a second or 2 and you should be all connected and ready to go!
Now, simply type in your `grep` command (without filename.log) on any of the connected VM and it will serve you with the needed output.
PLEASE NOTE that you do not need to pass the name of the filename that is `vm1.log` or similar. It will be automatically appended for each VM, your command should include everything EXCEPT the file name, for example as follows. 
`grep 'PATTERN'` or `grep -c 'PATTERN' -i` or `grep -E ".*"`
Now if you want to position the filename (common requirement with pipe operators), or pass a different filename (given that it exists) please read the last line of this section. 

Upon the completion of the run you will see the total matching lines from each machine, Grand Total and the latency. It will not print out the output to the terminal but rather store the output of the latest grep run on `vm(i).txt` file, which can be used to see exact matching lines.  See some of the screenshots from our run [here](https://gitlab.engr.illinois.edu/aak14/g28#some-screenshots-of-the-run-on-10-vms)
The system supports every possible query with grep, but for advanced usage take a look [here](https://gitlab.engr.illinois.edu/aak14/g28#advanced-grep-usage)

### 2. Running Unit Tests (Distributed unit tests)
#### Step 1
In unit testing, one node will act as the Tester and the rest as the distributed_log_querier.
Make sure you start all the VMs (NOT THE TESTER NODE) with the following command , the auto_addresses may or may not be empty, we dont really use them for testing anyway
```bash
SELF_NAME=vm2 PORT="8080" AUTO_ADDRESSES="" go run core_process_auto_interface.go
```
Make sure to change the machine name for each instance. Port can be constant. The actual address for the tester node is hard coded, and can be changed [here](https://gitlab.engr.illinois.edu/aak14/g28/-/blob/main/distributed_log_querier/distributed_generate_logs_and_verify_test.go?ref_type=heads#L42)
#### Step 2
Once all the "to be peers" to the tester nodes are up in the Listening State, we can now run the following on the "Tester" node. Similarly for the other distributed unit tests too 
```bash 
CUSTOM_FILENAME="TEST_NEW.log" CUSTOM_PATTERNS="HELLO,WORLD,new,something,100,200,500" go test -v distributed_generate_logs_and_verify_test.go
```

### 3. Running Unit Tests (Local unit tests)
#### The only step
Just run the test file in the distributed_log_querier directory, and sit back :) 
example usages 
```bash
go test main_spawner_test.go 
go test generate_logs_and_verify_test.go
go test different_grep_commands_test.go
```


### Advanced grep usage
Grep mostly works as you would expect it to, but we noticed some areas where you might face 
problems with how we implement the grep command. Those are filename placement in the query for piped queries and when you want to change the filename

##### Case 1 : Position the filename in the query
You simply need to add the tag `<filename>` in the location where you want the filename to be placed, remember, it shouldn't be the actual filename, just the text `<filename>`

example usage
```bash
grep 'ERROR' <filename> | grep -v 'DEBUG'
```
the above query will be converted to the following automatically by the system. i: vm number
```bash 
grep 'ERROR' vm(i).log | grep -v 'DEBUG'
```
Note that if the tag wasn't used, it would be translated to the following command (appended at the end)
```bash 
grep 'ERROR' | grep -v 'DEBUG' vm(i).log
```
Just remember that `<filename>` is not just a placeholder, its actual syntax

##### Case 2 : Name of the filename in the query
If you do not want to use the default vm(i).log file on that particular VM, you can simply pass your favorite filename instead in the grep command as follows. 

```bash 
grep 'PATTERN' <fnactual my_custom_file.log>
```
here the query will run on the filename `my_custom_file.log`, translated ti

```bash 
grep 'PATTERN' my_custom_file.log
```
please note that the `<fnactual>` does not help with positioning, use `<filename>` for it instead! for example
```bash
grep 'ERROR' <fnactual test.log> | grep -v 'DEBUG'
```
will translate to the following, notice its simply appended at the end of the query
```bash
grep 'ERROR' | grep -v 'DEBUG test.log'
```

##### Case 3 : Both name and positioning of the filename in the Query
Combine the logic of above two, example as follows. 
```bash
grep 'ERROR' <fnactual test.log> <filename> | grep -v 'DEBUG'
```
will translate to 
```bash
grep 'ERROR' test.log | grep -v 'DEBUG'
```



#### Some screenshots of the run on 10 VMs 
![alt text](<Pasted Graphic 5.png>)
![alt text](<Pasted Graphic 6.png>)
![alt text](<Pasted Graphic 7.png>)

### Looking for something else?
Sorry! All of this documentation was hand written and we found only so much time to cover the above contents before submission deadline.
You can go through the actual code files, there is vast inline documentation too! If you still can't figure it out, please feel free to reach out to sra9@illinois.edu or aak14@illinois.edu!






