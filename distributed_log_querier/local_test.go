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
	//testLocalOperatorRunWordCount(t)
	testLocalOperatorRunSimpleLichess(t)
	//testDistinctNBuffersRead(t)
	//testRateFilterOperator(t)
	//testLichessOperator(t)
}

func testLocalOperatorRunSimpleLichess(t *testing.T) {
	lines := []string{
		"nWhCo0rP,True,1496301655765.0,1496302689313.0,68,mate,white,7+5,anopiloric,1756,wojand,1667,e4 e5 Bc4 Nf6 Nf3 Nc6 a3 d5 exd5 Nxd5 Qe2 Nf4 Qf1 Bc5 d3 Ne6 Qe2 Qf6 O-O O-O b4 Bd4 c3 Bb6 b5 Ne7 Qxe5 Qxe5 Nxe5 Nf5 d4 Re8 a4 Ba5 Bd2 Re7 Bd3 Nd6 c4 Nxd4 Bxa5 Rxe5 Bxc7 Re6 Nc3 Nb3 Rad1 Ne8 Bg3 Nf6 Bc2 Nc5 a5 a6 b6 Bd7 Bh4 Ng4 Nd5 Bc6 Nc7 Rh6 Nxa8 Rxh4 h3 Ne5 Rd8+ Be8,C55,Italian Game,6",
		"XWOeTEaM,True,1496301498952.0,1496301647321.0,26,resign,white,10+0,ivanov32,1761,wojand,1675,e4 e5 Nf3 Nc6 Bc4 Bc5 c3 Nf6 d4 exd4 cxd4 Bb4+ Nc3 Nxe4 O-O Nxc3 bxc3 Bxc3 Ba3 Bxa1 Re1+ Ne7 Bxe7 Qxe7 Rxe7+ Kxe7,C54,Italian Game: Giuoco Piano |  Aitken Variation,19",
		"fZNM7JV0,True,1496152442509.0,1496153082339.0,74,mate,black,10+0,wojand,1682,andychem70,1792,e4 c5 Nf3 d6 d4 cxd4 Nxd4 Nf6 Nc3 a6 Be2 e6 Be3 Be7 Qd2 Nbd7 O-O-O Qc7 f3 O-O g4 Ne5 h4 Nc4 Bxc4 Qxc4 g5 Nd7 f4 b5 f5 e5 Nf3 Bb7 h5 Bxe4 Rdf1 Rfc8 f6 Bf8 fxg7 Bxg7 g6 f5 gxh7+ Kh8 Bh6 Bxh6 Qxh6 b4 Rhg1 bxc3 Rg8+ Rxg8 hxg8=Q+ Kxg8 Rg1+ Kf7 Rg7+ Ke8 Qg6+ Kd8 Qg5+ Kc7 bxc3 Qf1+ Kb2 Rb8+ Ka3 Qc1+ Ka4 Qxc2+ Ka5 Qxa2#,B84,Sicilian Defense: Scheveningen Variation |  Classical Variation,12",
		"MP9Ox4EM,True,1496084376362.0,1496085561151.0,124,outoftime,black,10+0,wojand,1694,hungryg,1636,e4 e6 d4 c6 Nc3 Bb4 Bd2 d6 Nf3 Nf6 Bd3 b5 a3 Ba5 O-O Bb7 e5 dxe5 Bg5 exd4 Ne4 Bb6 Nxf6+ gxf6 Bh4 Nd7 Be4 Qc7 Nxd4 O-O-O Nb3 Nc5 Qf3 Nxe4 Qxe4 c5 Qe2 Rhg8 Bg3 e5 Qxb5 a6 Qc4 Qc6 f3 Rd7 Rad1 Qe6 Qxe6 fxe6 Nd2 Rd4 Ne4 f5 Nf6 Rgd8 Rxd4 Rxd4 Bxe5 Rd2 Nxh7 Rxc2 Rd1 Re2 Bf6 e5 Ng5 e4 fxe4 fxe4 Rd6 Bc7 Re6 e3 Kf1 Rd2 Rxe3 Bxg2+ Kg1 Bf4 Re8+ Kb7 Bc3 Rc2 Re7+ Kb6 Re6+ Kb5 a4+ Kxa4 Re4+ Bxe4 Nxe4 Bxh2+ Kf1 Kb5 Ke1 a5 Kd1 Rg2 Nd6+ Kc6 Nf5 a4 Ne3 Rg3 Ke2 Rg8 Kd2 Rd8+ Kc2 c4 Nxc4 a3 Nxa3 Rc8 Kd3 Kd7 Kd4 Ke8 b4 Kf7 Kd5 Rd8+,C00,French Defense: Normal Variation,3",
		"0IUkH7T9,True,1495965987775.0,1495966774646.0,90,resign,black,4+6,nicola1396,1685,wojand,1684,e4 e5 Nf3 Nc6 d4 exd4 Nxd4 Bc5 Nxc6 dxc6 Qxd8+ Kxd8 Bc4 Nf6 e5 Ne4 Be3 Bxe3 fxe3 Ng5 h4 Be6 Bxe6 Nxe6 Nc3 Re8 O-O-O+ Ke7 Rhf1 Kf8 g4 Nc5 g5 Rxe5 Rde1 b5 Rg1 Ne6 Rg3 Rd8 Rf3 Nd4 Rff1 Nf5 h5 Rxe3 Rxe3 Nxe3 Rf3 Nc4 g6 hxg6 hxg6 Ne5 Rh3 Nxg6 Ne4 f6 Nc5 Kf7 Re3 Nf4 Re4 Nd5 Ne6 Re8 c4 Rxe6 Rxe6 Kxe6 cxd5+ cxd5 Kd2 g5 Ke3 f5 Kf3 Ke5 b3 g4+ Kg3 d4 Kf2 d3 Ke3 g3 Kxd3 g2 Ke2 g1=Q,C45,Scotch Game: Classical Variation,8",
		"UWHwHtWT,True,1495965869850.0,1495965914436.0,9,resign,white,5+8,rassem,1668,wojand,1695,e4 e5 Bc4 Nf6 d4 d6 dxe5 Nxe4 Qd5,C24,Bishop's Opening: Ponziani Gambit,5",
		"oTKuskKk,True,1495957736271.0,1495958350657.0,64,resign,black,4+6,regalsin,1595,wojand,1687,d4 d5 Nf3 e6 Nc3 c5 e4 Nf6 e5 Nfd7 Be3 Nc6 Bb5 a6 Bxc6 bxc6 dxc5 Nxc5 Bxc5 Bxc5 O-O O-O Nd4 Qc7 a3 Qxe5 Nxc6 Qc7 Nd4 Bd7 b4 Bd6 Nce2 Bxh2+ Kh1 Be5 f4 Bf6 g4 e5 Nf5 Bxf5 gxf5 exf4 Rb1 Qe5 Nxf4 Qxf5 Qxd5 Qxc2 Nh5 Bh4 Qg2 Qxg2+ Kxg2 Rab8 Rf4 Bg5 Rg4 h6 Nf4 f5 Rg3 Bxf4,D02,Queen's Pawn Game: Zukertort Variation,3",
		"ZXXVCrPu,False,1504454450657.0,1504454454397.0,1,outoftime,white,15+30,critico-82,2403,lesauteurdeclasse,1746,e4,B00,King's Pawn,1",
		"rj36zvil,False,1504448999828.0,1504452375791.0,5,outoftime,white,30+30,rajuppi,2454,lesauteurdeclasse,1746,e4 e6 d4 Bb4+ c3,C00,French Defense: Normal Variation,3",
	}
	for _, line := range lines {
		output := distributed_log_querier.RunOperator("lichess_op_1_mac", line)
		//deserialize the output
		var result []string
		err := json.Unmarshal([]byte(output), &result)
		if err != nil {
			fmt.Println("Error converting to JSON: ", err)
		}
		if len(result) > 0 {
			output = distributed_log_querier.RunOperator("lichess_op_2_mac", line)
			//deserialize the output
			var result []string
			err := json.Unmarshal([]byte(output), &result)
			if err != nil {
				fmt.Println("Error converting to JSON: ", err)
			}
			fmt.Println(result)
		}
	}
}

func testLichessOperator(t *testing.T) {
	lines := []string{
		"nWhCo0rP,True,1496301655765.0,1496302689313.0,68,resign,white,7+5,anopiloric,1756,wojand,1667,e4 e5 Bc4 Nf6 Nf3 Nc6 a3 d5 exd5 Nxd5 Qe2 Nf4 Qf1 Bc5 d3 Ne6 Qe2 Qf6 O-O O-O b4 Bd4 c3 Bb6 b5 Ne7 Qxe5 Qxe5 Nxe5 Nf5 d4 Re8 a4 Ba5 Bd2 Re7 Bd3 Nd6 c4 Nxd4 Bxa5 Rxe5 Bxc7 Re6 Nc3 Nb3 Rad1 Ne8 Bg3 Nf6 Bc2 Nc5 a5 a6 b6 Bd7 Bh4 Ng4 Nd5 Bc6 Nc7 Rh6 Nxa8 Rxh4 h3 Ne5 Rd8+ Be8,C55,Italian Game,6",
		"XWOeTEaM,True,1496301498952.0,1496301647321.0,26,resign,white,10+0,ivanov32,1761,wojand,1675,e4 e5 Nf3 Nc6 Bc4 Bc5 c3 Nf6 d4 exd4 cxd4 Bb4+ Nc3 Nxe4 O-O Nxc3 bxc3 Bxc3 Ba3 Bxa1 Re1+ Ne7 Bxe7 Qxe7 Rxe7+ Kxe7,C54,Italian Game: Giuoco Piano |  Aitken Variation,19",
		"fZNM7JV0,True,1496152442509.0,1496153082339.0,74,mate,black,10+0,wojand,1682,andychem70,1792,e4 c5 Nf3 d6 d4 cxd4 Nxd4 Nf6 Nc3 a6 Be2 e6 Be3 Be7 Qd2 Nbd7 O-O-O Qc7 f3 O-O g4 Ne5 h4 Nc4 Bxc4 Qxc4 g5 Nd7 f4 b5 f5 e5 Nf3 Bb7 h5 Bxe4 Rdf1 Rfc8 f6 Bf8 fxg7 Bxg7 g6 f5 gxh7+ Kh8 Bh6 Bxh6 Qxh6 b4 Rhg1 bxc3 Rg8+ Rxg8 hxg8=Q+ Kxg8 Rg1+ Kf7 Rg7+ Ke8 Qg6+ Kd8 Qg5+ Kc7 bxc3 Qf1+ Kb2 Rb8+ Ka3 Qc1+ Ka4 Qxc2+ Ka5 Qxa2#,B84,Sicilian Defense: Scheveningen Variation |  Classical Variation,12",
		"MP9Ox4EM,True,1496084376362.0,1496085561151.0,124,outoftime,black,10+0,wojand,1694,hungryg,1636,e4 e6 d4 c6 Nc3 Bb4 Bd2 d6 Nf3 Nf6 Bd3 b5 a3 Ba5 O-O Bb7 e5 dxe5 Bg5 exd4 Ne4 Bb6 Nxf6+ gxf6 Bh4 Nd7 Be4 Qc7 Nxd4 O-O-O Nb3 Nc5 Qf3 Nxe4 Qxe4 c5 Qe2 Rhg8 Bg3 e5 Qxb5 a6 Qc4 Qc6 f3 Rd7 Rad1 Qe6 Qxe6 fxe6 Nd2 Rd4 Ne4 f5 Nf6 Rgd8 Rxd4 Rxd4 Bxe5 Rd2 Nxh7 Rxc2 Rd1 Re2 Bf6 e5 Ng5 e4 fxe4 fxe4 Rd6 Bc7 Re6 e3 Kf1 Rd2 Rxe3 Bxg2+ Kg1 Bf4 Re8+ Kb7 Bc3 Rc2 Re7+ Kb6 Re6+ Kb5 a4+ Kxa4 Re4+ Bxe4 Nxe4 Bxh2+ Kf1 Kb5 Ke1 a5 Kd1 Rg2 Nd6+ Kc6 Nf5 a4 Ne3 Rg3 Ke2 Rg8 Kd2 Rd8+ Kc2 c4 Nxc4 a3 Nxa3 Rc8 Kd3 Kd7 Kd4 Ke8 b4 Kf7 Kd5 Rd8+,C00,French Defense: Normal Variation,3",
		"0IUkH7T9,True,1495965987775.0,1495966774646.0,90,resign,black,4+6,nicola1396,1685,wojand,1684,e4 e5 Nf3 Nc6 d4 exd4 Nxd4 Bc5 Nxc6 dxc6 Qxd8+ Kxd8 Bc4 Nf6 e5 Ne4 Be3 Bxe3 fxe3 Ng5 h4 Be6 Bxe6 Nxe6 Nc3 Re8 O-O-O+ Ke7 Rhf1 Kf8 g4 Nc5 g5 Rxe5 Rde1 b5 Rg1 Ne6 Rg3 Rd8 Rf3 Nd4 Rff1 Nf5 h5 Rxe3 Rxe3 Nxe3 Rf3 Nc4 g6 hxg6 hxg6 Ne5 Rh3 Nxg6 Ne4 f6 Nc5 Kf7 Re3 Nf4 Re4 Nd5 Ne6 Re8 c4 Rxe6 Rxe6 Kxe6 cxd5+ cxd5 Kd2 g5 Ke3 f5 Kf3 Ke5 b3 g4+ Kg3 d4 Kf2 d3 Ke3 g3 Kxd3 g2 Ke2 g1=Q,C45,Scotch Game: Classical Variation,8",
		"UWHwHtWT,True,1495965869850.0,1495965914436.0,9,resign,white,5+8,rassem,1668,wojand,1695,e4 e5 Bc4 Nf6 d4 d6 dxe5 Nxe4 Qd5,C24,Bishop's Opening: Ponziani Gambit,5",
		"oTKuskKk,True,1495957736271.0,1495958350657.0,64,resign,black,4+6,regalsin,1595,wojand,1687,d4 d5 Nf3 e6 Nc3 c5 e4 Nf6 e5 Nfd7 Be3 Nc6 Bb5 a6 Bxc6 bxc6 dxc5 Nxc5 Bxc5 Bxc5 O-O O-O Nd4 Qc7 a3 Qxe5 Nxc6 Qc7 Nd4 Bd7 b4 Bd6 Nce2 Bxh2+ Kh1 Be5 f4 Bf6 g4 e5 Nf5 Bxf5 gxf5 exf4 Rb1 Qe5 Nxf4 Qxf5 Qxd5 Qxc2 Nh5 Bh4 Qg2 Qxg2+ Kxg2 Rab8 Rf4 Bg5 Rg4 h6 Nf4 f5 Rg3 Bxf4,D02,Queen's Pawn Game: Zukertort Variation,3",
		"ZXXVCrPu,False,1504454450657.0,1504454454397.0,1,outoftime,white,15+30,critico-82,2403,lesauteurdeclasse,1746,e4,B00,King's Pawn,1",
		"rj36zvil,False,1504448999828.0,1504452375791.0,5,outoftime,white,30+30,rajuppi,2454,lesauteurdeclasse,1746,e4 e6 d4 Bb4+ c3,C00,French Defense: Normal Variation,3",
	}
	for _, line := range lines {
		output := distributed_log_querier.RunOperator("lichess_operator_1", line)
		//deserialize the output
		fmt.Println("Output", output)
		var result []string
		err := json.Unmarshal([]byte(output), &result)
		if err != nil {
			fmt.Println("Error converting to JSON: ", err)
		}
		fmt.Println(result)
	}
}

func testRateFilterOperator(t *testing.T) {
	inputs := []string {"-9822327.40910577,4882292.41177499,1,624,14,0.25,6,63,CP75,CAMPUS $.75/HR,,1,Yes,500,500 S Third St,3,7:00 AM - 21:00 PM,Monday - Saturday,2 hr max in 3 hr period,,No Charge 9PM - 7AM,,","-9822317.04959963,4882696.08124157,2,617,14,0.75,6,63,CP75,CAMPUS $.75/HR,,2,Yes,200,200 S Third St,2,7:00 AM - 21:00 PM,Monday - Saturday,10 hr max,,No Charge 9PM - 7AM,,",
	"-9822327.37494088,4882272.83336435,3,626,14,0.75,6,63,CP75,CAMPUS $.25/HR,,3,Yes,500,500 S Third St,3,7:00 AM - 21:00 PM,Monday - Saturday,2 hr max in 3 hr period,,No Charge 9PM - 7AM,,"}

	for _, input := range inputs {
		output := distributed_log_querier.RunOperatorlocal("rate_filter_operator", input,0)
		//deserialize the output
		var result []string
		err := json.Unmarshal([]byte(output), &result)
		if err != nil {
			fmt.Println("Error converting to JSON: ", err)
		}
		fmt.Println(result)
	}
	
}


func testDistinctNBuffersRead(t *testing.T) {
	output, error := distributed_log_querier.GetLastNDistinctTaskBuffers("output.txt", 3)
	if error != nil {
		fmt.Println("Error reading file: ", error)
	}
	fmt.Println(output)
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
	output := distributed_log_querier.RunOperatorlocal("split_operator", ip_string,0)
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
	output := distributed_log_querier.RunOperator("count_op", ip_string)
	fmt.Println(output)
	var countMap map[string]int
	err := json.Unmarshal([]byte(output), &countMap)
	if err != nil {
		fmt.Println("Error converting to JSON: ", err)
	}
	if !(countMap["hello"] == 1 ){
		fmt.Println("Test Failed")
	}
	ip_string = "hello"
	output = distributed_log_querier.RunOperator("count_op", ip_string)
	err = json.Unmarshal([]byte(output), &countMap)
	if err != nil {
		fmt.Println("Error converting to JSON: ", err)
	}
	if !(countMap["hello"] == 2 ){
		//fmt.Println("Test Failed")
	}
	ip_string = "world"
	output = distributed_log_querier.RunOperator("count_op", ip_string)
	fmt.Println(output)
	err = json.Unmarshal([]byte(output), &countMap)
	if err != nil {
		fmt.Println("Error converting to JSON: ", err)
	}
	if !(countMap["world"] == 1 && countMap["hello"] == 2){
		fmt.Println("Test Failed")
	}

	//loop for 1000 times with different words
	// for i := 0; i < 1000; i++ {
	// 	ip_string = "word" + strconv.Itoa(i)
	// 	output = distributed_log_querier.RunOperator("count_op_mac", ip_string)
		
	// 	err = json.Unmarshal([]byte(output), &countMap)
		
	// 	if err != nil {
	// 		fmt.Println("Error converting to JSON: ", err)
	// 	}
	// 	if !(countMap["world"] == 1 && countMap["hello"] == 2 && countMap["word" + strconv.Itoa(i)] == 1){
	// 		fmt.Println("Test Failed")
	// 	}
	// }


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