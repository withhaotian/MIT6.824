package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}
		log.Println("Calling Coordinator.GetTask...")
		call("Coordinator.GetTask", &args, &reply)

		if reply.Done {
			break
		}

		switch reply.State {
		case MapState:
			doMap(reply, mapf)
		case ReduceState:
			doReduce(reply, reducef)
		case WaitState:
			time.Sleep(time.Second)
		case StopState:
			break
		}
	}
}

func doMap(reply RequestTaskReply, mapf func(string, string) []KeyValue) {
	file, err := os.Open(reply.InputFile)

	if err != nil {
		log.Fatalf("cannot open %v", err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", err)
	}

	file.Close()

	kva := mapf(reply.InputFile, string(content))

	temp := make([]*os.File, reply.ReduceCount)
	tempName := make([]string, reply.ReduceCount)

	// create file of all reduce tasks
	for i := 0; i < reply.ReduceCount; i++ {
		// not nameed as the standard, because of the early exit for access
		tempInterFileName := fmt.Sprintf("mr-%d-%d", reply.MapJobIndex, i)
		tempInterFile, err := os.Create(tempInterFileName)
		if err != nil {
			log.Fatalf("Cannot create temp inter file: %v\n", tempInterFileName)
		}
		temp[i] = tempInterFile
		tempName[i] = tempInterFile.Name()
	}

	// close the file
	defer func() {
		for i := 0; i < len(temp); i++ {
			err = temp[i].Close()
			if err != nil {
				log.Fatalf("Cannot close file: %v\n", temp[i].Name())
			}
		}
	}()

	// save intermidiate results
	for _, kv := range kva {
		reduceIndex := ihash(kv.Key) % reply.ReduceCount
		encodeFile := temp[reduceIndex]

		// encode with json
		encoder := json.NewEncoder(encodeFile)
		err = encoder.Encode(kv)

		if err != nil {
			log.Fatalf("Cannot encode kv: %v\n, %v\n", kv, err)
		}
	}

	done_args := DoneTaskArgs{"map", reply.MapJobIndex, tempName}
	done_reply := DoneTaskReply{}

	log.Println("Calling Task Done...")
	call("Coordinator.TaskDone", &done_args, &done_reply)

	return
}

func doReduce(reply RequestTaskReply, reducef func(string, []string) string) {

	intermediate := []KeyValue{}

	// Read from intermediate files
	for i := 0; i < reply.NumberMap; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", i, reply.ReduceJobIndex)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("Cannot open file: %v\n", err)
		}

		// decoder
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := decoder.Decode(&kv)
			if err != nil {
				break
			} else {
				intermediate = append(intermediate, kv)
			}
		}
	}

	// Sort intermediate KVs
	sort.Sort(ByKey(intermediate))

	// Write to output file
	tempOutputFileName := fmt.Sprintf("mr-out-%d", reply.ReduceJobIndex)
	tempOutputFile, err := os.Create(tempOutputFileName)
	defer tempOutputFile.Close()

	if err != nil {
		log.Fatalf("Can not create file, %v", err)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempOutputFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	done_args := DoneTaskArgs{"reduce", reply.ReduceJobIndex, []string{tempOutputFile.Name()}}
	done_reply := DoneTaskReply{}

	log.Println("Calling Task Done...")
	call("Coordinator.TaskDone", &done_args, &done_reply)

	return
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
