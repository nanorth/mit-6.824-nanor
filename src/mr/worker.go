package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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
		args := WorkArgs{}
		reply := Workreply{}
		ok := call("Coordinator.Schedule", args, reply)
		if !ok {
			break
		}
		switch reply.jobType {
		case 0:
			break
		case 1:
			doMap(mapf, reply)
		case 2:
			doReduce()
		}
		time.Sleep(time.Second)

	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func doMap(mapf func(string, string) []KeyValue, reply Workreply) {
	intermediate := []KeyValue{}
	filename := reply.filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	buckets := make([][]KeyValue, reply.R)
	for i := range buckets {
		buckets[i] = []KeyValue{}
	}
	for _, kv := range intermediate {
		idx := ihash(kv.Key)
		buckets[idx] = append(buckets[idx], kv)
	}
	for i := range buckets {
		oname := "mr-" + strconv.Itoa(reply.mapID) + "-" + strconv.Itoa(i)
		ofile, _ := ioutil.TempFile("", oname+"*")
		enc := json.NewEncoder(ofile)
		for _, kv := range buckets[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write into %v", oname)
			}
		}
		os.Rename(ofile.Name(), oname)
		ofile.Close()
	}
	finishedArgs := WorkArgs{reply.mapID, 1}
	finishedReply := ExampleReply{}
	call("Master.MapTaskFinished", &finishedArgs, &finishedReply)
}

func doReduce() {

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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
