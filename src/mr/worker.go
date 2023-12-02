package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
		ok := call("Coordinator.Schedule", &args, &reply)
		if !ok {
			break
		}
		switch reply.JobType {
		case 0:
			return
		case 1:
			//fmt.Println("Get a Map job, working...")
			//fmt.Printf("Working on job num: %v", reply.MapID)
			doMap(mapf, reply)
		case 2:
			//fmt.Println("Get a Reduce job, working...")
			//fmt.Printf("Working on job num: %v", reply.ReduceID)
			doReduce(reducef, reply)
		case 3:
			continue
		}
		time.Sleep(time.Second)

	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func doMap(mapf func(string, string) []KeyValue, reply Workreply) {
	intermediate := []KeyValue{}
	filename := reply.Filename
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
		buckets[idx%reply.R] = append(buckets[idx%reply.R], kv)
	}
	for i := range buckets {
		oname := "mr-" + strconv.Itoa(reply.MapID) + "-" + strconv.Itoa(i)
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
	finishedArgs := WorkArgs{reply.MapID, 1}
	finishedReply := Workreply{}
	call("Coordinator.MapTaskFinished", &finishedArgs, &finishedReply)
}

func doReduce(reducef func(string, []string) string, reply Workreply) {
	intermediate := []KeyValue{}
	for i := 0; i < reply.M; i++ {
		iname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceID)
		file, err := os.Open(iname)
		if err != nil {
			log.Fatalf("cannot open %v", file)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))
	// output file
	oname := "mr-out-" + strconv.Itoa(reply.ReduceID)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-Y.
	//
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()

	// send the finish message to master
	finishedArgs := WorkArgs{reply.ReduceID, 1}
	finishedReply := Workreply{}
	call("Coordinator.ReduceTaskFinished", &finishedArgs, &finishedReply)
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
