package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	R              int // number of reduce task
	M              int // number of map task
	files          []string
	mapfinished    int   // number of finished map task
	maptasklog     []int // log for map task, 0: not allocated, 1: waiting, 2:finished
	reducefinished int   // number of finished map task
	reducetasklog  []int // log for reduce task

	// channel for synchronizing
	mapFinishedChan    chan int
	reduceFinishedChan chan int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Schedule(args *ExampleArgs, reply *ExampleReply) error {
	if c.mapfinished != c.M {
		//do map
	} else if c.reducefinished != c.R {
		//do reduce
	} else {
		// all done

	}
	//reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) MapTaskFinished(args *WorkArgs, reply *Workreply) {
	c.mapFinishedChan <- args.taskID
}

func (c *Coordinator) ReduceTaskFinished(args *WorkArgs, reply *Workreply) {
	c.reduceFinishedChan <- args.taskID
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	return c.mapfinished == c.M && c.reducefinished == c.R
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//

func (c *Coordinator) UpdateMapTaskLog() {
	for taskID := range c.mapFinishedChan {
		c.maptasklog[taskID] = 2
		c.mapfinished += 1
	}
}
func (c *Coordinator) UpdateReduceTaskLog() {
	for taskID := range c.reduceFinishedChan {
		c.maptasklog[taskID] = 2
		c.reducefinished += 1
	}
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	go c.UpdateReduceTaskLog()
	go c.UpdateMapTaskLog()

	c.server()
	return &c
}
