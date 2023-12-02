package mr

import "C"
import (
	"fmt"
	"log"
)
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
	mapTaskLog     []int // log for map task, 0: not allocated, 1: waiting, 2:finished
	reducefinished int   // number of finished map task
	reduceTaskLog  []int // log for reduce task

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

func (c *Coordinator) Schedule(args *WorkArgs, reply *Workreply) error {
	if c.mapfinished != c.M {

		//do map
		allocate := -1
		for i := 0; i < c.M; i++ {
			if c.mapTaskLog[i] == 0 {
				allocate = i
				break
			}
		}
		if allocate == -1 {
			// waiting for unfinished map jobs
			reply.JobType = 3
		} else {
			// allocate map jobs

			reply.JobType = 1
			reply.Filename = c.files[allocate]
			reply.MapID = allocate
			reply.ReduceID = -1
			reply.M = c.M
			reply.R = c.R
			c.mapTaskLog[allocate] = 1 // waiting
			//fmt.Println(reply)
		}
	} else if c.reducefinished != c.R {
		//do reduce
		allocate := -1
		for i := 0; i < c.R; i++ {
			if c.reduceTaskLog[i] == 0 {
				allocate = i
				break
			}
		}
		if allocate == -1 {
			// waiting for unfinished reduce jobs
			reply.JobType = 3
		} else {
			// allocate reduce jobs
			reply.ReduceID = allocate
			reply.JobType = 2
			reply.M = c.M
			reply.R = c.R
			c.reduceTaskLog[allocate] = 1 // waiting
		}
	} else {
		// all done
		//fmt.Println("i never been here")
		reply.JobType = 0
	}
	//fmt.Println(reply)
	return nil
}

func (c *Coordinator) MapTaskFinished(args *WorkArgs, reply *Workreply) error {
	c.mapFinishedChan <- args.taskID
	return nil
}

func (c *Coordinator) ReduceTaskFinished(args *WorkArgs, reply *Workreply) error {
	c.reduceFinishedChan <- args.taskID
	return nil
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
		c.mapTaskLog[taskID] = 2
		c.mapfinished += 1
	}
}
func (c *Coordinator) UpdateReduceTaskLog() {
	for taskID := range c.reduceFinishedChan {
		c.mapTaskLog[taskID] = 2
		c.reducefinished += 1
	}
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		M:                  len(files),
		R:                  nReduce,
		files:              files,
		mapfinished:        0,
		mapTaskLog:         make([]int, len(files)),
		reducefinished:     0,
		reduceTaskLog:      make([]int, nReduce),
		mapFinishedChan:    make(chan int, len(files)),
		reduceFinishedChan: make(chan int, nReduce),
	}
	fmt.Println(c)
	// Your code here.
	fmt.Println("Coordinator is running")
	go c.UpdateReduceTaskLog()
	go c.UpdateMapTaskLog()

	c.server()
	return &c
}
