package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type StatusCode int

type Workreply struct {
	JobType  int // 0: done, 1: map, 2: reduce 3: wait
	MapID    int
	ReduceID int
	M        int //M map tasks
	R        int //R reduce tasks
	Filename string
}

type WorkArgs struct {
	taskID   int
	TaskDone int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
