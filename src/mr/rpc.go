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

/* communication between the master and works	*/
type MapJob struct {
	InputFile   string /* file of map task*/
	MapJobIndex int    /* the index of assigned map task*/
	ReduceCount int    /* number of reduce tasks under the mapf*/
}

/* if all the map tasks are completed*/
type ReduceJob struct {
	ReduceJobIndex int /* the index of reduce task*/
	nMap           int /* number of map tasks*/
}

/* report the map task to the master*/
type RequestTaskArgs struct {
}

type TaskState int

const (
	MapState    TaskState = 0
	ReduceState TaskState = 1
	StopState   TaskState = 2
	WaitState   TaskState = 3
)

/* the job response if all jobs are done */
type RequestTaskReply struct {
	State TaskState // defined states, MapState, ReduceState, etc.

	// map
	InputFile   string /* file of map task*/
	MapJobIndex int    /* the index of assigned map task*/
	ReduceCount int    /* number of reduce tasks under the mapf*/

	// reduce
	ReduceJobIndex int /* the index of reduce task*/
	NumberMap      int /* number of map tasks*/

	Done bool
}

type DoneTaskArgs struct {
	TaskType     string
	TaskId       int
	TaskFileName []string
}

type DoneTaskReply struct {
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
