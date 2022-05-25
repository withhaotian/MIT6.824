package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Flag struct {
	processing bool
	completed  bool
	StartTime  time.Time
}

type Coordinator struct {
	// Your definitions here.
	FileNames     []string   // input filename
	MapFlags      []Flag     // map function state
	ReduceFlags   []Flag     // reduce function state
	MapTaskCnt    int        // map task arr
	ReduceTaskCnt int        // reduce task arr
	MapAllDone    bool       // confirm all the map tasks are done
	ReduceAllDone bool       // confirm all the reduce tasks are done
	Mu            sync.Mutex // synchronisation
}

// Your code here -- RPC handlers for the worker to call.

// RPC function for workers to request the task
func (c *Coordinator) GetTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	/* all tasks are done*/
	if c.MapAllDone && c.ReduceAllDone {
		reply.Done = true
		return nil
	}

	/* if map task*/
	if !c.MapAllDone {
		uncomplete := false
		for i := 0; i < c.MapTaskCnt; i++ {
			if c.MapFlags[i].completed == true {
				continue
			}

			uncomplete = true

			isTimeout := time.Now().After(c.MapFlags[i].StartTime.Add(time.Duration(5) * time.Second))

			/* return the task info*/
			if c.MapFlags[i].processing != true || isTimeout {
				// update the task
				c.MapFlags[i].processing = true
				c.MapFlags[i].StartTime = time.Now()

				// reply
				reply.State = MapState
				reply.InputFile = c.FileNames[i]
				reply.MapJobIndex = i
				reply.ReduceCount = c.ReduceTaskCnt
				return nil
			}
		}

		if uncomplete {
			reply.State = WaitState // Tell worker to wait
			// ( i.e., if one worker done the map early, it has to wait for all map tasks done)
			return nil
		}

		c.MapAllDone = true
	}

	/* if reduce task */
	uncomplete := false
	for i := 0; i < c.ReduceTaskCnt; i++ {
		if c.ReduceFlags[i].completed == true {
			continue
		}

		uncomplete = true

		isTimeout := time.Now().After(c.ReduceFlags[i].StartTime.Add(time.Duration(5) * time.Second))

		// return the task info
		if c.ReduceFlags[i].processing != true || isTimeout {
			// update the reduce task
			c.ReduceFlags[i].processing = true
			c.ReduceFlags[i].StartTime = time.Now()

			// reply
			reply.State = ReduceState
			reply.NumberMap = c.MapTaskCnt
			reply.ReduceJobIndex = i
			return nil
		}
	}

	if uncomplete {
		reply.State = WaitState // tell the worker to wait
		return nil
	}

	c.ReduceAllDone = true
	reply.State = StopState
	reply.Done = true

	return nil
}

// Complete the task
func (c *Coordinator) TaskDone(args *DoneTaskArgs, reply *DoneTaskReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	switch args.TaskType {
	case "map":
		c.MapFlags[args.TaskId].processing = false
		c.MapFlags[args.TaskId].completed = true

		//// rename
		//for i := 0; i < c.ReduceTaskCnt; i++ {
		//	name := fmt.Sprintf("mr-%v-%v", args.TaskId, i)
		//	os.Rename(args.TaskFileName[i], name)
		//}

	case "reduce":
		c.ReduceFlags[args.TaskId].processing = false
		c.ReduceFlags[args.TaskId].completed = true

		//// rename
		//name := fmt.Sprintf("mr-out-%v", args.TaskId)
		//os.Rename(args.TaskFileName[0], name)
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	c.Mu.Lock()
	defer c.Mu.Unlock()

	ret := false

	// Your code here.
	ret = c.MapAllDone && c.ReduceAllDone

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.

// m := mr.MakeCoordinator(os.Args[1:], 10)

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// init the master
	// each file take as one task
	c := Coordinator{}

	c.FileNames = files
	c.MapFlags = make([]Flag, len(files))
	for i := 0; i < len(files); i++ {
		c.MapFlags[i] = Flag{
			processing: false,
			completed:  false,
			StartTime:  time.Now(),
		}
	}
	c.ReduceFlags = make([]Flag, nReduce)
	for i := 0; i < len(files); i++ {
		c.ReduceFlags[i] = Flag{
			processing: false,
			completed:  false,
			StartTime:  time.Now(),
		}
	}
	c.MapTaskCnt = len(files)
	log.Println("number of map tasks", c.MapTaskCnt)
	c.ReduceTaskCnt = nReduce
	c.MapAllDone = false
	c.ReduceAllDone = false

	// Your code here.

	c.server()

	return &c
}
