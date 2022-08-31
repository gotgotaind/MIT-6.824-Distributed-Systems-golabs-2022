package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type worker struct {
	id         string
	status     string
	processing string
}

type file_status struct {
	status     string
	start_time int64
}
type Coordinator struct {
	// Your definitions here.
	files_status map[string]file_status
	step         string
	nReduce      int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetWork(args *GetWorkArgs, reply *GetWorkReply) error {

	var chosen_file string
	for file, file_status := range c.files_status {
		if file_status.status == "unstarted" {
			chosen_file = file
			file_status.status = "started"
			file_status.start_time = time.Now().Unix()
			break
		}
	}

	reply.MapOrReduce = "map"
	reply.Filename = chosen_file
	reply.Nreduce = c.nReduce
	return nil
}

//
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
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = c.nReduce
	files_stat := make(map[string]file_status)
	for _, file := range files {
		files_stat[file] = file_status{"unstarted", 0}
	}
	c.files_status = files_stat
	// workers := make([]worker, 0)

	c.server()
	return &c
}
