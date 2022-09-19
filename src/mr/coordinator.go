package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var f *os.File
var err error

type worker struct {
	id         string
	status     string
	processing string
}

type file_status struct {
	status     string
	id         int
	start_time int64
}

type reduce_status struct {
	status     string
	start_time int64
}
type Coordinator struct {
	// Your definitions here.
	files_status   map[string]file_status
	reduces_status map[int]reduce_status
	step           string
	nReduce        int
	Nmap           int
	mu             sync.Mutex
	max_task_time  int64
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetWork(args *GetWorkArgs, reply *GetWorkReply) error {

	c.mu.Lock()
	reply.MapOrReduce = "wait"
	if c.step == "map" {
		for file, file_status := range c.files_status {
			if file_status.status == "unstarted" {

				file_status.status = "started"
				file_status.start_time = time.Now().Unix()
				c.files_status[file] = file_status

				reply.MapOrReduce = "map"
				reply.Filename = file
				reply.Nreduce = c.nReduce
				reply.MapTaskId = file_status.id

				break
			}
		}
	} else {
		for id, reduce_status := range c.reduces_status {
			if reduce_status.status == "unstarted" {
				reduce_status.status = "started"
				reduce_status.start_time = time.Now().Unix()
				c.reduces_status[id] = reduce_status

				reply.MapOrReduce = "reduce"
				reply.Nreduce = c.nReduce
				reply.ReduceId = id
				reply.Nmap = c.Nmap

				break
			}
		}
	}
	// if no map or reduce  left to allocate, probably should manage
	// some kind of wait task
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) NotifyMapEnd(args *NotifyMapEndArgs, reply *NotifyMapEndReply) error {
	c.mu.Lock()
	file_status := c.files_status[args.Filename]
	file_status.status = "finished"
	c.files_status[args.Filename] = file_status
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) NotifyReduceEnd(args *NotifyReduceEndArgs, reply *NotifyReduceEndReply) error {
	c.mu.Lock()
	reduce_status := c.reduces_status[args.ReduceId]
	reduce_status.status = "finished"
	c.reduces_status[args.ReduceId] = reduce_status
	c.mu.Unlock()
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
	c.mu.Lock()
	map_finished := 0
	// Your code here.
	for file, file_status := range c.files_status {
		if file_status.status == "finished" {
			map_finished++
		} else {
			if (time.Now().Unix() - file_status.start_time) > c.max_task_time {
				new_file_status := c.files_status[file]
				new_file_status.status = "unstarted"
				c.files_status[file] = new_file_status

			}
		}
	}
	// log.Printf("Map tasks %v/%v\n", map_finished, len(c.files_status))
	if map_finished == len(c.files_status) && c.step == "map" {
		c.step = "reduce"
	}

	reduce_finished := 0
	for id, reduce_status := range c.reduces_status {
		if reduce_status.status == "finished" {
			reduce_finished++
		} else {
			if (time.Now().Unix() - reduce_status.start_time) > c.max_task_time {
				new_reduce_status := c.reduces_status[id]
				new_reduce_status.status = "unstarted"
				c.reduces_status[id] = new_reduce_status

			}
		}
	}
	// log.Printf("Reduce tasks %v/%v\n", reduce_finished, len(c.reduces_status))
	if reduce_finished == len(c.reduces_status) {
		c.step = "done"
		ret = true
		log.Println("All done!")
	}
	c.mu.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	/*f, err = os.OpenFile("testlogfile", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	log.SetOutput(f)
	*/

	c := Coordinator{}

	// Your code here.
	c.step = "map"
	c.max_task_time = 10
	c.nReduce = nReduce
	files_stat := make(map[string]file_status)
	Nmap := 0
	for i, file := range files {
		files_stat[file] = file_status{"unstarted", i, 0}
		Nmap++
	}
	c.Nmap = Nmap
	c.files_status = files_stat

	reduces_status := make(map[int]reduce_status)
	for id := 0; id < nReduce; id++ {
		reduces_status[id] = reduce_status{"unstarted", 0}
		// log.Printf("reduce id %v", id)
	}
	// log.Printf("size of reduces_status : %v", len(reduces_status))
	c.reduces_status = reduces_status
	// workers := make([]worker, 0)

	c.server()
	return &c
}
