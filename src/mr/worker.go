package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	for {
		fmt.Printf("one loop!\n")
		args := GetWorkArgs{}
		reply := GetWorkReply{}
		ok := call("Coordinator.GetWork", &args, &reply)

		Filename := reply.Filename
		MapTaskId := reply.MapTaskId
		Nreduce := reply.Nreduce
		ReduceId := reply.ReduceId
		Nmap := reply.Nmap

		if ok {
			// reply.Y should be 100.
			fmt.Printf("reply %v\n", reply)
			if reply.MapOrReduce == "map" {
				fmt.Printf("I is a map")
				do_map(Filename, MapTaskId, Nreduce, mapf)
				args := NotifyMapEndArgs{Filename: Filename}
				reply := NotifyMapEndReply{}
				ok := call("Coordinator.NotifyMapEnd", &args, &reply)
				if !ok {
					fmt.Printf("Call to NotifyMapEnd failed!\n")
				}
			} else if reply.MapOrReduce == "reduce" {
				fmt.Printf("It is a reduce %v", reply.MapOrReduce)
				do_reduce(ReduceId, Nmap, Nreduce, reducef)
				args := NotifyReduceEndArgs{ReduceId: ReduceId}
				reply := NotifyReduceEndReply{}
				ok := call("Coordinator.NotifyReduceEnd", &args, &reply)
				if !ok {
					fmt.Printf("Call to NotifyReduceEnd failed!\n")
				}
			}
		} else {
			fmt.Printf("call to GetWork failed!\n")
		}

		time.Sleep(8 * time.Second)
	}
	// CallExample()

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

//
// performs map function
//
func do_map(filename string, file_id int, nReduce int, mapf func(string, string) []KeyValue) bool {

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	} else {
		log.Printf("opened %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	reduce_files_json_encoder := make([]*json.Encoder, nReduce)
	reduce_files_handlers := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		o_file_name := "mr-map-intermediate-" + strconv.Itoa(file_id) + "-" + strconv.Itoa(i) + ".txt"
		file, err := os.OpenFile(o_file_name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			log.Fatalf("cannot open %v", o_file_name)
		}
		enc := json.NewEncoder(file)
		reduce_files_json_encoder[i] = enc
		reduce_files_handlers[i] = file
	}

	for _, kv := range kva {
		iReduce := ihash(kv.Key) % nReduce
		if err := reduce_files_json_encoder[iReduce].Encode(&kv); err != nil {
			log.Fatalf("cannot encode %v", kv)
		}
	}

	for i := 0; i < nReduce; i++ {
		if err := reduce_files_handlers[i].Close(); err != nil {
			log.Fatalf("cannot close %v", reduce_files_handlers[i])
		}
	}

	return true
}

func do_reduce(ReduceId int, Nmap int, Nreduce int, reducef func(string, []string) string) {

	kva := make([]KeyValue, 0)
	for i := 0; i < Nmap; i++ {
		i_file_name := "mr-map-intermediate-" + strconv.Itoa(i) + "-" + strconv.Itoa(ReduceId) + ".txt"
		file, err := os.Open(i_file_name)
		if err != nil {
			log.Fatalf("cannot open %v", i_file_name)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal("Error decoding file ", i_file_name, err)
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))

	oname := "mr-out-" + strconv.Itoa(ReduceId)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
}
