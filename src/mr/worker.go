package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
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
	Key   string `json:"key"`
	Value string `json:"value"`
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
	reply, ok := NReduce()
	if !ok {
		log.Fatalln("call NReduce error")
		return
	}
	nReduce := reply.NReduce
	for  {
		reply, ok := RequestTask()
		if !ok {
			return
		}
		task := reply.Task
		if "map" == task.TaskType {
			filename := task.Name
			fmt.Printf("map phase filename:%v\n", filename)
			// map
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
			kvReduceMap := make(map[int]ByKey)
			for _, kv := range kva {
				reduceTaskNum := ihash(kv.Key) % nReduce
				kvReduceMap[reduceTaskNum] = append(kvReduceMap[reduceTaskNum], kv)
			}
			for k, v := range kvReduceMap {
				ofile := "mr-" + strconv.Itoa(task.Id) + "-" + strconv.Itoa(k)
				//ioutil.TempFile(ofile, "*")
				_, err := os.Stat(ofile)
				if err != nil {
					if os.IsNotExist(err) {
						_, err := os.Create(ofile)
						if err != nil {
							log.Fatalf("cannot create %v\n", ofile)
						}
					}
				}
				file, err := os.OpenFile(ofile, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
				if err != nil {
					log.Fatalf("cannot open %v\n", ofile)
				}
				enc := json.NewEncoder(file)
				err = enc.Encode(v)
				if err != nil {
					log.Fatalf("cannot write %v\n", ofile)
				}
				file.Close()
			}
			task.State = "completed"
			ReportTask(task)
		} else if "reduce" == task.TaskType {
			// reduce
			fmt.Printf("reduce phase task:%v\n", task.Name)
			var reduceFiles []string
			pattern := "mr-(.)-" + task.Name
			reg, err := regexp.Compile(pattern)
			if err != nil {
				fmt.Println(err)
			}
			rd, err := ioutil.ReadDir(".")
			if err != nil {
				fmt.Println("read dir fail:", err)
			}
			for _, fi := range rd {
				if fi.IsDir() {
					continue
				} else {
					matched := reg.MatchString(fi.Name())
					if matched {
						reduceFiles = append(reduceFiles, fi.Name())
					}
				}
			}
			fmt.Printf("reduceFiles:%v\n", reduceFiles)
			var intermediate ByKey
			// Todo:并发
			for _, reduceFile := range reduceFiles {
				file, err := os.Open(reduceFile)
				if err != nil {
					//log.Fatalf("cannot open %v\n", file)
					fmt.Printf("cannot open %v\n", file)
				}
				dec := json.NewDecoder(file)

				var kva []KeyValue
				if err := dec.Decode(&kva); err != nil {
					fmt.Printf("decode file %v\n", err)
					break
				}
				intermediate = append(intermediate, kva...)
			}

			sort.Sort(intermediate)
			oname := "mr-out-" + task.Name
			ofile, _ := os.Create(oname)
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
			// 删除临时文件
			for _, reduceFile := range reduceFiles {
				err = os.Remove(reduceFile)
				if err != nil {
					panic(err)
				}
			}
			task.State = "completed"
			ReportTask(task)
		} else {
			time.Sleep(1 * time.Second)
		}
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()
}

func RequestTask() (DistributeTaskReply, bool) {
	args := DistributeTaskArgs{}
	reply := DistributeTaskReply{}
	ok := call("Master.DistributeTask", args, &reply)
	if !ok  {
		return reply, false
	}
	return reply, true
}

func ReportTask(task Task) (ReportTaskReply, bool) {
	args := ReportTaskArgs{}
	args.Task = task
	reply := ReportTaskReply{}
	ok := call("Master.ReportTask", args, &reply)
	if !ok {
		return reply, false
	}
	return reply, true
}

func NReduce() (NReduceReply, bool) {
	args := NReduceArgs{}
	reply := NReduceReply{}
	ok := call("Master.NReduce", args, &reply)
	if !ok {
		return reply, false
	}
	return reply, true
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
