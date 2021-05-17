package mr

import (
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	// Your definitions here.
	mapTask []*Task
	reduceTask []*Task
	mu sync.Mutex
	mapTaskNum int
	reduceTaskNum int
}

type Task struct {
	Id int
	Name string
	TaskType string
	State string // idle/in-process/completed
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	if !m.mapPhaseDone() {
		m.mu.Lock()
		defer m.mu.Unlock()
		for _, mapTask := range m.mapTask {
			if "idle" == mapTask.State {
				mapTask.State = "in-process"
				reply.Task = *mapTask
				go m.traceTask(mapTask)
				return nil
			}
		}
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, reduceTask := range m.reduceTask {
		if "idle" == reduceTask.State {
			reduceTask.State = "in-process"
			reply.Task = *reduceTask
			go m.traceTask(reduceTask)
			return nil
		}
	}
	return nil
}

func (m *Master)ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	taskId := args.TaskId
	taskType := args.TaskType
	m.mu.Lock()
	defer m.mu.Unlock()
	if "map" == taskType {
		m.mapTask[taskId].State = "completed"
	} else {
		m.reduceTask[taskId].State = "completed"
	}
	return nil
}

func (m *Master) NReduce(args *NReduceArgs, reply *NReduceReply) error {
	reply.NReduce = m.reduceTaskNum
	return nil
}

func (m *Master) mapPhaseDone() bool {
	done := true
	m.mu.Lock()
	for _, mapTask := range m.mapTask {
		if "completed" != mapTask.State {
			done = false
		}
	}
	m.mu.Unlock()
	return done
}

func (m *Master) traceTask(task *Task) {
	bT := time.Now()
	for  {
		time.Sleep(1 * time.Second)
		eT := time.Since(bT)
		m.mu.Lock()
		state := task.State
		log.Printf(" %v task %v, state:%v, exec time:%v\n", task.TaskType, task.Id, state, eT.Seconds())
		if "completed" == state {
			log.Printf("%v task %v completed\n", task.TaskType, task.Id)
			m.mu.Unlock()
			return
		} else if eT > 10 * time.Second {
			task.State = "idle"
			m.mu.Unlock()
			return
		}
		m.mu.Unlock()
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := true

	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, task := range m.reduceTask {
		if task.State == "completed" {
			continue
		} else {
			return false
		}
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	for index, file := range files{
		task := Task{
			Id: index,
			Name: file,
			TaskType: "map",
			State: "idle",
		}
		m.mapTask = append(m.mapTask, &task)
	}

	m.mapTaskNum = len(files)
	m.reduceTaskNum = nReduce

	for i := 0; i < nReduce; i++ {
		task := Task{
			Id: i,
			Name: strconv.Itoa(i),
			TaskType: "reduce",
			State: "idle",
		}
		m.reduceTask = append(m.reduceTask, &task)
	}
	m.server()
	return &m
}
