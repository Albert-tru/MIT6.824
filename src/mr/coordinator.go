package mr

import (
    "log"
    "net"
    "os"
    "net/rpc"
    "net/http"
    "time"
    "sync"
)

// 任务状态枚举
type TaskStatus int

const (
    TaskStatusIdle TaskStatus = iota
    TaskStatusAssigned
    TaskStatusDone
)

// 任务阶段枚举
type Phase int

const (
    PhaseMap Phase = iota
    PhaseReduce
    PhaseDone
)

// Map 任务结构体
type MapTask struct {
    id         int
    inputFile  string
    status     TaskStatus
    startTime  time.Time
}

// Reduce 任务结构体
type ReduceTask struct {
    id        int
    status    TaskStatus
    startTime time.Time
}

type Coordinator struct {
    // 互斥锁，保证线程安全
    mu sync.Mutex

    // Map 任务相关
    mapTasks   []MapTask
    mapPhase   Phase

    // Reduce 任务相关
    reduceTasks []ReduceTask
    reducePhase Phase

    // 所有任务是否完成
    allDone bool

    // Reduce 任务数量
    nReduce int
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

// filepath: /home/dmin/golang_learn/6.5840/src/mr/coordinator.go
//GetTask: Coordinator 的一个 RPC 方法，用于向 Worker 分配任务。
//	c *Coordinator:这个函数的接收者是个 Coordinator 类型的指针
//	接收参数：GetTaskArgs 类型的参数,GetTaskReply 类型的回复
//	返回值：error 类型的错误
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
    //互斥锁c.mu
	c.mu.Lock()
	//defer确保在函数返回时自动解锁
    defer c.mu.Unlock()

	//检查是否所有 Map 任务已完成
    if c.allDone {
        reply.AllDone = true
        return nil
    }

    // 查找未分配的 Map 任务
    for i, task := range c.mapTasks {
        if task.status == TaskStatusIdle {
            task.status = TaskStatusAssigned
            task.startTime = time.Now()
            c.mapTasks[i] = task

            reply.TaskType = "map"
            reply.InputFile = task.inputFile
            reply.TaskId = task.id
            reply.NReduce = c.nReduce
            reply.AllDone = false
            return nil
        }
    }

    // 查找未分配的 Reduce 任务（如果 Map 任务已完成）
    if c.mapPhase == PhaseDone {
        for i, task := range c.reduceTasks {
            if task.status == TaskStatusIdle {
                task.status = TaskStatusAssigned
                task.startTime = time.Now()
                c.reduceTasks[i] = task

                reply.TaskType = "reduce"
                reply.TaskId = task.id
                reply.NReduce = c.nReduce
                reply.AllDone = false
                return nil
            }
        }
    }

    // 没有任务可分配
    reply.TaskType = ""
    reply.AllDone = false
    return nil
}

// ReportTask: Coordinator 的一个 RPC 方法，用于 Worker 向 Coordinator 报告任务完成情况。
//	c *Coordinator:这个函数的接收者是个 Coordinator 类型的指针		
//	接收参数：ReportTaskArgs 类型的参数,ReportTaskReply 类型的回复
//	返回值：error 类型的错误
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    reply.Success = true

	//检查任务类型是否为 Map 任务
    if args.TaskType == "map" {
		// 如果是 Map 任务，更新对应的 Map 任务状态
        task := c.mapTasks[args.TaskId]
		//检查任务状态是否为已分配
        if task.status == TaskStatusAssigned {
            task.status = TaskStatusDone
            c.mapTasks[args.TaskId] = task
            c.checkMapPhase()
        }
    } else if args.TaskType == "reduce" {
        task := c.reduceTasks[args.TaskId]
        if task.status == TaskStatusAssigned {
            task.status = TaskStatusDone
            c.reduceTasks[args.TaskId] = task
            c.checkReducePhase()
        }
    }

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
    c.mu.Lock()
    defer c.mu.Unlock()
    return c.allDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
    c := Coordinator{
        nReduce: nReduce,
        mapPhase: PhaseMap,
        reducePhase: PhaseMap, // 初始阶段为 Map 阶段，Reduce 阶段还未开始
        allDone: false,
    }

    // 初始化 Map 任务
    for i, file := range files {
        task := MapTask{
            id:        i,
            inputFile: file,
            status:    TaskStatusIdle,
        }
        c.mapTasks = append(c.mapTasks, task)
    }

    // 初始化 Reduce 任务
    for i := 0; i < nReduce; i++ {
        task := ReduceTask{
            id:     i,
            status: TaskStatusIdle,
        }
        c.reduceTasks = append(c.reduceTasks, task)
    }

    c.server()
    return &c
}

func (c *Coordinator) checkMapPhase() {
    for _, task := range c.mapTasks {
        if task.status != TaskStatusDone {
            return
        }
    }
    c.mapPhase = PhaseDone
}

func (c *Coordinator) checkReducePhase() {
    for _, task := range c.reduceTasks {
        if task.status != TaskStatusDone {
            return
        }
    }
    c.reducePhase = PhaseDone
    c.allDone = true
}

