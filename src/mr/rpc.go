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

//Worker向Coordinator请求任务
type GetTaskArgs struct {
	WorkerID int;
}

//Worker向Coordinator请求任务的回复
type GetTaskReply struct {
    TaskType  string    // 任务类型（"map" 或 "reduce"）
    InputFile string    // 输入文件
    TaskId    int       // 任务 ID
    NReduce   int       // Reduce 任务的数量
    AllDone   bool      // 是否所有任务都已完成
}

// Worker向Coordinator报告任务完成
type ReportTaskArgs struct {
	WorkerID int       // Worker ID
	TaskId    int       // 任务 ID
	TaskType  string    // 任务类型（"map" 或 "reduce"）
}	

// Worker向Coordinator报告任务完成的回复
type ReportTaskReply struct {
	Success bool      // 是否成功报告任务完成	
}		
// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
