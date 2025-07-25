package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort" // 导入 sort 包
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
// 这个函数是把用户定义的 mapf 和 reducef 注册进来，worker 就能用这两个函数去处理任务了。
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	workerId := generateWorkerId()

	for {
		// 1. 请求任务
		args := GetTaskArgs{WorkerID: workerId}
		reply := GetTaskReply{}
		// 调用 Coordinator 的 GetTask 方法
		// 这里的 call 函数会发送 RPC 请求到 Coordinator
		ok := call("Coordinator.GetTask", &args, &reply)
		if !ok {
			fmt.Printf("call failed!\n")
			break
		}

		// 2. 处理任务
		if reply.AllDone {
			fmt.Printf("all tasks done, worker exit\n")
			break
		}

		switch reply.TaskType {
		case "map":
			doMap(mapf, reply.InputFile, reply.TaskId, reply.NReduce, workerId)
			reportTask(reply.TaskId, "map", workerId)
		case "reduce":
			// TODO: 实现 doReduce
			doReduce(reducef, reply.TaskId, reply.NReduce)
			reportTask(reply.TaskId, "reduce", workerId)
		case "":
			// 没有任务，sleep 一下
			time.Sleep(time.Second)
		default:
			fmt.Printf("unknown task type %v\n", reply.TaskType)
		}

		time.Sleep(time.Millisecond * 10)
	}
}

func doMap(
	mapf func(string, string) []KeyValue,
	inputFile string,
	taskId int,
	nReduce int,
	workerId int,
) {
	// 1. 读取输入文件
	content, err := os.ReadFile(inputFile)
	if err != nil {
		log.Fatalf("cannot read %v", inputFile)
	}

	// 2. 调用 Map 函数
	// Map函数：读取输入文件，处理键值对，并把这些键值对传给kva
	kva := mapf(inputFile, string(content))

	// 3. 将中间结果写入中间文件
	intermediate := make([][]KeyValue, nReduce) //这是一个长度为nReduce的[]KeyValue切片
	//intermediate[i] 代表所有将发送到第 i 个 Reduce 任务的键值对。
	for _, kv := range kva {
		reduceId := ihash(kv.Key) % nReduce
		intermediate[reduceId] = append(intermediate[reduceId], kv)
	}

	for i := 0; i < nReduce; i++ {
		outputFile := fmt.Sprintf("mr-%d-%d", taskId, i)
		tempFile := outputFile + "-" + strconv.Itoa(workerId)
		// 创建临时文件
		file, err := os.Create(tempFile)
		if err != nil {
			log.Fatalf("cannot create %v", tempFile)
		}

		// 写入 JSON 数据
		enc := json.NewEncoder(file)
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode %v", kv)
			}
		}
		// 关闭文件
		err = file.Close()
		if err != nil {
			log.Fatalf("cannot close %v", tempFile)
		}

		// 重命名临时文件为正式文件
		err = os.Rename(tempFile, outputFile)
		if err != nil {
			log.Fatalf("cannot rename %v to %v", tempFile, outputFile)
		}
	}
}

// doReduce 执行一个 Reduce 任务。
// reducef: 用户定义的 reduce 函数 (func(string, []string) string)
// reduceId: 当前 Reduce 任务的 ID
// nMap: Map 任务的总数 (用于查找所有相关的中间文件)
// ByKey 用于实现对 KeyValue 切片按键排序
type ByKey []KeyValue

// Len 实现 sort.Interface 接口的 Len 方法
func (a ByKey) Len() int { return len(a) }

// Less 实现 sort.Interface 接口的 Less 方法
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Swap 实现 sort.Interface 接口的 Swap 方法
func (a ByKey) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func doReduce(reducef func(string, []string) string, reduceId int, nMap int) {
	// 1. 收集所有分配给当前 Reduce 任务的中间键值对。
	// 这些键值对分散在 nMap 个中间文件中，每个文件由一个 Map 任务生成。
	intermediate := []KeyValue{}
	for i := 0; i < nMap; i++ {
		fileName := reduceName(i, reduceId) // 获取中间文件名 (mr-mapID-reduceID)
		file, err := os.Open(fileName)
		if err != nil {
			// 如果文件不存在或无法打开，可能是因为对应的 Map 任务没有为这个 Reduce 任务生成数据。
			// 在实际的 MapReduce 中，这通常不会发生，因为每个 Map 任务都会尝试为所有 Reduce 任务生成文件。
			// 但为了健壮性，可以打印警告并继续。
			// log.Printf("Warning: Could not open intermediate file %s: %v\n", fileName, err)
			continue // 继续处理下一个文件
		}

		// 使用 JSON 解码器读取文件中的键值对
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				// 遇到文件末尾或解码错误时停止
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close() // 关闭文件
	}

	// 2. 根据键对中间键值对进行排序。
	// 这使得相同键的所有值都相邻，便于后续分组。
	sort.Sort(ByKey(intermediate))

	// 3. 遍历排序后的键值对，按键分组值，并为每个键调用 reduce 函数。
	oname := mergeName(reduceId) // 生成最终输出文件名 (mr-out-reduceID)
	// 创建一个临时输出文件，以确保原子性写入
	tmpFileName := fmt.Sprintf("%s-tmp-%d", oname, os.Getpid()) // 使用进程ID作为临时文件后缀
	ofile, err := os.Create(tmpFileName)
	if err != nil {
		log.Fatalf("Cannot create temporary output file %s: %v", tmpFileName, err)
	}

	// 使用 JSON 编码器将最终结果写入输出文件
	// 注意：Lab 1 通常要求最终输出是文本格式，每行一个键值对，用空格分隔。
	// 如果是这样，您需要将 json.NewEncoder 替换为 fmt.Fprintf。
	// 这里我先用 JSON 编码器，因为它更通用，如果 Lab 要求文本格式，请修改。
	// enc := json.NewEncoder(ofile) // 如果需要 JSON 输出

	i := 0
	for i < len(intermediate) {
		j := i + 1
		// 找到所有具有相同键的键值对
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		// 收集当前键的所有值
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		// 调用用户定义的 reduce 函数
		output := reducef(intermediate[i].Key, values)

		// 将键和 reduce 后的值写入输出文件
		// Lab 1 通常要求输出格式为 "key value\n"
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		// 如果需要 JSON 输出，使用以下代码：
		// if err := enc.Encode(KeyValue{Key: intermediate[i].Key, Value: output}); err != nil {
		// 	log.Fatalf("Cannot encode output for key %s: %v", intermediate[i].Key, err)
		// }

		i = j // 移动到下一个不同的键
	}
	ofile.Close() // 关闭临时文件

	// 4. 原子性地重命名临时文件为最终输出文件
	err = os.Rename(tmpFileName, oname)
	if err != nil {
		log.Fatalf("Cannot rename temporary output file %s to %s: %v", tmpFileName, oname, err)
	}
}

func reportTask(taskId int, taskType string, workerId int) {
	args := ReportTaskArgs{
		TaskId:   taskId,
		TaskType: taskType,
		WorkerID: workerId,
	}
	reply := ReportTaskReply{}

	ok := call("Coordinator.ReportTask", &args, &reply)
	if !ok {
		fmt.Printf("report task failed!\n")
	}
}

func generateWorkerId() int {
	return int(rand.Int31())
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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
	//调用Coordinator的Example方法
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

// call是如何实现的？
// 它会创建一个 RPC 客户端，连接到 Coordinator 的 UNIX 域套接字，然后发送 RPC 请求。
// rpcname 是 RPC 方法的名称，args 是请求参数，reply 是响应参数。
// 如果调用成功，返回 true，否则返回 false。
// 注意：这里的 call 函数是一个通用的 RPC 调用函数，可以用于任何 RPC 方法的调用。
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

// reduceName 根据 Map 任务 ID 和 Reduce 任务 ID 生成中间文件名
func reduceName(mapId, reduceId int) string {
	return fmt.Sprintf("mr-%d-%d", mapId, reduceId)
}

// mergeName 根据 Reduce 任务 ID 生成最终输出文件名
func mergeName(reduceId int) string {
	return fmt.Sprintf("mr-out-%d", reduceId)
}
