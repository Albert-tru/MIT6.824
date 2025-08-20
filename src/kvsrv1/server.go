package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data map[string]struct {
		value   string //存储对应的值
		version int    //存储对应的版本号
	}
}

// 初始化KVServer
func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.data = make(map[string]struct {
		value   string //存储对应的值
		version int    //存储对应的版本号
	})
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	//加锁，【避免并发读写冲突】
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//键为空
	if args.Key == "" {
		reply.Err = rpc.ErrNoKey
		return
	}

	//键不为空，值存在的话
	if _, ok := kv.data[args.Key]; ok {
		reply.Value = kv.data[args.Key].value
		reply.Version = rpc.Tversion(kv.data[args.Key].version) //类型转换
		reply.Err = rpc.OK
		return
	}

	//键非空，但不存在
	reply.Err = rpc.ErrNoKey
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
// Put不需要返回值
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {

	//加锁[KVserver会被多个goroutine访问，操作之前要加锁，操作之后要解锁]
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//处理空key
	if args.Key == "" {
		reply.Err = rpc.ErrNoKey
		return
	}

	entry, exist := kv.data[args.Key]

	//key存在
	if exist {
		//比较版本号
		if args.Version != rpc.Tversion(entry.version) {
			reply.Err = rpc.ErrVersion
			return
		}
		//版本匹配
		entry.value = args.Value
		entry.version++
		kv.data[args.Key] = entry
		reply.Err = rpc.OK
	} else {
		//key不存在，传入的version为0时，创建映射
		if args.Version == 0 {
			entry.value = args.Value
			entry.version = 1
			kv.data[args.Key] = entry
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
