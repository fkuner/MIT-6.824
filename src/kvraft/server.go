package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op string
	Key string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data map[string]string
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("[server %d] Execute Get", kv.me)
	op := Op{
		Op: "Get",
		Key: args.Key,
	}
	DPrintf("[server] Get Op:{Op:%v, Key:%v}", op.Op, op.Key)
	kv.rf.Start(op)
	applyMsg := <- kv.applyCh
	if applyMsg.Command == op {
		value, ok := kv.data[args.Key]
		if !ok {
			reply.Err = ErrNoKey
		}
		reply.Err = OK
		reply.Value = value
	} else {
		kv.applyCh <- applyMsg
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("[server %d] Execute PutAppend", kv.me)
	op := Op{
		Op: args.Op,
		Key: args.Key,
		Value: args.Value,
	}
	DPrintf("[server] PutAppend Op:{Op:%v, Key:%v, Value:%v}", op.Op, op.Key, op.Value)
	kv.rf.Start(op)
	applyMsg := <- kv.applyCh
	DPrintf("hahaha")
	if applyMsg.Command == op {
		if args.Op == "Put" {
			kv.data[args.Key] = args.Value
		} else {
			kv.data[args.Key] += args.Value
		}
		reply.Err = OK
	} else {
		kv.applyCh <- applyMsg
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)

	go func() {
		time.Sleep(1 * time.Second)
		for {
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				<- kv.applyCh
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	// You may need initialization code here.
	return kv
}
