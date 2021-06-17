package kvraft

import (
	"../labrpc"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	serialNum int
	clientID int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.serialNum = 0
	ck.clientID = int(nrand())
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	DPrintf("[Client %d] Execute Get Operation", ck.clientID)
	args := GetArgs{
		Key: key,
		ClientId: ck.clientID,
		SerialNum: ck.serialNum,
	}
	ck.serialNum++
	DPrintf("[Client %d] GetArgs:{Key:%v, SerialNum:%d}", ck.clientID, args.Key, args.SerialNum)
	for {
		for i := 0; i < len(ck.servers); i++ {
			reply := GetReply{}
			ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
			if !ok {
				DPrintf("[Client %d] Get access false", ck.clientID)
				continue
			}
			if reply.Err == ErrWrongLeader {
				//DPrintf("[Client] Get wrong leader")
				continue
			} else if reply.Err == ErrNoKey {
				DPrintf("[Client %d] Get no key", ck.clientID)
				return ""
			} else {
				DPrintf("[Client %d] Get success", ck.clientID)
				DPrintf("[Client %d] GetReply:%v, GetArgs:%v", ck.clientID, reply, args)
				return reply.Value
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	DPrintf("[Client %d] Execute PutAppend Operation", ck.clientID)
	args := PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		ClientId: ck.clientID,
		SerialNum: ck.serialNum,
	}
	ck.serialNum++
	DPrintf("[Client %d] PutAppendArgs: {Key:%v, Value:%v, Op:%v, SerialNum:%v}", ck.clientID, args.Key, args.Value, args.Op, args.SerialNum, )
	for {
		for i := 0; i < len(ck.servers); i++ {
			reply := PutAppendReply{}
			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			if !ok {
				DPrintf("[Client %d] PutAppend access false", ck.clientID)
				continue
			}
			if reply.Err == ErrWrongLeader {
				//DPrintf("[Client] PutAppend wrong leader")
				continue
			} else if reply.Err == ErrNoKey {
				DPrintf("[Client %d] PutAppend no key", ck.clientID)
				return
			} else {
				DPrintf("[Client %d] PutAppend success", ck.clientID)
				DPrintf("[Client %d] PutAppendReply:%v, PutAppendArgs:%v", ck.clientID, reply, args)
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
