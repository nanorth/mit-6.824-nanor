package kvraft

import (
	"6.824/labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	currentLeader int
	sequenceNum   int
	me            int64
	//mu            sync.Mutex
}

type Command struct {
	Op    string
	Key   string
	Value string
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
	ck.currentLeader = 0
	ck.me = nrand()
	ck.sequenceNum = 0
	return ck
}

type KVRequest struct {
	ClientId    int64
	SequenceNum int
	Command     Command
}

type KVReply struct {
	Status   bool
	Response string
}

func (ck *Clerk) sendKVRequest(server int, args *KVRequest, reply *KVReply) string {
	ok := ck.servers[server].Call("KVServer.KVRequest", args, reply)
	if ok && reply.Status == true {
		return reply.Response
	}
	for {
		for index, _ := range ck.servers {
			ok = ck.servers[index].Call("KVServer.KVRequest", args, reply)
			if ok && reply.Status == true {
				return reply.Response
			}
		}
	}
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
	value := ck.PutAppend(key, "", "get")
	// You will have to modify this function.
	return value
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
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	ck.sequenceNum++
	args := KVRequest{ClientId: ck.me, SequenceNum: ck.sequenceNum, Command: Command{Op: op, Key: key, Value: value}}
	reply := KVReply{}
	response := ck.sendKVRequest(ck.currentLeader, &args, &reply)
	return response
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
