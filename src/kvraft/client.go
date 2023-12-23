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
	ck.sequenceNum = 1
	return ck
}

func (ck *Clerk) sendKVRequest(args *KVRequest, reply *KVReply) string {
	for {
		ok := ck.servers[ck.currentLeader].Call("KVServer.KVRequest", args, reply)

		if !ok || reply.Status == ErrWrongLeader || reply.Status == ErrTimeout {
			if !ok {
				//DPrintf("client can't find server %v", ck.currentLeader)
			} else {
				//DPrintf("client get an %v reply", reply)
			}
			ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
			continue
		}
		//DPrintf("REPLY: %v", reply)
		return reply.Response
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
	value := ck.ClientCommand(key, "", "Get")
	// You will have to modify this function.
	return value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.ClientCommand", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) ClientCommand(key string, value string, op string) string {
	// You will have to modify this function.
	args := KVRequest{Command: Command{Op: op, Key: key, Value: value, ClientID: ck.me, SequenceNum: ck.sequenceNum}}
	//DPrintf("client %v send a %v request, command is key: %v value: %v", ck.me, op, key, value)
	reply := KVReply{}
	response := ck.sendKVRequest(&args, &reply)
	ck.sequenceNum++
	//DPrintf("client get a response: %v", response)
	return response
}

func (ck *Clerk) Put(key string, value string) {
	ck.ClientCommand(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.ClientCommand(key, value, "Append")
}
