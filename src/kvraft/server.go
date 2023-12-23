package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false
const TIMEOUT = 200

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ClientOperation struct {
	SequenceNum int
	Value       string
	Success     Err
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastClientOperations map[int64]ClientOperation
	memoryKV             MemoryKV
	replyClientChans     map[int]chan KVReply
	lastApplied          int
}

func (kv *KVServer) KVRequest(args *KVRequest, reply *KVReply) {
	// Your code here.
	kv.mu.RLock()
	value, exists := kv.lastClientOperations[args.Command.ClientID]
	if args.Command.Op != "Get" && exists && (value.SequenceNum == args.Command.SequenceNum) {
		reply.Response = value.Value
		reply.Status = Err(OK)
		DPrintf("args: %v is duplicate", args)
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	index, _, success := kv.rf.Start(args.Command)
	if !success {
		reply.Status = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch, exists := kv.replyClientChans[index]
	if !exists {
		kv.replyClientChans[index] = make(chan KVReply, 1)
		ch = kv.replyClientChans[index]
	}

	kv.mu.Unlock()

	select {
	case notice := <-ch:
		reply.Response = notice.Response
		reply.Status = notice.Status
	case <-time.After(TIMEOUT * time.Millisecond):
		reply.Status = ErrTimeout
	}
	go func() {
		kv.mu.Lock()
		delete(kv.replyClientChans, index)
		kv.mu.Unlock()
	}()
	return
}

func (kv *KVServer) applyToClients() {
	for {
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			kv.mu.Lock()
			command := applyMsg.Command.(Command)
			if applyMsg.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = applyMsg.CommandIndex
			response := ""
			status := Err(OK)
			if command.Op != "Get" && kv.lastClientOperations[command.ClientID].SequenceNum == command.SequenceNum {
				response = kv.lastClientOperations[command.ClientID].Value
				status = kv.lastClientOperations[command.ClientID].Success
			} else {
				response, status = kv.memoryKV.doCommand(command)
				kv.lastClientOperations[command.ClientID] = ClientOperation{Value: response, Success: status, SequenceNum: command.SequenceNum}
			}
			if term, isLeader := kv.rf.GetState(); isLeader && applyMsg.CommandTerm == term {
				_, exists := kv.replyClientChans[applyMsg.CommandIndex]
				if !exists {
					ch := make(chan KVReply, 1)
					kv.replyClientChans[applyMsg.CommandIndex] = ch
				}
				kv.replyClientChans[applyMsg.CommandIndex] <- KVReply{Response: response, Status: status}
			}
			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
				kv.snapShot(applyMsg.CommandIndex)
			}
			kv.mu.Unlock()
		} else if applyMsg.SnapshotValid {
			kv.mu.Lock()
			kv.InstallSnapShot(applyMsg.Snapshot)
			kv.lastApplied = applyMsg.SnapshotIndex
			kv.mu.Unlock()
		}
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

func (kv *KVServer) InstallSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var stateMachine MemoryKV
	var lastOperations map[int64]ClientOperation
	if d.Decode(&stateMachine) != nil ||
		d.Decode(&lastOperations) != nil {
		DPrintf("something went wrong when decoding")
	}
	kv.memoryKV, kv.lastClientOperations = stateMachine, lastOperations
}

func (kv *KVServer) snapShot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.memoryKV)
	e.Encode(kv.lastClientOperations)
	kv.rf.Snapshot(index, w.Bytes())
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
	labgob.Register(Command{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.lastApplied = 0
	kv.memoryKV = MemoryKV{KV: make(map[string]string)}
	kv.lastClientOperations = make(map[int64]ClientOperation)
	kv.replyClientChans = make(map[int]chan KVReply)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.InstallSnapShot(persister.ReadSnapshot())
	// You may need initialization code here.
	go kv.applyToClients()
	return kv
}
