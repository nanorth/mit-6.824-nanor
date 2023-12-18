package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"math"
	"math/rand"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type State int

const (
	Leader    State = iota // 0
	Follower               // 1
	Candidate              // 2
)

const HEATBEAT int = 100   // leader send heatbeat
const TIMEOUTLOW int = 300 // the timeout period randomize between 300ms - 600ms
const TIMEOUTHIGH int = 900
const APPLYCHECKER int = 100

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// 2A
	state         State
	currentTerm   int
	voteFor       int
	startElection bool // true if receive rpc / heartbeat
	grantedVote   int  // count for grantedVotes
	// 2B
	logs        []Entry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyCh     chan ApplyMsg
	// 2D
	lastIncludedIndex int
	lastIncludedTerm  int
	offset            int
	snapshot          []byte

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	//DPrintf("getstate hold the lock")
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.state == Leader
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) persistAndSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()

	if len(rf.snapshot) == 0 {
		rf.persister.SaveStateAndSnapshot(data, nil)
	} else {
		rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
	}

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	//DPrintf("readpersist hold the lock")
	defer rf.mu.Unlock()
	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.voteFor) != nil || d.Decode(&rf.logs) != nil ||
		d.Decode(&rf.lastIncludedIndex) != nil || d.Decode(&rf.lastIncludedTerm) != nil {
		DPrintf("something went wrong when decoding")
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	//DPrintf("lock is being hold")
	rf.mu.Lock()
	//DPrintf("snapshot hold the lock")
	defer rf.mu.Unlock()
	if index > rf.commitIndex || index <= rf.lastIncludedIndex {
		return
	}
	entry := rf.indexToEntry(index)
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.logs[entry].Term
	rf.snapshot = snapshot
	newLogs := make([]Entry, 0)
	newLogs = append(newLogs, Entry{Term: 0})
	newLogs = append(newLogs, rf.logs[entry+1:]...)
	rf.logs = newLogs
	DPrintf("snapshot!!!!!! Node %v do a snapshot", rf.me)
	rf.persistAndSnapshot()

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendSnapShot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	//DPrintf("start hold the lock")
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := rf.state == Leader

	if !isLeader {
		return index, term, false
	}

	// Your code here (2B).

	term = rf.currentTerm
	rf.logs = append(rf.logs, Entry{Term: term, Command: command})
	index = rf.getLastLogIndex()
	rf.nextIndex[rf.me]++
	//rf.matchIndex[rf.me]++
	isLeader = true
	DPrintf("user is adding a command on index %v", index)
	rf.persist()
	//DPrintf("user finished add a command on index %v", index)
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		//DPrintf("ticker hold the lock")
		if rf.state != Leader {
			if rf.startElection {
				go rf.ElectionWithLock()
			} else {
				rf.startElection = true
			}
		}
		rf.mu.Unlock()
		//cant use this stupid trick!!!!!!!!!too much split votes! my bad, this might be fine
		rand.Seed(time.Now().UnixNano())
		randomNumber := rand.Intn(TIMEOUTHIGH-TIMEOUTLOW) + TIMEOUTLOW
		//DPrintf("{Node %v} will sleep %v millisecond", rf.me, randomNumber)
		time.Sleep(time.Millisecond * time.Duration(randomNumber))
	}
}

func (rf *Raft) heartBeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		//DPrintf("heartbeat hold the lock")
		if rf.state == Leader {
			rf.BroadcastHeartbeat()
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * time.Duration(HEATBEAT))
	}
}
func (rf *Raft) resetElectionTimer() {
	//DPrintf("{Node %v} timer is reset", rf.me)
	rf.startElection = false
}

func (rf *Raft) ElectionWithLock() {
	rf.mu.Lock()
	//DPrintf("election hold the lock")
	rf.resetElectionTimer()
	rf.state = Candidate
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.grantedVote = 1
	DPrintf("{Node %v} become candidate in term %v", rf.me, rf.currentTerm)
	rf.persist()
	Term := rf.currentTerm
	CandidateId := rf.me
	LastLogIndex := rf.getLastLogIndex()
	LastLogTerm := rf.logs[rf.indexToEntry(LastLogIndex)].Term
	Me := rf.me
	rf.mu.Unlock()
	//finished := false
	for server := range rf.peers {
		if server == Me {
			continue
		}
		go func(server int) {
			args := RequestVoteArgs{}
			reply := RequestVoteReply{}
			args.Term = Term
			args.CandidateId = CandidateId
			args.LastLogIndex = LastLogIndex
			args.LastLogTerm = LastLogTerm

			ok := rf.sendRequestVote(server, &args, &reply)

			if ok {
				rf.mu.Lock()
				//DPrintf("votes hold the lock")
				//DPrintf("{Node %v}get {Node %v}'s votes in term %v,", rf.me, server, args.Term)
				if rf.currentTerm == args.Term && rf.state == Candidate {
					//DPrintf("{Node %v} get a reply from {Node %v}: %v", rf.me, server, reply)
					if reply.VoteGranted {
						rf.grantedVote++
						if rf.grantedVote > len(rf.peers)/2 {
							//finished = true
							DPrintf("{Node %v} receives majority votes in term %v and become leader", rf.me, rf.currentTerm)
							rf.setState(Leader, rf.currentTerm)
							rf.BroadcastHeartbeat()
						}
					} else if rf.currentTerm < reply.Term {
						DPrintf("{Node %v} finds a newer {Node %v} with term %v ", rf.me, server, reply.Term)
						rf.setState(Follower, reply.Term)
					}
				}
				rf.mu.Unlock()
			}
		}(server)
	}

}

func (rf *Raft) BroadcastHeartbeat() {
	//DPrintf("{Node %v} is BroadcastHeartbeat in term %v", rf.me, rf.currentTerm)
	nextIndexes := make([]int, len(rf.nextIndex))
	copy(nextIndexes, rf.nextIndex)
	//DPrintf("{Node %v}'s nextIndexes %v", rf.me, nextIndexes)
	Term := rf.currentTerm
	LeaderId := rf.me
	logs := make([]Entry, len(rf.logs))
	copy(logs, rf.logs)
	LeaderCommit := rf.commitIndex
	LastLogIncluded := rf.lastIncludedIndex
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		if rf.nextIndex[server] <= rf.lastIncludedIndex {
			installSnapshotArgs := InstallSnapshotArgs{
				Term:             rf.currentTerm,
				LeaderId:         rf.me,
				LastIncludeIndex: rf.lastIncludedIndex,
				LastIncludedTerm: rf.lastIncludedTerm,
				Data:             rf.snapshot,
			}
			installSnapshotReply := InstallSnapshotReply{}
			go func(server int) {
				ok := rf.sendSnapShot(server, &installSnapshotArgs, &installSnapshotReply)
				rf.mu.Lock()
				DPrintf("node %v installsnapshot to node %v", rf.me, server)
				if ok {
					if installSnapshotReply.Term > rf.currentTerm {
						rf.setState(Follower, installSnapshotReply.Term)
					} else {
						rf.nextIndex[server] = int(math.Max(float64(LastLogIncluded+1), float64(rf.nextIndex[server])))
						rf.matchIndex[server] = int(math.Max(float64(LastLogIncluded), float64(rf.matchIndex[server])))
						if rf.nextIndex[server] > rf.getLastLogIndex()+1 {
							rf.offset = 100
						}
					}
				}
				rf.mu.Unlock()
			}(server)
		} else {
			go func(server int) {
				args := AppendEntriesArgs{}
				reply := AppendEntriesReply{}
				//DPrintf("NextIndex: %v", rf.nextIndex[server])
				nextIndex := nextIndexes[server]
				args.Term = Term
				args.LeaderId = LeaderId
				args.PrevLogIndex = nextIndex - 1
				args.PrevLogTerm = logs[args.PrevLogIndex-LastLogIncluded].Term
				args.LeaderCommit = LeaderCommit
				args.Entries = logs[(nextIndex - LastLogIncluded):]

				// Make the RPC call
				ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
				if ok {
					rf.mu.Lock()
					//DPrintf("append hold the lock")
					if reply.Success == false {
						if reply.Term > rf.currentTerm {
							rf.setState(Follower, reply.Term)
							DPrintf("{Node %v} find himself outdated and he is now follower in term %v", rf.me, rf.currentTerm)
						} else {
							if reply.XTerm == -1 {
								rf.nextIndex[server] = reply.XLen + 1
								if rf.nextIndex[server] > rf.getLastLogIndex()+1 {
									rf.offset = 100
								}
							} else {
								containsXTerm := -1
								for index, log := range rf.logs {
									if log.Term == reply.XTerm {
										containsXTerm = index
									}
								}
								if containsXTerm == -1 {
									rf.nextIndex[server] = reply.XIndex
									if rf.nextIndex[server] == 0 {
										rf.nextIndex[server]++
									}
									if rf.nextIndex[server] > rf.getLastLogIndex()+1 {
										rf.offset = 100
									}
								} else {
									rf.nextIndex[server] = containsXTerm + rf.lastIncludedTerm
									if rf.nextIndex[server] == 0 {
										rf.nextIndex[server]++
									}
									if rf.nextIndex[server] > rf.getLastLogIndex()+1 {
										rf.offset = 100
									}
								}
							}
							//if rf.nextIndex[server] <= rf.lastIncludedIndex {
							//	installSnapshotArgs := InstallSnapshotArgs{
							//		Term:             rf.currentTerm,
							//		LeaderId:         rf.me,
							//		LastIncludeIndex: rf.lastIncludedIndex,
							//		LastIncludedTerm: rf.lastIncludedTerm,
							//		Data:             rf.snapshot,
							//	}
							//	installSnapshotReply := InstallSnapshotReply{}
							//	go func(server int) {
							//		ok := rf.sendSnapShot(server, &installSnapshotArgs, &installSnapshotReply)
							//		rf.mu.Lock()
							//		DPrintf("node %v installsnapshot to node %v", rf.me, server)
							//		if ok {
							//			if installSnapshotReply.Term > rf.currentTerm {
							//				rf.setState(Follower, installSnapshotReply.Term)
							//			} else {
							//				rf.nextIndex[server] = int(math.Max(float64(LastLogIncluded+1), float64(rf.nextIndex[server])))
							//				rf.matchIndex[server] = int(math.Max(float64(LastLogIncluded), float64(rf.matchIndex[server])))
							//			}
							//		}
							//		rf.mu.Unlock()
							//	}(server)
							//}
						}
					} else {
						rf.nextIndex[server] = nextIndex + len(args.Entries)
						rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
						if rf.nextIndex[server] > rf.getLastLogIndex()+1 {
							rf.offset = 100
						}
					}
					rf.mu.Unlock()
				}
			}(server)
		}
	}
	//DPrintf("{Node %v} finish a BroadcastHeartbeat in term %v", rf.me, rf.currentTerm)
	//DPrintf("{Node %v}'s matchIndexes %v", rf.me, rf.matchIndex)
	// update nextIndex[] and matchIndex[]

}

func (rf *Raft) setState(state State, term int) {
	switch state {
	case Follower:
		rf.state = Follower
		rf.currentTerm = term
		rf.voteFor = -1
		rf.grantedVote = 0
	case Leader:
		rf.state = Leader
		rf.currentTerm = term
		rf.grantedVote = 0
		rf.nextIndex = make([]int, len(rf.peers))
		DPrintf("nextIndex for all is %v", rf.getLastLogIndex()+1)
		for i := range rf.nextIndex {
			rf.nextIndex[i] = rf.getLastLogIndex() + 1
		}
		rf.matchIndex = make([]int, len(rf.peers))
		//rf.BoardCastLog()
	}
	rf.persist()
	//rf.resetElectionTimer()
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) - 1 + rf.lastIncludedIndex
}

func (rf *Raft) indexToEntry(index int) int {
	return index - rf.lastIncludedIndex
}

func (rf *Raft) apply() {
	for rf.killed() == false {
		rf.mu.Lock()
		//DPrintf("apply hold the lock")
		if rf.lastIncludedTerm > rf.commitIndex {
			rf.commitIndex = rf.lastIncludedIndex
			rf.lastApplied = rf.lastIncludedIndex
		} else {
			commitIndex := rf.commitIndex
			for N := range rf.logs[rf.indexToEntry(commitIndex+1):] {
				count := 1
				for index, matchIndex := range rf.matchIndex {
					if index == rf.me {
						continue
					}
					if matchIndex > N+commitIndex {
						count++
						if count > len(rf.peers)/2 && rf.logs[rf.indexToEntry(N+commitIndex+1)].Term == rf.currentTerm {
							rf.commitIndex = N + 1 + commitIndex
							break
						}
					}
				}
			}
		}

		appliedMsgs := []ApplyMsg{}
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			msg := ApplyMsg{CommandValid: true, CommandIndex: rf.lastApplied, Command: rf.logs[rf.indexToEntry(rf.lastApplied)].Command}
			appliedMsgs = append(appliedMsgs, msg)
			DPrintf("Node %v apply command %v", rf.me, rf.lastApplied)
		}
		rf.mu.Unlock()
		for _, msg := range appliedMsgs {
			rf.applyCh <- msg
		}
		time.Sleep(time.Millisecond * time.Duration(APPLYCHECKER))
	}

}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	// 2A
	rf.voteFor = -1
	rf.grantedVote = 0
	rf.currentTerm = 0
	rf.state = Follower
	rf.startElection = false

	// 2B
	rf.logs = make([]Entry, 0)
	rf.logs = append(rf.logs, Entry{Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartBeat()
	go rf.apply()
	return rf
}
