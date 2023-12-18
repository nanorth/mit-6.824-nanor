package raft

import (
	"math"
)

//
// example RequestVote RPC handler.
//

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// Your data here (2A,2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here (2A,2B).
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludedTerm int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	//DPrintf("node{v}vote args")
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.voteFor != -1 && rf.voteFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.setState(Follower, args.Term)
	}
	if args.LastLogTerm <= rf.logs[len(rf.logs)-1].Term && (args.LastLogTerm != rf.logs[len(rf.logs)-1].Term || args.LastLogIndex < rf.getLastLogIndex()) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	rf.voteFor = args.CandidateId
	rf.persist()
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
	//rf.resetElectionTimer()
	rf.mu.Unlock()

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	if (args.Term == rf.currentTerm && rf.state == Candidate) || args.Term > rf.currentTerm {
		rf.setState(Follower, args.Term)
	}
	//DPrintf("Node %v log is %v", rf.me, rf.logs)
	//DPrintf("args.Entries is %v, prvidx is %v", args.Entries, args.PrevLogIndex)
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.Success = false
		reply.XLen = len(rf.logs) + rf.lastIncludedIndex - 1
		reply.XTerm = -1
		rf.resetElectionTimer()
		rf.mu.Unlock()
		return
	}
	if rf.logs[rf.indexToEntry(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.Success = false
		reply.XLen = len(rf.logs) + rf.lastIncludedIndex - 1
		reply.XTerm = rf.logs[rf.indexToEntry(args.PrevLogIndex)].Term
		for index, log := range rf.logs {
			if log.Term == reply.XTerm {
				reply.XIndex = index
				index += rf.lastIncludedIndex
				break
			}
		}
		rf.resetElectionTimer()
		rf.mu.Unlock()
		return
	}
	if args.PrevLogIndex < rf.lastIncludedIndex {
		alreadySnapshotLogLen := rf.lastIncludedIndex - args.PrevLogIndex
		if alreadySnapshotLogLen <= len(args.Entries) {
			newArgs := &AppendEntriesArgs{
				Term:         args.Term,
				LeaderId:     args.LeaderId,
				PrevLogTerm:  rf.lastIncludedTerm,
				PrevLogIndex: rf.lastIncludedIndex,
				Entries:      args.Entries[alreadySnapshotLogLen:],
				LeaderCommit: args.LeaderCommit,
			}
			args = newArgs
		} else {
			reply.Success = true
			return
		}
	}
	argIdx := 0
	presist := false
	for index := args.PrevLogIndex + 1; index <= rf.getLastLogIndex(); {
		if argIdx >= len(args.Entries) {
			break
		}
		if rf.logs[rf.indexToEntry(index)].Term != args.Entries[index-args.PrevLogIndex-1].Term {
			rf.logs = rf.logs[:rf.indexToEntry(index)]
			rf.logs = append(rf.logs, args.Entries[argIdx:]...)
			argIdx = len(args.Entries)
			presist = true
			break
		}
		index++
		argIdx++
	}
	if argIdx < len(args.Entries) {
		rf.logs = append(rf.logs, args.Entries[argIdx:]...)
		presist = true

	}
	if presist {
		rf.persist()
	}

	// cant guarantee, maybe out of if
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.getLastLogIndex())))
	}
	//DPrintf("Node %v log is %v", rf.me, rf.logs)

	rf.resetElectionTimer()
	reply.Success = true
	rf.mu.Unlock()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.setState(Follower, args.Term)
	rf.resetElectionTimer()

	if rf.lastIncludedTerm == args.LastIncludedTerm && rf.lastIncludedIndex == args.LastIncludeIndex {
		return
	}
	newLogs := make([]Entry, 0)
	newLogs = append(newLogs, Entry{Term: 0})
	if rf.getLastLogIndex() >= args.LastIncludeIndex {
		newLogs = append(newLogs, rf.logs[rf.indexToEntry(args.LastIncludeIndex)+1:]...)
	}
	rf.logs = newLogs
	rf.snapshot = make([]byte, 0)
	rf.snapshot = args.Data
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.lastIncludedIndex = args.LastIncludeIndex
	rf.commitIndex = int(math.Max(float64(args.LastIncludeIndex), float64(rf.commitIndex)))
	rf.lastApplied = int(math.Max(float64(args.LastIncludeIndex), float64(rf.lastApplied)))
	rf.persistAndSnapshot()

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludeIndex,
		}
	}()
}
