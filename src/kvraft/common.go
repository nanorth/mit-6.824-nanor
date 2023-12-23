package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrIllegalOp   = "ErrIllegalOp"
)

type Err string

type KVRequest struct {
	Command Command
}

type KVReply struct {
	Status   Err
	Response string
}

type Command struct {
	Op          string
	Key         string
	Value       string
	ClientID    int64
	SequenceNum int
}
