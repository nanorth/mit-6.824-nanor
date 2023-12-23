package kvraft

//type KVStateMachine interface {
//	Get(key string) (string, Err)
//	Put(key, value string) Err
//	Append(key, value string) Err
//}

type MemoryKV struct {
	KV map[string]string
}

//func NewMemoryKV() *MemoryKV {
//	return &MemoryKV{make(map[string]string)}
//}

func (memoryKV *MemoryKV) Get(key string) (string, Err) {
	if value, ok := memoryKV.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (memoryKV *MemoryKV) Put(key, value string) Err {
	memoryKV.KV[key] = value
	return OK
}

func (memoryKV *MemoryKV) Append(key, value string) Err {
	memoryKV.KV[key] += value
	return OK
}

func (memoryKV *MemoryKV) doCommand(command Command) (string, Err) {
	switch command.Op {
	case "Get":
		return memoryKV.Get(command.Key)
	case "Put":
		return "", memoryKV.Put(command.Key, command.Value)
	case "Append":
		return "", memoryKV.Append(command.Key, command.Value)
	}
	return "", ErrIllegalOp
}
