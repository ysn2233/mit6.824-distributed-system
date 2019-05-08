package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Operation string
	Key       string
	Value     string
	Seq       int64
	ClientId  int64

	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu             sync.Mutex
	me             int
	rf             *raft.Raft
	applyCh        chan raft.ApplyMsg
	data           map[string]string
	clientSeqMap   map[int64]int64
	indexOpchanMap map[int]chan Op

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
}

func (kv *KVServer) ProcessOp(op Op) Err {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return ErrWrongLeader
	}
	kv.mu.Lock()
	if _, ok := kv.indexOpchanMap[index]; !ok {
		kv.indexOpchanMap[index] = make(chan Op)
	}
	appliedOpCh := kv.indexOpchanMap[index]
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.indexOpchanMap, index)
		kv.mu.Unlock()
	}()

	select {
	case appliedOp := <-appliedOpCh:
		if appliedOp == op {
			return OK
		} else {
			return OpNotConsistant
		}
	case <-time.After(2000 * time.Millisecond):
		return Timeout
	}
}

func (kv *KVServer) ApplyLoop() {
	for {
		select {
		case msg := <-kv.applyCh:
			idx := msg.CommandIndex
			op := msg.Command.(Op)
			kv.mu.Lock()
			if op.Seq > kv.clientSeqMap[op.ClientId] {
				// log.Printf("msg: %v, %v, %v", msg, op, kv.data)
				if op.Operation == "Put" {
					kv.data[op.Key] = op.Value
				} else if op.Operation == "Append" {
					if _, ok := kv.data[op.Key]; !ok {
						kv.data[op.Key] = op.Value
					} else {
						kv.data[op.Key] += op.Value
					}
				}
				kv.clientSeqMap[op.ClientId] = op.Seq
			}
			appliedOpCh, ok := kv.indexOpchanMap[idx]
			if ok {
				select {
				case <-appliedOpCh:
				default:
				}
				appliedOpCh <- op
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{Operation: "Get", Key: args.Key, Seq: args.Seq, ClientId: args.ClientId}
	err := kv.ProcessOp(op)
	reply.Err = err
	switch err {
	case ErrWrongLeader:
		reply.WrongLeader = true
	case Timeout:
		reply.WrongLeader = false
	case OpNotConsistant:
		reply.WrongLeader = false
	case OK:
		reply.WrongLeader = false
		reply.Value = kv.data[op.Key]
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{Operation: args.Op, Key: args.Key, Value: args.Value, Seq: args.Seq, ClientId: args.ClientId}
	err := kv.ProcessOp(op)
	reply.Err = err
	switch err {
	case ErrWrongLeader:
		reply.WrongLeader = true
	case Timeout:
		reply.WrongLeader = false
	case OpNotConsistant:
		reply.WrongLeader = false
	case OK:
		reply.WrongLeader = false
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
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
	kv.clientSeqMap = make(map[int64]int64)
	kv.indexOpchanMap = make(map[int]chan Op)
	// You may need initialization code here.
	go kv.ApplyLoop()

	return kv
}
