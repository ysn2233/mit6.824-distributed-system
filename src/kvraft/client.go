package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"sync"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	id      int64
	mu      sync.Mutex
	seq     int64
	leader  int
	// You will have to modify this struct.
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
	ck.id = nrand()
	ck.leader = 0
	ck.seq = 0
	// You'll have to add code here.
	return ck
}

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
	i := ck.leader
	ck.mu.Lock()
	ck.seq++
	ck.mu.Unlock()
	args := GetArgs{
		Key:      key,
		Seq:      ck.seq,
		ClientId: ck.id,
	}
	for {
		var reply GetReply
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok {
			if !reply.WrongLeader && reply.Err == OK {
				ck.mu.Lock()
				ck.leader = i
				ck.mu.Unlock()
				return reply.Value
			}
			i = (i + 1) % len(ck.servers)
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
	i := ck.leader
	ck.mu.Lock()
	ck.seq++
	ck.mu.Unlock()
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		Seq:      ck.seq,
		ClientId: ck.id,
	}
	for {
		var reply PutAppendReply
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		// log.Println(i, ok)
		if ok {
			// log.Println(reply.Err)
			if !reply.WrongLeader && reply.Err == OK {
				ck.mu.Lock()
				ck.leader = i
				ck.mu.Unlock()
				return
			}
		}
		i = (i + 1) % len(ck.servers)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
