package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	"time"
	"math/rand"
	"log"
)

// import "bytes"
// import "labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//


const (
	FOLLOWER = 0
	CANDIDATE = 1
	LEADER = 2
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
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

	n 			int
	state		int
	currentTerm	int
	votedFor	int
	votedMe		int
	log			[]LogEntry

	commitIndex	int
	lastApplied	int

	nextIndex	[]int
	matchIndex	[]int

	chanHeartBeat 	chan bool
	chanVote 		chan bool
	chanGrantVote	chan bool
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) LastLogIndexAndTerm() (int, int) {
	last := len(rf.log) - 1
	return last, rf.log[last].Term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {

	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int

}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {

	Term			int
	VoteGranted		bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.mu.Unlock()
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastIdx, lastTerm := rf.LastLogIndexAndTerm()
		if args.LastLogIndex < lastIdx {
			reply.VoteGranted = false
			return
		} else if args.LastLogIndex == lastIdx {
			if args.LastLogTerm < lastTerm {
				reply.VoteGranted = false
				return
			} else {
				rf.mu.Lock()
				log.Println("votedFor", rf.votedFor)
				rf.votedFor = args.CandidateId
				rf.chanGrantVote <- true
				rf.mu.Unlock()
				reply.VoteGranted = true
				return
			}
		}
	}
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
func (rf *Raft) SendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) BroadcastRequestVote() {
	for i:=0; i<rf.n; i++ {
		if i == rf.me {
			continue
		}

		go func(i int) {
			if rf.state != CANDIDATE {
				return
			}
			lIdx, lTerm := rf.LastLogIndexAndTerm()
			args := RequestVoteArgs{
				Term:			rf.currentTerm,
				CandidateId:	rf.me,
				LastLogIndex:	lIdx,
				LastLogTerm:	lTerm,
			}
			var reply RequestVoteReply

			ok := rf.SendRequestVote(i, &args, &reply)
			if ok {
				if rf.currentTerm != reply.Term {
					return
				}
				if reply.VoteGranted {
					rf.mu.Lock()
					rf.votedMe++
					log.Println(rf.me, rf.votedMe, rf.n)
					if rf.state == CANDIDATE && rf.votedMe > rf.n / 2 {
						log.Println(rf.me, "not voted yet")
						rf.chanVote <- true
						log.Println(rf.me, "voted")
					}
					rf.mu.Unlock()
				} else {
					if reply.Term > rf.currentTerm {
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.state = FOLLOWER
						rf.mu.Unlock()
						return
					}
				}
			}
		}(i)
	}
}

type AppendEntriesArgs struct {
	Term			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries			[]LogEntry
	LeaderCommit 	int
}

type AppendEntriesReply struct {
	Term 			int
	Success			bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	rf.mu.Unlock()
	rf.chanHeartBeat <- true
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) broadcastAppendEntries() {
	for i:=0; i<rf.n; i++ {
		if i == rf.me {
			continue
		}
		go func() {
			var args AppendEntriesArgs
			var reply AppendEntriesReply
			ok := rf.sendAppendEntries(i, &args, &reply)
			if ok {
				if rf.currentTerm < reply.Term {
					rf.mu.Lock()
					log.Println(rf.me, "become follower")
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.state = FOLLOWER
					rf.mu.Unlock()
				}
			}
		}()
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) FollowerJob() {
	select {
		case <- rf.chanHeartBeat:
		case <- rf.chanGrantVote:
		case <- time.After(time.Duration(400 + rand.Intn(500)) * time.Millisecond):
			log.Println("term", rf.currentTerm, rf.me, "become candidate")
			rf.mu.Lock()
			rf.state = CANDIDATE
			rf.mu.Unlock()
	}
}

func (rf *Raft) CandidateJob() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm ++
	rf.votedFor = rf.me
	rf.votedMe = 1
	go rf.BroadcastRequestVote()

	select {
		case <- rf.chanHeartBeat:
			log.Println("term", rf.currentTerm, rf.me, "back to follower")
			rf.state = FOLLOWER
		case <- rf.chanVote:
			log.Println("term", rf.currentTerm, rf.me, "become leader")
			rf.state = LEADER
		case <- time.After(time.Duration(500 + rand.Intn(300)) * time.Millisecond):
	}
}

func (rf *Raft) LeaderJob() {
	go rf.broadcastAppendEntries()
	time.Sleep(70 * time.Millisecond)
}

func (rf *Raft) RaftLoop() {
	for {
		switch  rf.state {
		case FOLLOWER:
			rf.FollowerJob()
		case CANDIDATE:
			rf.CandidateJob()
		case LEADER:
			rf.LeaderJob()
		}

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

	rf.n = len(rf.peers)
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.votedMe = 0
	rf.log = make([]LogEntry, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)

	rf.chanVote = make(chan bool)
	rf.chanHeartBeat = make(chan bool)
	rf.chanGrantVote = make(chan bool)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.RaftLoop()


	return rf
}
