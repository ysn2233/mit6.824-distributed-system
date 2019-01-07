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

import (
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"time"
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
	chanCommit 		chan bool

	chanApply		chan ApplyMsg
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
	return rf.currentTerm, rf.state == LEADER
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
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.state = FOLLOWER
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastIdx, lastTerm := rf.LastLogIndexAndTerm()
		if args.LastLogTerm < lastTerm {
			return
		} else {
			if args.LastLogTerm == lastTerm && args.LastLogIndex < lastIdx {
				return
			} else {
				rf.mu.Lock()
				rf.votedFor = args.CandidateId
				log.Println(rf.me, "votedFor", rf.votedFor)
				rf.chanGrantVote <- true
				rf.mu.Unlock()
				reply.VoteGranted = true
				return
			}
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) broadcastRequestVote() {
	for i:=0; i<rf.n; i++ {
		if i == rf.me {
			continue
		}

		go func(i int) {
			lastIdx, lastTerm := rf.LastLogIndexAndTerm()
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			var reply RequestVoteReply

			if rf.state != CANDIDATE {
				return
			}
			ok := rf.sendRequestVote(i, &args, &reply)
			if ok {
				if reply.VoteGranted {
					rf.mu.Lock()
					rf.votedMe++
					if rf.state == CANDIDATE && rf.votedMe * 2 > rf.n {
						/** if not reset the state, the broadcast may be called for more than one times, which easily
							cause the deadlock.
						 */
						rf.becomeFollower()
						rf.chanVote <- true
					}
					rf.mu.Unlock()
				} else {
					if reply.Term > rf.currentTerm {
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.becomeFollower()
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
	Term         int
	Success      bool
	ConflictTerm int
	NewNextIndex int

}

func (rf *Raft) becomeFollower() {
	rf.state = FOLLOWER
	rf.votedFor = -1
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//log.Println(rf.me, "from", args.LeaderId, "selfLog", rf.log, "args", args, "on term", rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.chanHeartBeat <- true
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.becomeFollower()
	}
	lastIdx, _ := rf.LastLogIndexAndTerm()
	if args.PrevLogIndex > lastIdx {
		reply.NewNextIndex = lastIdx + 1
		reply.Success = false
		return
	} else {
		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			conflictTerm := rf.log[args.PrevLogIndex].Term
			reply.ConflictTerm = conflictTerm
			reply.NewNextIndex = 1
			for i:= args.PrevLogIndex; i>=0; i-- {
				if rf.log[i].Term != conflictTerm {
					reply.NewNextIndex = i + 1
				}
			}
			return
		}

		newStartIdx := args.PrevLogIndex + 1
		for i:= 0; i<= lastIdx-newStartIdx; i++ {
			if args.Entries != nil {
				if rf.log[args.PrevLogIndex + i].Term != args.Entries[i].Term {
					rf.log = rf.log[:newStartIdx+i]
					break
				}
			}
		}

		rf.log = append(rf.log, args.Entries...)

		if args.LeaderCommit > rf.commitIndex {
			lastEntryIndex := len(args.Entries) + args.PrevLogIndex
			if args.LeaderCommit < lastEntryIndex {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = lastEntryIndex
			}
			//log.Println(rf.me, rf.commitIndex)
			rf.chanCommit <- true
		}
		reply.Success = true
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
		go func(i int) {

			prevLogIndex := rf.nextIndex[i] - 1
			prevLogTerm := rf.log[prevLogIndex].Term
			entries := rf.log[rf.nextIndex[i]: ]
			args := AppendEntriesArgs{
				Term:			rf.currentTerm,
				LeaderId:		rf.me,
				PrevLogIndex: 	prevLogIndex,
				PrevLogTerm: 	prevLogTerm,
				Entries:		entries,
				LeaderCommit: 	rf.commitIndex,
			}

			//log.Println(rf.me, i, args, rf.log)

			var reply AppendEntriesReply
			ok := rf.sendAppendEntries(i, &args, &reply)
			if rf.state != LEADER {
				return
			}
			if ok {
				if reply.Success {
					rf.mu.Lock()
					rf.nextIndex[i] += len(entries)
					rf.matchIndex[i] = rf.nextIndex[i] - 1
					rf.mu.Unlock()
				} else {
					if rf.currentTerm < reply.Term {
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.becomeFollower()
						rf.mu.Unlock()
						return
					} else {
						rf.mu.Lock()
						rf.nextIndex[i] = reply.NewNextIndex
						rf.mu.Unlock()
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) CommitLogs() {
	newCommitIndex := rf.commitIndex
	for i:= rf.commitIndex + 1; i<len(rf.log); i++ {
		matchCount := 1
		for j := range rf.peers {
			if j == rf.me {
				continue
			}
			//log.Println(j, rf.matchIndex[j], i, rf.log[i].Term, rf.currentTerm)
			if rf.matchIndex[j] >= i && rf.log[i].Term == rf.currentTerm {
				matchCount++
			}
		}

		//log.Println(rf.me, rf.log[i])
		if matchCount* 2 > rf.n {
			newCommitIndex = i
		}
	}

	if newCommitIndex != rf.commitIndex {
		rf.mu.Lock()
		rf.commitIndex = newCommitIndex
		rf.chanCommit <- true
		rf.mu.Unlock()
	}
	//log.Println(rf.me, "commitIdx", rf.commitIndex)

}


func (rf *Raft) ApplyCommitted() {
	for {
		select {
		case <- rf.chanCommit:
			if rf.commitIndex > rf.lastApplied {
				for i:= rf.lastApplied+1; i<=rf.commitIndex; i++ {
					rf.mu.Lock()
					applyMsg := ApplyMsg{CommandValid:true, Command:rf.log[i].Command, CommandIndex:i}
					//log.Println(rf.me, applyMsg)
					rf.chanApply <- applyMsg
					rf.lastApplied = i
					rf.mu.Unlock()
				}
			}
		}
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
	term := rf.currentTerm
	isLeader := rf.state == LEADER
	logEntry := LogEntry{Term:term, Command:command}
	//log.Println(rf.me, "start", command, rf.state)
	if isLeader {
		index = len(rf.log)
		rf.log = append(rf.log, logEntry)
	}


	return index, term, rf.state == LEADER
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
	//log.Println(rf.me, "follower job", rf.votedFor)
	select {
		case <- time.After(time.Duration(400 + rand.Intn(300)) * time.Millisecond):
			rf.mu.Lock()
			rf.state = CANDIDATE
			rf.mu.Unlock()
		case <- rf.chanHeartBeat:
		case <- rf.chanGrantVote:
	}
}

func (rf *Raft) CandidateJob() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votedMe = 1
	rf.mu.Unlock()
	go rf.broadcastRequestVote()

	select {
		case <- time.After(time.Duration(400 + rand.Intn(300)) * time.Millisecond):
			rf.becomeFollower()
		case <- rf.chanHeartBeat:
			rf.becomeFollower()
		case <- rf.chanVote:
			rf.mu.Lock()
			log.Println("term", rf.currentTerm, rf.me, "become leader")
			rf.state = LEADER
			rf.nextIndex = make([]int, rf.n)
			rf.matchIndex = make([]int, rf.n)
			lastIdx, _ := rf.LastLogIndexAndTerm()
			for i:=0; i<rf.n; i++ {
				rf.nextIndex[i] = lastIdx + 1
				rf.matchIndex[i] = 0
			}
			rf.mu.Unlock()
	}
}

func (rf *Raft) LeaderJob() {
	rf.CommitLogs()
	go rf.broadcastAppendEntries()
	//log.Println(rf.me, "send hb")
	time.Sleep(100 * time.Millisecond)
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
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term:0})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, rf.n)
	rf.matchIndex = make([]int, rf.n)

	rf.chanVote = make(chan bool)
	rf.chanHeartBeat = make(chan bool, 100)
	rf.chanGrantVote = make(chan bool)
	rf.chanCommit = make(chan bool)

	rf.chanApply = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.RaftLoop()
	go rf.ApplyCommitted()


	return rf
}
