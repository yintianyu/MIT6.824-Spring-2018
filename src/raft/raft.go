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
	"labrpc"
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
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       int      // state machine : 0 - leader 1 - follwer 2 - candidate
	currentTerm int      // lastest term server has seen(initialized to 0)
	votedFor    int      // candidateId that received vote in current term(or null if none)
	log         []string // log entries; each entry contains command for state machine, and term when entry was recieved by leader
	commitIndex int      // index of highest log entry known to be commited(initialized to 0)
	lastApplied int      // index of highest log entry applied to state machine(initialized to 0)

	voteTimer *time.Timer // vote timer
	mux       sync.Mutex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mux.Lock()
	term = rf.currentTerm
	isleader = rf.state == 0
	rf.mux.Unlock()
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
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidates's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example AppendEntries RPC arguments structure
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	// Your data here.
	Term         int      // leader's term
	LeaderId     int      // so follower can redirect clients
	PrevLogIndex int      // index of log entry immediately preceding new ones
	PrevLogTerm  int      // term of prevLogIndex entry
	Entries      []string // log entries to store(empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int      // leader's commitIndex
}

//
// example AppendEntries RPC reply structure
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	// Your data here.
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mux.Lock()
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = 1
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.voteTimerReset()
		DPrintf("[Server %d] Voted to %d at Term %d\n", rf.me, rf.votedFor, rf.currentTerm)
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("[Server %d] Reject %d at Term %d\n", rf.me, args.CandidateId, rf.currentTerm)
	}
	rf.mux.Unlock()
}

//
// example AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.
	rf.mux.Lock()
	if rf.currentTerm <= args.Term {
		rf.currentTerm = args.Term
		reply.Term = args.Term
		reply.Success = true
		rf.state = 1
		DPrintf("[Server %d] in appendEntries handler SUCCESS at Term %d\n", rf.me, args.Term)
		rf.mux.Unlock()
		rf.voteTimerReset()
	} else {
		reply.Success = false
		reply.Term = rf.currentTerm
		DPrintf("[Server %d] in appendEntries handler FAIL at Term %d\n", rf.me, reply.Term)
		rf.mux.Unlock()
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// example code to send a AppendEntries RPC to a server.
//
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

// vote timer check
func (rf *Raft) voteTimerCheck() {
	for {
		<-rf.voteTimer.C
		rf.mux.Lock()
		if rf.state != 0 {
			DPrintf("[Server %d] vote Timer expires at Term %d\n", rf.me, rf.currentTerm)
			rf.state = 2 // turn to candidate
			rf.mux.Unlock()
			go rf.issueVote()
		} else {
			DPrintf("[Server %d] vote Timer expires at Term %d, but a leader\n", rf.me, rf.currentTerm)
			rf.mux.Unlock()
		}
		rf.voteTimerReset()
	}
}

// vote timer Reset
func (rf *Raft) voteTimerReset() {
	if !rf.voteTimer.Stop() && len(rf.voteTimer.C) > 0 {
		<-rf.voteTimer.C
	}
	d := rand.Int31n(250) + 400
	rf.voteTimer.Reset(time.Duration(d) * time.Millisecond)
}

//
// issue a vote
//
func (rf *Raft) issueVote() {
	rf.voteTimerReset()
	rf.mux.Lock()
	rf.currentTerm++
	DPrintf("[Server %d] issues a vote at Term %d\n", rf.me, rf.currentTerm)
	var args RequestVoteArgs
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	rf.votedFor = rf.me
	rf.mux.Unlock()
	count := 1
	voteMeChan := make(chan bool)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(i int) { // send requestVote to another
				var reply RequestVoteReply
				ok := rf.sendRequestVote(i, &args, &reply)
				if !ok {
					voteMeChan <- false
					DPrintf("[Server %d] requestVote to %d return failed at Term %d, granted = %v\n", rf.me, i, rf.currentTerm, reply.VoteGranted)
				} else {
					if reply.VoteGranted {
						voteMeChan <- true
						DPrintf("[Server %d] %d voted to me at Term %d\n", rf.me, i, reply.Term)
					} else {
						voteMeChan <- false
						rf.mux.Lock()
						DPrintf("[Server %d] %d didn't vote to me at Term %d\n", rf.me, i, reply.Term)
						if reply.Term > rf.currentTerm {
							DPrintf("[Server %d] Term is corrected by %d during election\n", rf.me, i)
							rf.currentTerm = reply.Term
							rf.state = 1
						}
						rf.mux.Unlock()
					}
				}
			}(i)
		}
	}
	for i := 0; i < len(rf.peers)-1; i++ {
		voteMe := <-voteMeChan
		DPrintf("[Server %d] got vote No.%d, %v\n", rf.me, i, voteMe)
		if voteMe {
			count++
		}
		if count > len(rf.peers)/2 {
			break
		}
	}
	DPrintf("[Server %d] count = %d at Term %d BEFORE LOCK\n", rf.me, count, rf.currentTerm)
	rf.mux.Lock()
	DPrintf("[Server %d] count = %d at Term %d AFTER LOCK\n", rf.me, count, rf.currentTerm)
	if rf.state == 2 && count > len(rf.peers)/2 {
		rf.state = 0 // wins
		DPrintf("[Server %d] becomes leader at Term %d\n", rf.me, rf.currentTerm)
	} else if rf.state == 2 {
		rf.state = 1 // fails
		DPrintf("[Server %d] fails the election at Term %d\n", rf.me, rf.currentTerm)
	}
	rf.mux.Unlock()
}

//
// Leader HeartBeat
//
func (rf *Raft) leaderHeartbeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(i int) {
				var args AppendEntriesArgs
				var reply AppendEntriesReply
				rf.mux.Lock()
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				rf.mux.Unlock()
				ok := rf.sendAppendEntries(i, &args, &reply)
				if ok {
					rf.mux.Lock()
					if reply.Term > rf.currentTerm {
						DPrintf("[Server %d] Term %d -> %d during heartbeat, convert to follower\n", rf.me, rf.currentTerm, reply.Term)
						rf.currentTerm = reply.Term
						rf.state = 1
					}
					rf.mux.Unlock()
				}
			}(i)
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

	// Your initialization code here (2A, 2B, 2C).
	rf.state = 1 // initially follower
	rf.currentTerm = 0

	d := rand.Int31n(150) + 500
	rf.voteTimer = time.NewTimer(time.Duration(d) * time.Millisecond)

	go rf.voteTimerCheck() // vote timer checker
	// behavior
	go func() {
		for {
			rf.mux.Lock()
			if rf.state == 2 {
				rf.mux.Unlock()
				// rf.issueVote()
			} else if rf.state == 0 {
				rf.mux.Unlock()
				time.Sleep(time.Duration(150) * time.Millisecond)
				rf.mux.Lock()
				if rf.state == 0 {
					rf.mux.Unlock()
					rf.leaderHeartbeat()
				} else {
					rf.mux.Unlock()
				}
				// TODO: Send AppendEntries
			} else {
				rf.mux.Unlock()
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
