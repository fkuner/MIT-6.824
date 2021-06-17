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
	//"bytes"
	//"labgob"
	//"labrpc"
	"math"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

import "bytes"
import "../labgob"

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
	dead      int32               // set by Kill()

	// Server state
	state State

	// peers' number
	peerNum int

	// last time
	lastTime time.Time

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	// Updated on stable storage before responding to RPCs
	currentTerm int     // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int     // candidateId that received vote in current term (or null if none)
	log         []Entry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders:
	// Reinitialized after election
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

type State int

const (
	FOLLOWER  State = 0
	CANDIDATE State = 1
	LEADER    State = 2
)

//
// information about each log entry
//
type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state == LEADER {
		isleader = true
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	DPrintf("[%d] readPersist", rf.me)
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Entry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		DPrintf("Decode Error")
	} else {
	  rf.currentTerm = currentTerm
	  rf.votedFor = votedFor
	  rf.log = log
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
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
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		logNew(args.LastLogIndex, args.LastLogTerm, lastLogIndex, lastLogTerm) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.resetLastTime()
		return
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
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

type AppendEntriesArgs struct {
	Term         int     // leader's term
	LeaderId     int     // so follower can redirect clients
	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of preLogIndex entry
	Entries      []Entry // log entries to store (empty for heartbeat); may send more than one for efficiency
	LeaderCommit int     // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	ConflictTerm int
	ConflictIndex int
	Success bool // true is follower contained entry matching prevLogIndex and preLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		rf.state = FOLLOWER
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	rf.currentTerm = args.Term
	rf.persist()
	rf.lastTime = time.Now()
	rf.state = FOLLOWER

	reply.Term = args.Term

	if len(rf.log) <= args.PrevLogIndex {
		reply.ConflictTerm = -1
		reply.ConflictIndex = len(rf.log)
		reply.Success = false
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		index := args.PrevLogIndex - 1
		for ; index > 0; index-- {
			if rf.log[index].Term != rf.log[args.PrevLogIndex].Term {
				break
			}
		}
		reply.ConflictIndex = index + 1
		reply.Success = false
		return
	}

	if args.Entries != nil {
		entry := args.Entries[0]
		if len(rf.log) > entry.Index && rf.log[entry.Index].Term != entry.Term {
			rf.log = rf.log[:entry.Index]
		}
		for _, entry := range args.Entries {
			flag := 0
			for _, e := range rf.log {
				if e == entry {
					flag = 1
					break
				}
			}
			if flag == 0 {
				rf.log = append(rf.log, entry)
				DPrintf("[%d] append 1 entry %v", rf.me, entry.Command)
			}
		}
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
	}
	reply.Success = true
	return
}

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.state == LEADER
	if isLeader {
		rf.log = append(rf.log, Entry{Index: len(rf.log), Term: rf.currentTerm, Command: command})
		rf.nextIndex[rf.me] = len(rf.log)
		rf.matchIndex[rf.me] = len(rf.log) - 1
		DPrintf("[%d] command:%v", rf.me, command)
	}
	rf.persist()
	index := len(rf.log) - 1

	return  index, term, isLeader
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

func (rf *Raft) initRaft() {
	rf.state = FOLLOWER
	rf.currentTerm = 1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor = -1
	rf.log = append(rf.log, Entry{Index: 0, Term: 0})
	rf.peerNum = len(rf.peers)
	rf.lastTime = time.Now()
	for i := 0; i < rf.peerNum; i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
}

// Reinitialized after election
func (rf *Raft) initIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := 0; i < rf.peerNum; i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	rf.persist()
}

func (rf *Raft) resetLastTime() {
	rf.lastTime = time.Now()
}

// CallRequestVote Call SendRequestVote to server and term
func (rf *Raft) CallRequestVote(server int, term int) bool {
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &args, &reply)
	if !ok {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.state = FOLLOWER
	}
	return reply.VoteGranted
}

// AttemptElection Kick off Election
func (rf *Raft) ExecuteElection() {
	rf.mu.Lock()
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	// Reset election timer
	rf.lastTime = time.Now()
	DPrintf("[%d] attempting an election at term %d", rf.me, rf.currentTerm)
	votes := 1
	done := false
	term := rf.currentTerm
	rf.mu.Unlock()
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			voteGranted := rf.CallRequestVote(server, term)
			if !voteGranted {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			votes++
			DPrintf("[%d] got vote from %d", rf.me, server)
			if done || votes <= rf.peerNum/2 {
				return
			}
			done = true
			DPrintf("[%d] we got enough votes, we are now the leader (currentTerm=%d)!", rf.me, rf.currentTerm)
			rf.state = LEADER
			go rf.ExecuteLeaderAction()
			return
		}(server)
	}
}

// ExecuteLeaderAction ...
func (rf *Raft) ExecuteLeaderAction() {
	rf.initIndex()
	rf.Start("no-op")
	for {
		rf.mu.Lock()
		if rf.state != LEADER {
			rf.mu.Unlock()
			return
		} else {
			rf.mu.Unlock()
		}
		DPrintf("[%d] execute leader action", rf.me)
		rf.SendHeartBeat()
		rf.mu.Lock()
		for i := rf.commitIndex + 1; i < len(rf.log); i++ {
			match := 0
			for j:= 0; j < rf.peerNum; j++{
				if rf.matchIndex[j] >= i && rf.log[i].Term == rf.currentTerm {
					match++
				}
			}
			if match > rf.peerNum / 2 {
				rf.commitIndex = i
			}
		}
		rf.mu.Unlock()
		// heartbeat time
		time.Sleep(200 * time.Millisecond)
	}
}

func (rf* Raft) SendHeartBeat() {
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		currentTerm := rf.currentTerm
		go func(server int) {
			rf.mu.Lock()
			var preLog Entry
			if rf.nextIndex[server] > 1 {
				preLog = rf.log[rf.nextIndex[server] - 1]
			}
			entries := make([]Entry, len(rf.log) - 1 - preLog.Index)
			copy(entries, rf.log[preLog.Index + 1:])
			args := AppendEntriesArgs{
				Term: currentTerm,
				LeaderId: rf.me,
				PrevLogIndex: preLog.Index,
				PrevLogTerm: preLog.Term,
				Entries: entries,
				LeaderCommit: rf.commitIndex,
			}
			DPrintf("[%d] args:%v %d----->%d", rf.me, args, rf.me, server)
			reply := AppendEntriesReply{}
			rf.mu.Unlock()
			ok := rf.sendAppendEntries(server, &args, &reply)
			DPrintf("[%d] reply:%v %d----->%d", rf.me, reply, server, rf.me)
			if !ok {
				DPrintf("[%d] ok == false", rf.me)
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Success {
				if rf.nextIndex[server] + len(entries) > len(rf.log) {
					return
				}
				rf.nextIndex[server] = rf.nextIndex[server] + len(entries)
				rf.matchIndex[server] = preLog.Index
			} else {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = FOLLOWER
					rf.persist()
					return
				} else {
					i := len(rf.log) - 1
					for ; i > 0; i-- {
						if rf.log[i].Term == reply.ConflictTerm {
							break
						}
					}
					if i > 0 {
						rf.nextIndex[server] = i + 1
					} else {
						rf.nextIndex[server] = reply.ConflictIndex
					}
				}
			}
		}(server)
	}
}

func (rf *Raft) Apply(applyCh chan ApplyMsg) {
	for {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			commandValid := true
			//if rf.log[rf.lastApplied].Command == "no-op" {
			//	DPrintf("test")
			//	commandValid = false
			//}
			applyCh <- ApplyMsg{
				CommandValid: commandValid,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.log[rf.lastApplied].Index,
			}
			DPrintf("[%d] apply command %v", rf.me, rf.log[rf.lastApplied].Command)
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) RaftInfo(funcName string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v=====%d===============", funcName, rf.me)
	DPrintf("State:%v", stateInfo(rf.state))
	DPrintf("CurrentTerm:%d", rf.currentTerm)
	DPrintf("votedFor:%d", rf.votedFor)
	DPrintf("log:")
	for _, entry := range rf.log {
		DPrintf("\t[term:%d, command:%v]", entry.Term, entry.Command)
	}
	DPrintf("commitIndex:%d", rf.commitIndex)
	DPrintf("lastApplied:%d", rf.lastApplied)
	DPrintf("nextIndex:")
	for _, index := range rf.nextIndex {
		DPrintf("\tindex:%d", index)
	}
	DPrintf("matchIndex:")
	for _, index := range rf.matchIndex {
		DPrintf("\tindex:%d", index)
	}
	DPrintf("%v=====%d===============", funcName, rf.me)
	DPrintf("")
}

func stateInfo(state State) string {
	var res string
	switch state {
	case 0:
		res = "Follower"
	case 1:
		res = "Candidate"
	case 2:
		res = "Leader"
	}
	return res
}

// 判断log1至少log2更新
func logNew(log1LastIndex int, log1LastTerm int, log2LastIndex int, log2LastTerm int) bool {
	if log1LastTerm > log2LastTerm {
		return true
	} else if (log1LastTerm == log2LastTerm) && (log1LastIndex >= log2LastIndex) {
		return true
	} else {
		return false
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
	rf.initRaft()

	// background goroutine that will kick off leader election periodically
	go func() {
		for {
			rf.mu.Lock()
			if rf.state == LEADER {
				rf.mu.Unlock()
				time.Sleep(100 * time.Millisecond)
				continue
			} else {
				rf.mu.Unlock()
			}
			rand.Seed(int64(rf.me) * time.Now().UnixNano())
			timeout := rand.Intn(300) + 300
			duration := time.Duration(timeout) * time.Millisecond
			time.Sleep(duration)
			rf.mu.Lock()
			if rf.state == LEADER {
				rf.mu.Unlock()
				time.Sleep(100 * time.Millisecond)
				continue
			} else {
				rf.mu.Unlock()
			}
			// Kick off election
			if rf.lastTime.Add(duration).Before(time.Now()) {
				DPrintf("[%d] election timeout", rf.me)
				rf.ExecuteElection()
				rf.mu.Lock()
				rf.lastTime = time.Now()
				rf.mu.Unlock()
			}
		}
	}()

	go rf.Apply(applyCh)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.RaftInfo("Make")

	return rf
}
