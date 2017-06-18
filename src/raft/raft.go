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
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type Log struct {
	Term    int
	Command interface{}
}
type State int

const (
	StateFollower State = iota
	StateCandidate
	StateLeader
)

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
	state       State
	leaderId    int
	currentTerm int
	votedFor    int
	voteGrants  int
	electTimer  *time.Timer

	logs        []Log
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
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
	if rf.state == StateLeader {
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
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

type RequestAppendArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type RequestAppendReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
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
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestAppend(args *RequestAppendArgs, reply *RequestAppendReply) {
	if args.LeaderId == rf.me {
		panic(fmt.Errorf("leaderId should not be self!"))
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	} else if args.Term == rf.currentTerm {
		if rf.state == StateLeader {
			panic(fmt.Errorf("two leaders in one term is forbidden!leaderId(%d) me(%d)", args.LeaderId, rf.me))
		} else if rf.state == StateCandidate || rf.state == StateFollower {
			rf.changeState(StateFollower)
			rf.votedFor = args.LeaderId
			rf.leaderId = args.LeaderId
		}
	} else if args.Term > rf.currentTerm {
		rf.changeState(StateFollower)
		rf.leaderId = args.LeaderId
		rf.votedFor = args.LeaderId
		rf.currentTerm = args.Term
	}
	if len(args.Entries) > 0 {
		if len(rf.logs) > args.PreLogIndex {
			if args.PreLogIndex == -1 || rf.logs[args.PreLogIndex].Term == args.PrevLogTerm {
				l := rf.logs[args.PreLogIndex+1:]
				l = append(l, args.Entries...)
				reply.Success = true
			}
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < (len(rf.logs) - 1) {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.logs) - 1
		}
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args.CandidateId == rf.me {
		panic(fmt.Errorf("candidateId should not be self"))
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	termChanged := false
	if args.Term < rf.currentTerm {
		return
	} else if args.Term == rf.currentTerm {
		if rf.votedFor != -1 {
			return
		}
	} else {
		rf.currentTerm = args.Term
		rf.changeState(StateFollower)
		termChanged = true
	}
	if len(rf.logs) != 0 {
		if rf.logs[len(rf.logs)-1].Term > args.LastLogTerm {
			return
		} else if rf.logs[len(rf.logs)-1].Term == args.LastLogTerm && len(rf.logs)-1 > args.LastLogIndex {
			return
		}
	}
	if !termChanged {
		rf.changeState(StateFollower)
	}
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
	/*if args.CandidateId == 0 {
		fmt.Printf("recv candiate 0 term:%d cTerm:%d self:%d\n", args.Term, rf.currentTerm, rf.me)
	}*/
	return
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
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
	// initialize from state persisted before a crash
	rf.logs = make([]Log, 0)
	rf.state = StateFollower
	rf.votedFor = -1
	rf.leaderId = -1
	rf.commitIndex = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.readPersist(persister.ReadRaftState())
	rf.electTimer = time.AfterFunc(randDuration(), rf.electTimeOut)

	return rf
}

func (rf *Raft) electTimeOut() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm++
	rf.changeState(StateCandidate)
	for i := range rf.peers {
		if i != rf.me {
			var index int
			var logTerm int
			if len(rf.logs) == 0 {
				index = -1
				logTerm = -1
			} else {
				index = len(rf.logs) - 1
				logTerm = rf.logs[index].Term
			}
			req := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: index,
				LastLogTerm:  logTerm,
			}
			go rf.SendVote(i, req)
		}
	}
}

func (rf *Raft) SendVote(server int, req RequestVoteArgs) {
	var reply RequestVoteReply
	isok := rf.sendRequestVote(server, &req, &reply)
	if isok {
		rf.mu.Lock()
		fmt.Printf("send_vote:req_term:%d current_term:%d candidateId:%d to_server:%d reply:%v isok:%v\n", req.Term, rf.currentTerm, req.CandidateId, server, reply, isok)
		if reply.Term > rf.currentTerm {
			if reply.VoteGranted {
				panic(fmt.Errorf("vote reply sholud not be success while reciver's term < sender's term"))
			}
			rf.currentTerm = reply.Term
			rf.changeState(StateFollower)
		} else if reply.VoteGranted && rf.state == StateCandidate && rf.currentTerm == req.Term {
			rf.voteGrants++
			fmt.Println(rf.voteGrants, len(rf.peers)/2)
			if rf.voteGrants > len(rf.peers)/2 {
				rf.changeState(StateLeader)
				fmt.Printf("%d change to leader term:%d\n", rf.me, rf.currentTerm)
				go rf.logAppend(rf.currentTerm)
			}
		}
		rf.mu.Unlock()
	} else if rf.state == StateCandidate && rf.currentTerm == req.Term {
		rf.mu.Lock()
		if rf.state == StateCandidate {
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 120)
			rf.SendVote(server, req)
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) changeState(state State) {
	if state == StateFollower {
		rf.electTimer.Reset(randDuration())
		rf.votedFor = -1
		rf.voteGrants = 0
		rf.leaderId = -1
	} else if state == StateCandidate {
		rf.electTimer.Reset(randDuration())
		rf.votedFor = rf.me
		rf.voteGrants = 1
		rf.leaderId = -1
	} else if state == StateLeader {
		if rf.votedFor != rf.me || rf.state != StateCandidate {
			panic(fmt.Errorf("can't switch to leader with invalid state voteFor(%d) me(%d) state(%d)", rf.votedFor, rf.me, rf.state))
		}
		rf.electTimer.Stop()
		rf.voteGrants = 0
		rf.leaderId = rf.me
		for i := range rf.peers {
			rf.nextIndex[i] = len(rf.logs)
			rf.matchIndex[i] = -1
		}
	}
	rf.state = state
}

func (rf *Raft) logAppend(term int) {
	for {
		rf.mu.Lock()
		if rf.state != StateLeader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		/*
			fmt.Println("leader:", rf.me)*/
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			var entries []Log
			var preLogIndex int = -1
			var prevLogTerm int = -1
			if rf.nextIndex[i] < len(rf.logs) {
				entries = rf.logs[rf.nextIndex[i]:]
				if rf.nextIndex[i] > 0 {
					preLogIndex = rf.nextIndex[i] - 1
					prevLogTerm = rf.logs[preLogIndex].Term
				}
			}
			req := RequestAppendArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PreLogIndex:  preLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			go rf.sendRequestAppend(i, req)
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 120)
	}
}

func (rf *Raft) handleAppendReply(server int, req *RequestAppendArgs, reply *RequestAppendReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != StateLeader {
		return false
	} else if reply.Term > rf.currentTerm {
		if reply.Success {
			panic(fmt.Errorf("append reply sholud not be success while follower's term > leader's term"))
		}
		rf.currentTerm = reply.Term
		rf.changeState(StateFollower)
		return false
	}

	if reply.Success {
		if req.PreLogIndex+len(req.Entries) >= rf.nextIndex[server] {
			rf.nextIndex[server] = req.PreLogIndex + len(req.Entries) + 1
		}
		if req.PreLogIndex+len(req.Entries) > rf.matchIndex[server] {
			rf.matchIndex[server] = req.PreLogIndex + len(req.Entries)

			if rf.commitIndex < (len(rf.logs) - 1) {
				for cmi := (rf.commitIndex + 1); cmi < len(rf.logs); cmi++ {
					var total int
					for _, mi := range rf.matchIndex {
						if mi >= cmi {
							total++
						}
					}
					if total > len(rf.peers)/2 {
						rf.commitIndex = cmi
					} else {
						break
					}
				}
			}
		}
		return false
	} else {
		if len(req.Entries) > 0 && rf.nextIndex[server]-rf.matchIndex[server] > 1 && rf.nextIndex[server] > 0 {
			rf.nextIndex[server]--
			var entries []Log
			var preLogIndex int = -1
			var prevLogTerm int = -1
			if rf.nextIndex[server] < len(rf.logs) {
				entries = rf.logs[rf.nextIndex[server]:]
				if rf.nextIndex[server] > 0 {
					preLogIndex = rf.nextIndex[server] - 1
					prevLogTerm = rf.logs[preLogIndex].Term
				}
			}
			req.Entries = entries
			req.Term = rf.currentTerm
			req.LeaderId = rf.me
			req.PreLogIndex = preLogIndex
			req.PrevLogTerm = prevLogTerm
			req.LeaderCommit = rf.commitIndex
			return true
		}
	}
	return false
}

func (rf *Raft) sendRequestAppend(server int, req RequestAppendArgs) {
	for {
		var reply RequestAppendReply
		ok := rf.peers[server].Call("Raft.RequestAppend", &req, &reply)
		if ok {
			if !rf.handleAppendReply(server, &req, &reply) {
				return
			}
		} else {
			return
		}
	}
}

func randDuration() time.Duration {
	tn := rand.Intn(350) + 350
	return time.Duration(int64(time.Millisecond) * int64(tn))
}
