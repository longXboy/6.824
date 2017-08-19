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
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

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
	Index   int
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
	state         State
	leaderId      int
	currentTerm   int
	votedFor      int
	voteGrants    int
	electTimer    *time.Timer
	lastHeartBeat int64

	logs        []Log
	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg

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

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {

	if len(data) > 0 { // bootstrap without any state?
		r := bytes.NewBuffer(data)
		d := gob.NewDecoder(r)
		d.Decode(&rf.currentTerm)
		d.Decode(&rf.votedFor)
		d.Decode(&rf.commitIndex)
		d.Decode(&rf.lastApplied)
		d.Decode(&rf.logs)
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
	Term          int
	Success       bool
	PrevTermIndex int
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

func (rf *Raft) addEntries(preindex int, entries []Log) {
	total := preindex + len(entries)
	for i := preindex + 1; i <= total; i++ {
		if i == len(rf.logs) {
			rf.logs = append(rf.logs, entries[i-preindex-1])
		} else if i < len(rf.logs) {
			rf.logs[i] = entries[i-preindex-1]
		} else {
			panic(fmt.Errorf("add entries out of logs index"))
		}
	}
}

func (rf *Raft) RequestAppend(args *RequestAppendArgs, reply *RequestAppendReply) {
	if args.LeaderId == rf.me {
		panic(fmt.Errorf("leaderId should not be self!"))
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	isChanged := false
	reply.PrevTermIndex = -2
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		if reply.PrevTermIndex == 0 {
			fmt.Println("got reply.prevTermIndex==0")
		}
		return
	} else if args.Term == rf.currentTerm {
		if rf.state == StateLeader {
			panic(fmt.Errorf("two leaders in one term is forbidden!leaderId(%d) me(%d)", args.LeaderId, rf.me))
		} else if rf.state == StateCandidate || rf.state == StateFollower {
			rf.changeState(StateFollower)
			if rf.votedFor != args.LeaderId {
				isChanged = true
				rf.votedFor = args.LeaderId
			}
			rf.leaderId = args.LeaderId
		}
	} else if args.Term > rf.currentTerm {
		rf.changeState(StateFollower)
		rf.leaderId = args.LeaderId
		rf.votedFor = args.LeaderId
		rf.currentTerm = args.Term
		isChanged = true
	}
	reply.Term = rf.currentTerm
	if len(args.Entries) > 0 {
		if len(rf.logs) > args.PreLogIndex {
			if args.PreLogIndex == -1 || rf.logs[args.PreLogIndex].Term == args.PrevLogTerm {
				if args.PreLogIndex < rf.commitIndex {
					reply.Success = true
					panic(fmt.Errorf("%d committed command(%d,%d) can't be overwritten (%d,%d,%d) by %d", rf.me, rf.logs[rf.commitIndex].Term, rf.commitIndex, args.PrevLogTerm, args.PreLogIndex, len(args.Entries), args.LeaderId))
				} else {
					rf.addEntries(args.PreLogIndex, args.Entries)
					reply.Success = true
					isChanged = true
				}
			} else if args.PreLogIndex > 0 {
				aTerm := rf.logs[args.PreLogIndex].Term
				for i := args.PreLogIndex; i >= rf.commitIndex && i >= 0; i-- {
					if rf.logs[i].Term != aTerm {
						reply.PrevTermIndex = i
						break
					}
				}
				if reply.PrevTermIndex == -2 {
					reply.PrevTermIndex = rf.commitIndex
				}
				fmt.Printf("%d shrink to %d,term:%d\n", rf.me, reply.PrevTermIndex, rf.currentTerm)
			}
		} else {
			reply.PrevTermIndex = len(rf.logs) - 1
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		previous := rf.commitIndex
		if args.LeaderCommit < (len(rf.logs) - 1) {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.logs) - 1
		}
		fmt.Printf("follower(%d) commit:%d term:%d\n", rf.me, rf.commitIndex, rf.currentTerm)
		for i := previous + 1; i <= rf.commitIndex; i++ {
			rf.apply(i)
		}
		isChanged = true
	}
	if isChanged {
		rf.persist()
	}
	rf.lastHeartBeat = time.Now().UnixNano()
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
	//reject vote req without real timeout
	if time.Now().UnixNano()-rf.lastHeartBeat < int64(time.Millisecond*300) {
		return
	}
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
		rf.persist()
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
	rf.persist()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.logs)
	term := rf.currentTerm
	isLeader := false
	if rf.state == StateLeader {
		//fmt.Println("recv cmd:", command, "who:", rf.me, "term:", rf.currentTerm)
		isLeader = true
		if len(rf.logs) == 0 {
			rf.logs = []Log{Log{0, rf.currentTerm, command}}
		} else {
			lastIndex := rf.logs[len(rf.logs)-1].Index
			rf.logs = append(rf.logs, Log{lastIndex + 1, rf.currentTerm, command})
		}
		rf.persist()
	}
	// Your code here (2B).

	return index + 1, term, isLeader
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
	rf.applyCh = make(chan ApplyMsg, 10240)

	rf.readPersist(persister.ReadRaftState())
	rf.mu.Lock()
	rf.electTimer = time.AfterFunc(randDuration(), rf.electTimeOut)
	rf.mu.Unlock()

	go func() {
		for log := range rf.applyCh {
			applyCh <- log
		}
	}()
	return rf
}

func (rf *Raft) electTimeOut() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm++
	rf.changeState(StateCandidate)
	rf.persist()
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
	rf.mu.Lock()
	if isok {
		//fmt.Printf("(%d) req vote from server(%d) term:(%d)\n", rf.me, server, rf.currentTerm)
		if reply.Term > rf.currentTerm {
			if reply.VoteGranted {
				panic(fmt.Errorf("vote reply sholud not be success while reciver's term < sender's term"))
			}
			rf.currentTerm = reply.Term
			rf.changeState(StateFollower)
			rf.persist()
		} else if reply.VoteGranted && rf.state == StateCandidate && rf.currentTerm == req.Term {
			rf.voteGrants++
			if rf.voteGrants > len(rf.peers)/2 {
				rf.changeState(StateLeader)
				go rf.logAppend(rf.currentTerm)
			}
		}
	} else if rf.state == StateCandidate && rf.currentTerm == req.Term {
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 120)
		rf.SendVote(server, req)
		return
	}
	rf.mu.Unlock()
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
		fmt.Println("switch to leader:", rf.me, "term:", rf.currentTerm)
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

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			var req RequestAppendArgs
			rf.fillUpAppendReq(i, &req)
			go rf.sendRequestAppend(i, &req)
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 120)
	}
}

func (rf *Raft) apply(index int) {
	for i := rf.lastApplied; i <= index; i++ {
		//fmt.Println("apply:", i+1, "who:", rf.me, "term:", rf.currentTerm, "cmd:", rf.logs[i].Command)
		rf.applyCh <- ApplyMsg{
			Index:   i + 1,
			Command: rf.logs[i].Command,
		}
	}
	rf.lastApplied = index + 1
	rf.persist()
	return
}

func (rf *Raft) handleAppendReply(server int, req *RequestAppendArgs, reply *RequestAppendReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != StateLeader || req.Term != rf.currentTerm {
		return false
	} else if reply.Term > rf.currentTerm {
		if reply.Success {
			panic(fmt.Errorf("append reply sholud not be success while follower's term > leader's term"))
		}
		rf.currentTerm = reply.Term
		rf.changeState(StateFollower)
		rf.persist()
		return false
	}

	if reply.Success {
		if req.PreLogIndex+len(req.Entries) >= rf.nextIndex[server] {
			rf.nextIndex[server] = req.PreLogIndex + len(req.Entries) + 1
		}
		if req.PreLogIndex+len(req.Entries) > rf.matchIndex[server] {
			rf.matchIndex[server] = req.PreLogIndex + len(req.Entries)
			isChanged := false
			if rf.commitIndex < (len(rf.logs) - 1) {
				for cmi := (rf.commitIndex + 1); cmi < len(rf.logs); cmi++ {
					if rf.logs[cmi].Term != rf.currentTerm {
						//prevoius log can't be committed,should committed by append new
						continue
					}
					//current leader's log must contains the log
					var total int = 1
					for _, mi := range rf.matchIndex {
						if mi >= cmi {
							total++
						}
					}
					if total > len(rf.peers)/2 {
						rf.commitIndex = cmi
						fmt.Printf("ledaer(%d) commit:%d term:%d\n", rf.me, rf.commitIndex, rf.currentTerm)
						//fmt.Println("cmIdx:", cmi, "self:", rf.me, "term:", rf.currentTerm, "cmd:", rf.logs[cmi].Command)
						rf.apply(cmi)
						isChanged = true
					} else {
						break
					}
				}
				if isChanged {
					rf.persist()
				}
			}
		}
		return false
	} else {
		if len(req.Entries) > 0 && rf.nextIndex[server]-rf.matchIndex[server] > 1 && rf.nextIndex[server] > 0 {
			if reply.PrevTermIndex != -2 {
				//rf.nextIndex[server]--
				rf.nextIndex[server] = reply.PrevTermIndex + 1
				fmt.Printf("%d recv shrink to %d term %d ct: %d\n", rf.me, reply.PrevTermIndex, reply.Term, rf.currentTerm)
			} else {
				rf.nextIndex[server]--
				fmt.Printf("%d minus to %d term %d ct: %d \n", rf.me, rf.nextIndex[server]-1, reply.Term, rf.currentTerm)
			}
			rf.fillUpAppendReq(server, req)
			return true
		}
	}
	return false
}

func (rf *Raft) fillUpAppendReq(server int, req *RequestAppendArgs) {
	var entries []Log
	var preLogIndex int = -1
	var prevLogTerm int = -1
	if rf.nextIndex[server] < len(rf.logs) {
		temp := rf.logs[rf.nextIndex[server]:]
		entries = make([]Log, len(temp))
		copy(entries, temp)
		if rf.nextIndex[server] > 0 {
			preLogIndex = rf.nextIndex[server] - 1
			prevLogTerm = rf.logs[preLogIndex].Term
		}
	}
	commitId := rf.commitIndex
	if rf.matchIndex[server] < rf.commitIndex {
		commitId = rf.matchIndex[server]
	}
	req.Entries = entries
	req.Term = rf.currentTerm
	req.LeaderId = rf.me
	req.PreLogIndex = preLogIndex
	req.PrevLogTerm = prevLogTerm
	req.LeaderCommit = commitId
}

func (rf *Raft) sendRequestAppend(server int, req *RequestAppendArgs) {
	for {
		var reply RequestAppendReply
		ok := rf.peers[server].Call("Raft.RequestAppend", req, &reply)
		if ok {
			if !rf.handleAppendReply(server, req, &reply) {
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
