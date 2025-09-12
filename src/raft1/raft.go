package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// LogEntry is the log record carried by Raft. Fields must be exported for RPC.
type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type role int

const (
	follower role = iota
	candidate
	leader
)

func (r role) String() string {
	switch r {
	case follower:
		return "Follower"
	case candidate:
		return "Candidate"
	case leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// A Go object implementing a single Raft peer.
// 封装一个节点的全部状态和行为
type Raft struct {
	//并发与外部接口
	mu        sync.Mutex            // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd   // RPC end points of all peers
	persister *tester.Persister     // Object to hold this peer's persisted state
	me        int                   // this peer's index into peers[]
	dead      int32                 // set by Kill()
	applyCh   chan raftapi.ApplyMsg // channel for sending ApplyMsgs to applyCh

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 持久化状态
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry // 日志

	// 瞬态状态
	CommitIndex int
	LastApplied int
	Role        string // 字符串仅用于对外显示
	role        role   // 内部使用枚举，便于比较

	// 选举/心跳时序
	lastHeard         time.Time     // 上次收到心跳/授票时间
	lastHeartbeatSent time.Time     // leader 上次发送心跳时间
	heartbeatInterval time.Duration // 心跳间隔（需 <= 10 次/秒）

	//leader专属瞬时状态
	NextIndex  []int
	MatchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
// 返回当前任期号和是否是领导人
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.role == leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
// 请求投票的参数
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // 候选人的任期号
	CandidateId  int // 候选人的ID
	LastLogIndex int // 候选人的最后日志索引
	LastLogTerm  int // 候选人的最后日志任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
// 请求投票的回复
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // 当前任期号，用于领导人去更新自己
	VoteGranted bool // 候选人是否赢得了选票
}

// example RequestVote RPC handler.
// 请求投票的处理器，处理reply
// args：由发起选举的候选人创建并发送给接收者的请求投票参数
// reply：由接收者创建并发送给发起者的请求投票回复
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	//拒绝投票
	// 如果候选人的任期号小于接收者的任期号，则拒绝投票
	if args.Term < rf.CurrentTerm {
		return
	}
	// 如果候选人的任期号大于接收者的任期号，则更新接收者的任期号，转为follower
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.Role = "Follower"
		rf.VotedFor = -1
		rf.persist()
	}

	// “日志不落后”
	myLastIndex := rf.lastLogIndex()
	myLastTerm := rf.lastLogTerm()
	upToDate := args.LastLogTerm > myLastTerm ||
		(args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex)

	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && upToDate {
		rf.VotedFor = args.CandidateId
		rf.persist()
		rf.resetElectionTimer()
		reply.VoteGranted = true
	}

	reply.Term = rf.CurrentTerm
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries (heartbeat/log replication) RPC
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm
	reply.Success = false

	if args.Term < rf.CurrentTerm {
		return
	}

	// newer term or equal -> follower, accept heartbeat
	if args.Term >= rf.CurrentTerm {
		if args.Term > rf.CurrentTerm {
			rf.CurrentTerm = args.Term
			rf.VotedFor = -1
		}
		rf.role = follower
		rf.Role = rf.role.String()
		rf.resetElectionTimer()
		reply.Term = rf.CurrentTerm
		reply.Success = true // 3A: 不做日志匹配检查
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Heartbeat or election timeout handling
		rf.mu.Lock()
		now := time.Now()
		// election timeout randomized per loop
		electionTimeout := 400*time.Millisecond + time.Duration(rand.Int63()%400)*time.Millisecond
		needElection := now.Sub(rf.lastHeard) >= electionTimeout && rf.role != leader
		needHeartbeat := rf.role == leader && now.Sub(rf.lastHeartbeatSent) >= rf.heartbeatInterval
		rf.mu.Unlock()

		if needElection {
			rf.startElection()
			continue
		}
		if needHeartbeat {
			rf.sendHeartbeats()
			continue
		}

		// sleep a bit to avoid busy loop
		time.Sleep(20 * time.Millisecond)
	}
}

// 开始选举
func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.role == leader {
		rf.mu.Unlock()
		return
	}
	rf.role = candidate
	rf.Role = rf.role.String()
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.persist()
	termStarted := rf.CurrentTerm
	rf.resetElectionTimer()
	lastIndex := rf.lastLogIndex()
	lastTerm := rf.lastLogTerm()
	nPeers := len(rf.peers)
	votes := 1 // vote for self
	rf.mu.Unlock()

	//并行发送请求投票
	for i := 0; i < nPeers; i++ {
		if i == rf.me {
			continue
		}
		go func(server int, term, lastIdx, lastTm int) {
			args := &RequestVoteArgs{Term: term, CandidateId: rf.me, LastLogIndex: lastIdx, LastLogTerm: lastTm}
			var reply RequestVoteReply
			if rf.sendRequestVote(server, args, &reply) {
				//reply:接收RequestVote的回复
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if term != rf.CurrentTerm {
					return
				}
				if reply.Term > rf.CurrentTerm {
					rf.CurrentTerm = reply.Term
					rf.VotedFor = -1
					rf.role = follower
					rf.Role = rf.role.String()
					rf.persist()
					return
				}
				if reply.VoteGranted && rf.role == candidate && rf.CurrentTerm == termStarted {
					votes++
					if votes > nPeers/2 {
						rf.role = leader
						rf.Role = rf.role.String()
						rf.lastHeartbeatSent = time.Time{}
						rf.heartbeatInterval = 100 * time.Millisecond
						// init leader volatile state
						for i := range rf.NextIndex {
							rf.NextIndex[i] = rf.lastLogIndex() + 1
						}
						for i := range rf.MatchIndex {
							rf.MatchIndex[i] = 0
						}
					}
				}
			}
		}(i, termStarted, lastIndex, lastTerm)
	}
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	if rf.role != leader {
		rf.mu.Unlock()
		return
	}
	rf.lastHeartbeatSent = time.Now()
	term := rf.CurrentTerm
	leaderId := rf.me
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == leaderId {
			continue
		}
		go func(server int) {
			args := &AppendEntriesArgs{Term: term, LeaderId: leaderId}
			var reply AppendEntriesReply
			_ = rf.peers[server].Call("Raft.AppendEntries", args, &reply)
			if reply.Term > term {
				rf.mu.Lock()
				if reply.Term > rf.CurrentTerm {
					rf.CurrentTerm = reply.Term
					rf.VotedFor = -1
					rf.role = follower
					rf.Role = rf.role.String()
					rf.persist()
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) lastLogIndex() int {
	if len(rf.Log) == 0 {
		return 0
	}
	return rf.Log[len(rf.Log)-1].Index
}

func (rf *Raft) lastLogTerm() int {
	if len(rf.Log) == 0 {
		return 0
	}
	return rf.Log[len(rf.Log)-1].Term
}

func (rf *Raft) resetElectionTimer() {
	rf.lastHeard = time.Now()
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.Role = "Follower"
	rf.role = follower
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Log = []LogEntry{}
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))
	for i := range rf.NextIndex {
		rf.NextIndex[i] = 1
		rf.MatchIndex[i] = 0
	}
	rf.lastHeard = time.Now()
	rf.heartbeatInterval = 100 * time.Millisecond

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
