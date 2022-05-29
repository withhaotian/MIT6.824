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
//	"bytes"
	"sync"
	"sync/atomic"

//	"6.824/labgob"
	"6.824/labrpc"

	"time"
	"math/rand"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// 宏定义-节点状态（跟随者、候选人、领导者）
type NodeState int
const (
	Follower    NodeState = 0
	Candidate	NodeState = 1
	Leader   	NodeState = 2
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	
	// 参考Fig.2
	// 定义raft数据结构
	state			NodeState			// 节点状态
	currentTerm 	int					// 最新的任期数
	votedFor		int					// 在当前任期投票给的候选人ID

	// 选举时间
	electionTime time.Time
}

// 选举定时器随机值设为150-300ms
func RandomizedElectionTimeout() time.Time {
	// 定义随机种子
	rand.Seed(time.Now().UnixNano())
	msec_base := rand.Intn(300) + 150

	t := time.Now()
	t = t.Add(time.Duration(msec_base) * time.Millisecond)
	
	return t
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	// 返回当前rf节点的任期数和是否为leader
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int 	// 候选人的任期数
	CandidateId		int		// 请求vote的候选人id
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term			int 	// currentTerm，对于候选人而言更新自己的term
	VoteGranted		bool 	// true意味着候选人已经收到了选票
}

type AppendEntriesArgs struct {
	Term 		int		// leader的任期数
	LeaderId	int		// leader的ID
}

type AppendEntriesReply struct {
	Term 		int 	// currentTerm，对于领导者而言更新自己的term
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果来自其他节点的投票请求的任期小于当前节点，则视为无效
	// 或者当前节点已经给其他节点投过票
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
	}
	
	rf.votedFor = args.CandidateId
	
	reply.Term = rf.currentTerm
	reply.VoteGranted = true

	rf.electionTime = RandomizedElectionTimeout()
}

//
// example AppendEntry RPC handler.
//
func (rf *Raft) AppendEntry(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果来自其他节点的投票请求的任期小于当前节点，则视为无效
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
	}
	
	// 候选人收到了来自leader的心跳包，会转为follower
	if rf.state == Candidate {
		rf.state = Follower
		rf.votedFor = -1
	}
	
	reply.Term = args.Term

	rf.electionTime = RandomizedElectionTimeout()
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

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

// 只有成为leader才会发送appendEntries
func (rf *Raft) sendAppendEntries(heartBeat bool) {
	args := AppendEntriesArgs{rf.currentTerm, rf.me}

	DPrintf("{Leader %v} sends AppendEntriesArgs %v", rf.me, args)

	for i:= range rf.peers {
		if i != rf.me {
			go func(i int) {
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntry(i, &args, &reply)

				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.votedFor = -1
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) startElection() {
	// 开始选举时，首先将自身变为candidate，并且让任期数+1
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me		// 每个候选人在发起选举后，投票给自己

	votes := 1

	args := RequestVoteArgs{rf.currentTerm, rf.me}
	DPrintf("{Node %v} starts election with RequestVoteArgs %v", rf.me, args)

	for i := range rf.peers {
		if i != rf.me {
			// 异步并行投票，不阻塞
			go func(i int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(i, &args, &reply)

				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					DPrintf("{Node %v} receives RequestVoteReply %v from {Node %v} after sending RequestVoteArgs %v in term %v", rf.me, reply, i, args, rf.currentTerm)
					
					// 候选人收到了选票
					if reply.VoteGranted {
						votes += 1
						// 获得超过半数的选票，则变为Leader
						if votes > len(rf.peers)/2 {
							rf.state = Leader
							rf.sendAppendEntries(true)	// 发送心跳包
							rf.electionTime = RandomizedElectionTimeout()	// 更新选举时间
						}
					} else if reply.Term > rf.currentTerm { // IF RPC request or response contains term T > currentterm: 
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.votedFor = -1
					}
				}
			}(i)
		}
	}
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		// 如果状态为leader，则需要定期发布心跳包
		if rf.state == Leader {
			rf.electionTime = RandomizedElectionTimeout()
			// 已经有leader，发送心跳包给其他节点
			rf.sendAppendEntries(true)
		} else if time.Now().After(rf.electionTime) { // 如果选举时间超时了，则自动进入下一轮选举
			rf.electionTime = RandomizedElectionTimeout()
			// 开始选举 
			rf.startElection()
		}
		rf.mu.Unlock();

		ms := 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.electionTime = RandomizedElectionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
