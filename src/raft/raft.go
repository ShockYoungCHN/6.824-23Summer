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
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
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

type AppendEntries struct {
}

type HeartbeatArgs struct {
	LeaderId      int
	LeaderTerm    int32
	AppendEntries AppendEntries
}

type HeartbeatReply struct {
	Ack bool
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateTerm int32
	CandidateId   int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool
	Term        int32
}

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
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

	state       int32 // 0: follower, 1: candidate, 2: leader
	currentTerm int32 // current term, increase monotonically
	votedFor    int   // candidate id that received vote in current term
	votes       int32 // votes received in current term

	recvHeartbeat   int32 // receive heartbeat from leader or not during one timeout
	electionTimeout int64 // timeout for election
	minDelay        int64
	maxDelay        int64
	heartbeat       int64 // heartbeat gap

	lock         sync.Mutex
	leaderIdLock sync.Mutex
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A)
	rf.mu.Lock()
	term = int(rf.currentTerm)
	isleader = rf.isLeader()
	rf.mu.Unlock()
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
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// what if the candidate's term is smaller than the current term?
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm
	//fmt.Printf("rf.me %d curr %d; id %d candidate %d\n", rf.me, rf.currentTerm, args.CandidateId, args.CandidateTerm)
	if args.CandidateTerm < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// if candidate term larger, then the current peer (no matter in LEADER or CANDIDATE state) should be follower
	if args.CandidateTerm > rf.currentTerm {
		rf.currentTerm = args.CandidateTerm
		rf.state = FOLLOWER
		rf.votedFor = args.CandidateId
	}

	// due to network issue, follower might receive 2 requests from same candidate;
	//  it is also why term is not the only factor to decide whether to grant vote

	// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		// the vote req is also viewed as heartbeat
		// to prevent too much follower get into CANDIDATE state
		atomic.StoreInt32(&rf.recvHeartbeat, 1)
		reply.VoteGranted = true
	}
}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	isLeader := rf.isLeader()
	rf.mu.Unlock()

	for isLeader && !rf.killed() {
		time.Sleep(time.Duration(rf.heartbeat) * time.Millisecond)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			rf.mu.Lock()
			args := &HeartbeatArgs{LeaderId: rf.me, LeaderTerm: rf.currentTerm, AppendEntries: AppendEntries{}}
			reply := &HeartbeatReply{}
			rf.mu.Unlock()
			rf.peers[i].Call("Raft.RecvHeartbeat", args, reply)
		}
		rf.mu.Lock()
		isLeader = rf.isLeader()
		rf.mu.Unlock()
		////fmt.Printf("%d sending heartbeat %v \n", rf.me, isLeader)
	}
}

// leader does not send heartbeat to itself
func (rf *Raft) RecvHeartbeat(args *HeartbeatArgs, reply *HeartbeatReply) {
	rf.mu.Lock() //mu protects term, votedFor, state
	defer rf.mu.Unlock()
	if args.LeaderTerm < rf.currentTerm {
		return
	}
	//fmt.Printf("%d recv heartbeat \n", rf.me)
	switch rf.getState() {
	case FOLLOWER:
		atomic.StoreInt32(&rf.recvHeartbeat, 1)
		rf.currentTerm = args.LeaderTerm
		rf.votedFor = args.LeaderId
	case CANDIDATE: // if a candidate receives heartbeat with term larger or equal, it should become follower
		if args.LeaderTerm == rf.currentTerm {
			//fmt.Printf("%d recv equal %d \n", rf.me, args.LeaderId)
		}
		rf.state = FOLLOWER
		atomic.StoreInt32(&rf.recvHeartbeat, 1)
		rf.currentTerm = args.LeaderTerm
		rf.votedFor = args.LeaderId
	case LEADER:
		//fmt.Printf("leader %d term %d recv heartbeat from leader %d term %d \n", rf.me, rf.currentTerm, args.LeaderId, args.LeaderTerm)
		if args.LeaderTerm > rf.currentTerm {
			rf.state = FOLLOWER
			rf.currentTerm = args.LeaderTerm
			rf.votedFor = args.LeaderId
			atomic.StoreInt32(&rf.recvHeartbeat, 1)
		} else { // this should never happen
			//fmt.Printf("There are two leaders in the same term %d \n", rf.currentTerm)
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
	s := rand.NewSource(int64(rf.me))
	r := rand.New(s)
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		randomDelay := r.Int63n(rf.maxDelay-rf.minDelay) + rf.minDelay
		time.Sleep(time.Duration(randomDelay) * time.Millisecond)
		rf.mu.Lock()
		// check peer's state
		switch rf.getState() {
		case FOLLOWER:
			// if the follower doesn't receive heartbeat from leader, then start election
			if atomic.LoadInt32(&rf.recvHeartbeat) == 0 {
				rf.beCandidate()
				go rf.election()
			} else {
				atomic.StoreInt32(&rf.recvHeartbeat, 0)
			}
		case CANDIDATE:
			go rf.election()
		case LEADER:
		}
		rf.mu.Unlock()
		// use waitgroup to check if the re-election is finished before next election happens
	}

}

func (rf *Raft) election() {
	// should I unlock until first round heartbeat is sent?
	// 1. increment current term
	rf.mu.Lock()
	rf.currentTerm++
	//fmt.Printf("%d in %d \n", rf.me, rf.currentTerm)
	// 2. vote for self
	rf.votedFor = rf.me
	rf.mu.Unlock()
	// 3. reset election timeout
	// because I use time.Sleep() to simulate the timeout, all I need is just to return to ticker immediately

	// 4. send request vote to all peers
	go func() {
		// vote for itself
		var votes = 1
		rf.mu.Lock()
		electionTerm := rf.currentTerm
		rf.mu.Unlock()

		callback := make(chan RequestVoteReply, len(rf.peers))
		args := &RequestVoteArgs{CandidateTerm: electionTerm, CandidateId: rf.me}
		// run vote requests in parallel
		for i := 0; i < len(rf.peers); i++ {
			// a intermediate variable is needed here, cuz the value of i can be changed
			server := i
			if server == rf.me {
				continue
			}

			go func() {
				reply := &RequestVoteReply{}
				if rf.sendRequestVote(server, args, reply) {
					callback <- *reply
				}
			}()
		}

		// after starting vote, check the if votes received from majority of servers,
		// and other conditions (in some case candidate can be turned into follower, or current election is out of date)
		for {
			select {
			case reply := <-callback:
				if reply.VoteGranted {
					votes++
				}
				// after receiving a the vote result (might get a vote, or not), check if the election is still valid
				rf.mu.Lock()
				if rf.state != CANDIDATE || electionTerm != rf.currentTerm {
					//fmt.Printf("raft err: id %d in %d: terms %d votes %d\n", rf.me, rf.currentTerm, electionTerm, votes)
					rf.mu.Unlock()
					return
				}
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = FOLLOWER
					rf.votedFor = -1
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				//fmt.Printf("raft: id %d in %d: terms %d votes %d\n", rf.me, rf.currentTerm, electionTerm, votes)

				// If votes received from the majority of servers: become leader
				if votes > len(rf.peers)/2 {
					rf.mu.Lock()
					rf.beLeader()
					rf.mu.Unlock()
					// send heartbeat to all peers
					go rf.sendHeartbeat()
				}
			}
		}
	}()
}

func (rf *Raft) beLeader() {
	rf.state = LEADER
}

func (rf *Raft) beCandidate() {
	rf.state = CANDIDATE
}

func (rf *Raft) beFollower() {
	rf.state = FOLLOWER
}

func (rf *Raft) isLeader() bool {
	return rf.state == LEADER
}

func (rf *Raft) getState() int32 {
	return rf.state
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
	rf.heartbeat = 100 // 8 times per second
	rf.votedFor = -1   // -1 means not yet voted for anyone in current term
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.recvHeartbeat = 0
	rf.minDelay = int64(600)
	rf.maxDelay = int64(1000)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
