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
	"6.824/labgob"
	"bytes"
	"fmt"
	"math/rand"
	"runtime"

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
	Term         int
	LeaderId     int
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int32
	// Success is true if follower contained entry matching AppendEntries.PrevLogIndex and AppendEntries.PrevLogTerm
	Success bool
	// because of network delay, some entries may be repeatedly sent to same peer
	// this bring trouble when updating nextIndex
	LastLogIndex int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// these args will be enough for election in 2A
	CandidateTerm int
	CandidateId   int
	// in 2B(log replication), more restrictions are added
	PrevLogEntryTerm  int
	LastLogEntryIndex int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool
	Term        int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

func (l LogEntry) String() string {
	return fmt.Sprintf("Term: %d, Command: %v", l.Term, l.Command)
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
	applyCh   chan ApplyMsg

	// Persistent state on all servers
	// Updated on stable storage before responding to RPCs， which means should be persisted when the state changes
	currentTerm int        // current term, increase monotonically
	votedFor    int        // candidate id that received vote in current term
	log         []LogEntry // first index is 1

	// fields not mentioned in paper, but implemented for convenience
	state           int32 // 0: follower, 1: candidate, 2: leader
	recvHeartbeat   int32 // receive heartbeat from leader or not during one timeout
	electionTimeout int64 // timeout for election
	minDelay        int64
	maxDelay        int64
	heartbeat       int64 // heartbeat gap

	// volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	lock     sync.Mutex
	peerLock []sync.Mutex // each peer has a lock, used when sending log replication
}

func (rf *Raft) changeCurrentTerm(term int) {
	rf.currentTerm = term
	rf.persist()
}

func (rf *Raft) changeVotedFor(votedFor int) {
	rf.votedFor = votedFor
	rf.persist()
}

func (rf *Raft) changeLog(log []LogEntry) {
	rf.log = log
	rf.persist()
}

func (rf *Raft) beLeader() {
	rf.state = LEADER
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex { // log index starts from 1, so nextIndex init as 1
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
}

func (rf *Raft) beCandidate() {
	rf.state = CANDIDATE
}

func (rf *Raft) beFollower(term int, votedFor int) {
	rf.state = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = votedFor
	rf.persist()
}

func (rf *Raft) isLeader() bool {
	return rf.state == LEADER
}

func (rf *Raft) getState() int32 {
	return rf.state
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.isLeader()
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
// currentTerm, votedFor, log[] should be persistent
// persist should be called each time the state changes
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	// fmt.Printf("%d persist %s \n", rf.me, rf.log)
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if err := d.Decode(&currentTerm); err != nil {
		rf.currentTerm = 0
	} else {
		rf.currentTerm = currentTerm
	}

	if err := d.Decode(&votedFor); err != nil {
		rf.votedFor = -1
	} else {
		rf.votedFor = votedFor
	}

	// fmt.Printf("%d read %s \n", rf.me, rf.log)
	if err := d.Decode(&log); err != nil {
		rf.log = make([]LogEntry, 1)
	} else {
		rf.log = log
	}
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
	// what if the candidate's term is smaller than the current term?
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//fmt.Printf("rf.me %d curr %d; id %d candidate %d\n", rf.me, rf.currentTerm, args.CandidateId, args.CandidateTerm)
	// Reply false if term < currentTerm OR
	// rf.log[len(rf.log)-1].Term<args.PrevLogEntryTerm  (see at 5.4.1) OR
	// rf.log[len(rf.log)-1].Term == args.PrevLogEntryTerm && len(rf.log)-1 > args.LastLogEntryIndex
	//(len(rf.log)-1>... means current peer has more log entries than the candidate)

	fmt.Printf("votes: peer %d term %d, recv %v \n", rf.me, rf.currentTerm, args)
	if args.CandidateTerm < rf.currentTerm ||
		rf.log[len(rf.log)-1].Term > args.PrevLogEntryTerm ||
		(rf.log[len(rf.log)-1].Term == args.PrevLogEntryTerm && len(rf.log)-1 > args.LastLogEntryIndex) {
		//fmt.Printf("%d recv %v \n", rf.me, args)
		if args.CandidateTerm > rf.currentTerm {
			rf.beFollower(args.CandidateTerm, args.CandidateId)
		}
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// if candidate term larger, then the current peer (no matter in LEADER or CANDIDATE state) should be follower
	if args.CandidateTerm > rf.currentTerm {
		rf.beFollower(args.CandidateTerm, args.CandidateId)
		//fmt.Printf("rf.me %d become follower \n", rf.me)
	}

	// due to network issue, follower might receive 2 requests from same candidate;
	//  it is also why term is not the only factor to decide whether to grant vote

	// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.changeVotedFor(args.CandidateId)
		// the vote req is also viewed as heartbeat
		// to prevent too much follower get into CANDIDATE state
		atomic.StoreInt32(&rf.recvHeartbeat, 1)
		reply.VoteGranted = true
	}
}

// repeatedly send Heartbeat to all peers
func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	isLeader := rf.isLeader()
	rf.mu.Unlock()

	for isLeader && !rf.killed() {
		time.Sleep(time.Duration(rf.heartbeat) * time.Millisecond)
		// it seems after being elected, the leader's term won't be changed
		// maybe I should construct args out of the loop
		rf.mu.Lock()
		args := &AppendEntries{LeaderId: rf.me, Term: rf.currentTerm, LeaderCommit: rf.commitIndex}
		isLeader = rf.isLeader()
		rf.mu.Unlock()

		rf.broadcastHeartBeat(args)
	}
}

// broadcast AppendEntries to all peers (only used for log replication)
// return true if the majority of peers reach consensus
func (rf *Raft) broadcastAppendEntries() bool {
	callback := make(chan bool, len(rf.peers))
	// appendEntries have been sent to itself already, so start from 1
	numCallback := 1
	rf.mu.Lock()
	currentLogIndex := len(rf.log) - 1
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		peer := i
		if peer == rf.me {
			continue
		}
		go func() {
			rf.peerLock[peer].Lock()
			defer rf.peerLock[peer].Unlock()
			rf.mu.Lock()
			isLeader := rf.isLeader()
			prevLogIndex := rf.nextIndex[peer] - 1
			// fmt.Printf("log rep: %d broadcast to %d with prevIndex %d\n", rf.me, peer, prevLogIndex)
			args := &AppendEntries{
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  rf.log[prevLogIndex].Term,
				Term:         rf.currentTerm,
				Entries:      rf.log[prevLogIndex+1:],
				LeaderCommit: rf.commitIndex,
			}
			reply := &AppendEntriesReply{}
			rf.mu.Unlock()
			//fmt.Printf("%d sending %v \n", rf.me, args.Entries)
			for isLeader && !rf.killed() {
				ok := rf.peers[peer].Call("Raft.RecvAppendEntries", args, reply)
				rf.mu.Lock()
				if ok {
					// update nextIndex and matchIndex for follower
					if reply.Success {
						callback <- true
						rf.nextIndex[peer] = reply.LastLogIndex + 1
						rf.matchIndex[peer] = reply.LastLogIndex
						rf.mu.Unlock()
						break
					} else { // if append entries failed, decrement nextIndex and retry until success
						rf.nextIndex[peer]--
						if rf.nextIndex[peer] <= 0 { // smallest nextIndex value is 1
							rf.nextIndex[peer] = 1
						}
					}
				} else {
					// If followers crash or run slowly, or if network packets are lost,
					// the leader retries Append Entries RPCs indefinitely (even after it has responded to the client)
					// until all followers eventually store all log entries
					DPrintf("%d send append entries to %d failed\n", rf.me, peer)
				}

				isLeader = rf.isLeader()
				//fmt.Printf("nextIndex %d, log last index %d\n", rf.nextIndex[peer], len(rf.log)-1)
				prevLogIndex = rf.nextIndex[peer] - 1
				args = &AppendEntries{
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  rf.log[prevLogIndex].Term,
					Term:         rf.currentTerm,
					Entries:      rf.log[prevLogIndex+1:],
					LeaderCommit: rf.commitIndex,
				}
				reply = &AppendEntriesReply{}
				rf.mu.Unlock()
			}
		}()
	}

	// catch panic
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("panic: %v from %d with %d\n", err, rf.me, rf.log)
		}
	}()
	for {
		select {
		case <-callback:
			numCallback++
			if numCallback > len(rf.peers)/2 {
				// If there exists an N such that N > commitIndex,
				// a majority of matchIndex[i] ≥ N,
				// and log[N].term == currentTerm:
				// set commitIndex = N (also apply the log to state machine)
				rf.mu.Lock()
				if rf.commitIndex < currentLogIndex &&
					rf.log[currentLogIndex].Term == rf.currentTerm {
					rf.commitIndex = currentLogIndex
					rf.applyLog(rf.lastApplied+1, rf.commitIndex)
				}
				rf.mu.Unlock()
				return true
			}
		}
	}
	//fmt.Printf("%d sending heartbeat %v \n", rf.me, isLeader)
}

// broadcast heartbeat to all peers
func (rf *Raft) broadcastHeartBeat(args *AppendEntries) {
	for i := 0; i < len(rf.peers); i++ {
		peer := i
		if peer == rf.me {
			continue
		}
		go func() {
			reply := &AppendEntriesReply{}
			rf.peers[peer].Call("Raft.RecvAppendEntries", args, reply)
		}()
	}
}

// leader does not send heartbeat to itself
func (rf *Raft) RecvAppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
	// 		if entry is nil means heartbeat OR just append no entries(cuz leader and follower's log are already synced)
	// in either case above, reply.Success = true.
	// 		else need to do log replication
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = true // heartbeat doesn't care about success
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// fmt.Printf("%d term %d recv entries from %d term %d with %v \n", rf.me, rf.currentTerm, args.LeaderId, args.Term, args.Entries)
	atomic.StoreInt32(&rf.recvHeartbeat, 1)

	switch rf.getState() {
	case FOLLOWER:
		// beFollower function is idempotent
		rf.beFollower(args.Term, args.LeaderId)
	case CANDIDATE: // if a candidate receives heartbeat with term larger or equal, it should become follower
		rf.beFollower(args.Term, args.LeaderId)
	case LEADER:
		//fmt.Printf("leader %d term %d recv heartbeat from leader %d term %d \n", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		if args.Term > rf.currentTerm {
			rf.beFollower(args.Term, args.LeaderId)
		} else { // this should never happen
			//fmt.Printf("There are two leaders in the same term %d \n", rf.currentTerm)
		}
	}
	// log replication
	if args.Entries != nil {
		//fmt.Printf("%d apply entries %v \n", rf.me, args.Entries)

		// if args.PrevLogIndex > len(rf.log)-1 then we can't replicate the log
		// else, trying to replicate the log
		if args.PrevLogIndex > len(rf.log)-1 {
			//do nothing
		} else {
			if args.PrevLogIndex != -1 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
				reply.Success = false
				return
			}
			for i := 0; i < len(args.Entries); i++ {
				// If an existing entry conflicts with a new one (same index but different terms),
				// delete the existing entry and all that follow it
				if len(rf.log)-1 >= args.PrevLogIndex+1+i && // args.PrevLogIndex+1+i is the index of the new entry
					rf.log[args.PrevLogIndex+1+i].Term != args.Entries[i].Term {
					rf.log = rf.log[:args.PrevLogIndex+i+1] // args.PrevLogIndex+i+1 is not included!
				}

				// Append any new entries not already in the log
				if len(rf.log)-1 < args.PrevLogIndex+1+i {
					rf.log = append(rf.log, args.Entries[i])
				}
			}
			reply.Success = true
			reply.LastLogIndex = len(rf.log) - 1

			// now the log is replicated, we can persist log,
			// I could probably call changeLog() each time in for-loop, but I think persisting once is better
			rf.persist()
		}
		//fmt.Printf("rf.me%d with term%d log%v\n", rf.me, rf.currentTerm, rf.log)
	}
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// todo: is it possible that leaderCommit > len(rf.log)-1 ?
	if rf.log[len(rf.log)-1].Term == rf.currentTerm {
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < len(rf.log)-1 {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = len(rf.log) - 1
			}
			rf.applyLog(rf.lastApplied+1, rf.commitIndex)
		}
	}
}

func (rf *Raft) applyLog(start int, end int) {
	for i := start; i <= end; i++ {
		//println(rf.me, i, rf.log[i].Command.(int), start, end, rf.lastApplied)
		//fmt.Printf("%d apply log %v \n", rf.me, rf.log)
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
		rf.lastApplied = i
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	rf.mu.Lock()
	isLeader := rf.getState() == LEADER
	rf.mu.Unlock()

	if isLeader {
		// appends the command to its log as a new entry
		rf.mu.Lock()
		// fmt.Printf("leader %d term %d recv command %v \n", rf.me, rf.currentTerm, command)
		rf.changeLog(append(rf.log, LogEntry{Term: rf.currentTerm, Command: command}))
		index = len(rf.log) - 1
		term = rf.currentTerm
		rf.mu.Unlock()

		// start the agreement and return immediately.
		// 'rf.broadcastAppendEntries() is true' being said the majority of peers reply
		go rf.broadcastAppendEntries()
	}
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
	rf.currentTerm += 1
	//fmt.Printf("%d in %d \n", rf.me, rf.currentTerm)
	// 2. vote for self
	rf.votedFor = rf.me
	rf.persist() // persist above 2 changes
	rf.mu.Unlock()
	// 3. reset election timeout
	// because I use time.Sleep() to simulate the timeout, all I need is just to return to ticker immediately

	// 4. send request vote to all peers
	go func() {
		// vote for itself
		var votes = 1
		rf.mu.Lock()
		electionTerm := rf.currentTerm
		prevLogEntryTerm := rf.log[len(rf.log)-1].Term
		lastLogEntryIndex := len(rf.log) - 1
		rf.mu.Unlock()

		callback := make(chan RequestVoteReply, len(rf.peers))
		args := &RequestVoteArgs{
			CandidateTerm:     electionTerm,
			CandidateId:       rf.me,
			PrevLogEntryTerm:  prevLogEntryTerm,
			LastLogEntryIndex: lastLogEntryIndex,
		}
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
					rf.beFollower(reply.Term, -1)
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				fmt.Printf("raft: id %d in %d: terms %d votes %d \n", rf.me, rf.currentTerm, electionTerm, votes)

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
	rf.heartbeat = 125 // 8 times per second
	rf.votedFor = -1   // -1 means not yet voted for anyone in current term
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.recvHeartbeat = 0
	rf.minDelay = int64(600)
	rf.maxDelay = int64(1000)
	rf.peerLock = make([]sync.Mutex, len(peers))
	rf.applyCh = applyCh

	rf.log = make([]LogEntry, 1, 100)

	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex { // log index starts from 1, so nextIndex init as 1
		rf.nextIndex[i] = 1
	}
	// initialize matchIndex to 0
	rf.matchIndex = make([]int, len(peers))

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// used for debugging
func printStack() {
	buf := make([]byte, 1024)
	n := runtime.Stack(buf, true)
	fmt.Printf("%s\n", buf[:n])
}

/*
Index:  1  | 2 | 3 | 4 | 5
Term:   1  | 1 | 2 | 2 | 3
Command: A | B | C | D | E
Now assume the leader wants to send the follower two new log entries, F and G, from term 3.

In this RPC, the leader needs to tell the follower where to insert the new entries.
In this case, prevLogIndex will be 5 (because F and G should be appended after E)
and prevLogTerm will be 3 (because the entry with index 5 belongs to term 3).
*/
