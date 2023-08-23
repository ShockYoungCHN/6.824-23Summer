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

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	Offset            int  // deprecated
	Done              bool // deprecated
}

type InstallSnapshotReply struct {
	Term int
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

	// I don't think we have to have these two fields,
	// however, without these fields lead to failure in some tests (cuz without these fields, sync log takes longer time)
	ConflictIndex int
	ConflictTerm  int
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

type RaftLock struct {
	mu       sync.Mutex
	name     string
	start    time.Time
	duration time.Duration
}

func (rflock *RaftLock) Lock(m string) {
	rflock.mu.Lock()
	//println("lock:", m)
	rflock.name = m
	rflock.start = time.Now()
}

func (rflock *RaftLock) Unlock() {
	rflock.duration = time.Since(rflock.start)
	//println("unlock:", rflock.name)
	// DPrintf("raft lock: %s for %d ms", rflock.name, rflock.duration/1000)
	rflock.name = ""
	rflock.mu.Unlock()
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
	mu        sync.Mutex
	raftLock  RaftLock            // Lock to protect shared access to this peer's state
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

	// snapshot metadata
	lastIncludedIndex int
	lastIncludedTerm  int

	applySig chan bool    // signal to apply log
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
	rf.raftLock.Lock("raft.GetState")
	term = rf.currentTerm
	isleader = rf.isLeader()
	rf.raftLock.Unlock()
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
	// fmt.Printf("%d persist %s \n", rf.me, rf.log)
	rf.persister.SaveRaftState(rf.getPersistentStateBytes())
}

func (rf *Raft) getPersistentStateBytes() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
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
	var lastIncludedIndex int
	var lastIncludeTerm int
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

	if err := d.Decode(&log); err != nil {
		rf.log = make([]LogEntry, 1)
	} else {
		rf.log = log
	}

	if err := d.Decode(&lastIncludedIndex); err != nil {
		rf.lastIncludedIndex = 0
	} else {
		rf.lastIncludedIndex = lastIncludedIndex
	}

	if err := d.Decode(&lastIncludeTerm); err != nil {
		rf.lastIncludedTerm = 0
	} else {
		rf.lastIncludedTerm = lastIncludeTerm
	}
}

// the passed-in index is the index on the non-trimmed log
// return the index on the trimmed log
func (rf *Raft) getRealIndex(index int) int {
	return index - rf.lastIncludedIndex
}

// the passed-in index is the index on the trimmed log
// return the index on the non-trimmed log
func (rf *Raft) getOriginalIndex(index int) int {
	return index + rf.lastIncludedIndex
}

//
// A service wants to switch to snapshot. Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
// "we suggest that you simply have it return true"
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// printStack()
	println(rf.me, "trim")
	rf.raftLock.Lock("raft.Snapshot")
	defer rf.raftLock.Unlock()

	// trim the log
	// even log[index] is included in the snapshot,
	// I still decide keep it in log, and set it as the first log entry just as how we initialize log[] in make()
	rf.log = rf.log[rf.getRealIndex(index):]
	rf.lastIncludedIndex = index
	DPrintf("%d trimmed from %d", rf.me, rf.lastIncludedIndex)
	rf.lastIncludedTerm = rf.log[0].Term
	persistentBytes := rf.getPersistentStateBytes()
	rf.persister.SaveStateAndSnapshot(persistentBytes, snapshot)
}

func (rf *Raft) compressLog(end int) {
	if end <= rf.lastIncludedIndex {
		return
	}

	for i := 1; i <= end; i++ {

	}
}

// SendInstallSnapshot should be called when:
// the leader has already discarded the next log entry that it needs to send to a follower.
// 1. a follower is too slow to keep up with the leader
// 2. an exceptionally slow follower or a new server joins the cluster
//
// Although I am very reluctant to use timeout in rpc, it seems I have to do so.
// If some node is disconnected, from sender call rpc, it will take about 4 sec for Call func return false.
// This will lead `TestSnapshotInstall2D` to take more than 120s.
//
func (rf *Raft) SendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf("%d send snapshot to %d", rf.me, server)
	rpcTicker := time.NewTicker(time.Millisecond * 100) // it is just a temporary solution, I will try to find a better way
	defer rpcTicker.Stop()
	ch := make(chan bool, 10)
	go func() {
		ch <- rf.peers[server].Call("Raft.RecvInstallSnapshot", args, reply)
	}()
	for {
		select {
		case ok := <-ch:
			if ok {
				DPrintf("%d send snapshot to %d success", rf.me, server)
				return
			} else {
				DPrintf("%d send snapshot to %d failed", rf.me, server)
			}
		case <-rpcTicker.C:
			DPrintf("%d send snapshot to %d timeout", rf.me, server)
			go func() {
				ch <- rf.peers[server].Call("Raft.RecvInstallSnapshot", args, reply)
			}()
		}
	}
}

func (rf *Raft) RecvInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf("%d try apply snapshot %d", rf.me, args.LastIncludedIndex)
	rf.raftLock.Lock("raft.RecvInstallSnapshot")
	defer rf.raftLock.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.beFollower(args.Term, args.LeaderId)
		rf.persist()
	}
	atomic.StoreInt32(&rf.recvHeartbeat, 1)
	// todo: is it possible that lastIncludedIndex >= args.LastIncludedIndex is true, and do we need to handle this case?
	if rf.lastIncludedIndex >= args.LastIncludedIndex {
		DPrintf("%d snapshot at %d, leader %d snapshot at %d",
			rf.me, rf.lastIncludedIndex,
			args.LeaderId, args.LastIncludedIndex)
		return
	}

	/* Ignored following steps as instructed by raft guide
	2. Create new snapshot file if first chunk (offset is 0)
	3. Write data into snapshot file at given offset
	4. Reply and wait for more data chunks if done is false
	*/

	//5. Save snapshot file, discard any existing or partial snapshot with a smaller index
	rf.persister.SaveStateAndSnapshot(rf.getPersistentStateBytes(), args.Data)

	//6. If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
	for i := 0; i < len(rf.log); i++ {
		if rf.getOriginalIndex(i) == args.LastIncludedIndex && rf.log[i].Term == args.LastIncludedTerm {
			rf.log = rf.log[i:]
			rf.lastIncludedIndex = args.LastIncludedIndex
			rf.lastIncludedTerm = args.LastIncludedTerm
			rf.applySig <- true
			return
		}
	}

	//7. Discard the entire log
	rf.log = make([]LogEntry, 1)
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	//8. Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
	DPrintf("%d apply snapshot %d", rf.me, args.LastIncludedIndex)
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// what if the candidate's term is smaller than the current term?
	rf.raftLock.Lock("raft.RequestVote")
	defer rf.raftLock.Unlock()

	//fmt.Printf("rf.me %d curr %d; id %d candidate %d\n", rf.me, rf.currentTerm, args.CandidateId, args.CandidateTerm)
	// Reply false if term < currentTerm OR
	// rf.log[len(rf.log)-1].Term<args.PrevLogEntryTerm  (see at 5.4.1) OR
	// rf.log[len(rf.log)-1].Term == args.PrevLogEntryTerm && len(rf.log)-1 > args.LastLogEntryIndex
	//(len(rf.log)-1>... means current peer has more log entries than the candidate)

	DPrintf("votes: peer %d term %d, recv %v \n", rf.me, rf.currentTerm, args)
	if args.CandidateTerm < rf.currentTerm ||
		rf.log[len(rf.log)-1].Term > args.PrevLogEntryTerm ||
		(rf.log[len(rf.log)-1].Term == args.PrevLogEntryTerm && rf.getOriginalIndex(len(rf.log)-1) > args.LastLogEntryIndex) {
		// DPrintf("%d recv vote req %v \n", rf.me, args)
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
	rf.raftLock.Lock("raft.sendHeartbeat.getLeaderState")
	isLeader := rf.isLeader()
	rf.raftLock.Unlock()

	for isLeader && !rf.killed() {
		time.Sleep(time.Duration(rf.heartbeat) * time.Millisecond)
		// it seems after being elected, the leader's term won't be changed
		// maybe I should construct args out of the loop
		rf.raftLock.Lock("raft.sendHeartbeat.constructArgs")
		// if we hear from last heartbeat:
		// reply.success is true, then we send heartbeat with no entries
		// reply.success is false, then we send heartbeat with entries

		// if we didn't hear from last heartbeat, that means network problem/append log or snapshot took too long
		// in this case, we should send AppendEntries with entries again

		args := &AppendEntries{LeaderId: rf.me, Term: rf.currentTerm, LeaderCommit: rf.commitIndex}
		isLeader = rf.isLeader()
		rf.raftLock.Unlock()

		rf.broadcastHeartBeat(args)
	}
}

// broadcast AppendEntries to all peers (only used for log replication),
// if the log entry we try to send become part of snapshot, then send snapshot to all peers
// return true if the majority of peers reach consensus
func (rf *Raft) broadcastAppendEntries() bool {
	callback := make(chan bool, len(rf.peers))
	// appendEntries have been sent to itself already, so start from 1
	numCallback := 1
	rf.raftLock.Lock("raft.broadcastAppendEntries.getMaxLogIndex")
	DPrintf("currentLogIndex %d, with entry %v\n", len(rf.log)-1, rf.log[len(rf.log)-1])
	rf.raftLock.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		peer := i
		if peer == rf.me {
			continue
		}
		DPrintf("%d broadcast entries or snapshot to %d", rf.me, peer)
		go func() {
			// the code should be simpler with do-while loop
			rf.peerLock[peer].Lock()
			defer rf.peerLock[peer].Unlock()
			rf.raftLock.Lock("raft.broadcastAppendEntries.needSnapshot")
			prevLogIndex := rf.nextIndex[peer] - 1
			realPrevLogIndex := rf.getRealIndex(prevLogIndex)
			rf.raftLock.Unlock()

			DPrintf("%d broadcast entries or snapshot to %d: mark1", rf.me, peer)
			if realPrevLogIndex < 0 {
				rf.raftLock.Lock("raft.broadcastAppendEntries.constructSnapshotArgs")
				DPrintf("%d send snapshot to %d, Prevlog %d, lastSnapshot %d",
					rf.me, peer, prevLogIndex, rf.lastIncludedIndex)
				snapshotArgs := &InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Data:              rf.persister.ReadSnapshot(),
				}
				snapshotReply := &InstallSnapshotReply{}
				rf.raftLock.Unlock()

				rf.SendInstallSnapshot(peer, snapshotArgs, snapshotReply)

				rf.raftLock.Lock("raft.broadcastAppendEntries.updateNextIndex")
				rf.nextIndex[peer] = rf.lastIncludedIndex + 1
				rf.matchIndex[peer] = rf.lastIncludedIndex
				if snapshotReply.Term > rf.currentTerm {
					rf.beFollower(snapshotReply.Term, -1) // should I set votedFor to -1?
					rf.raftLock.Unlock()
					return
				}
				rf.raftLock.Unlock()
			}

			rf.raftLock.Lock("raft.broadcastAppendEntries.constructArgs")
			prevLogIndex = rf.nextIndex[peer] - 1
			realPrevLogIndex = rf.getRealIndex(prevLogIndex) // reassign values, since they might be changed
			entriesLen := len(rf.log) - 1 - realPrevLogIndex
			isLeader := rf.isLeader()
			DPrintf("log rep: %d broadcast to %d,log range from %d to %d, %v\n",
				rf.me, peer, prevLogIndex+1, prevLogIndex+entriesLen,
				rf.log[realPrevLogIndex+1:realPrevLogIndex+entriesLen+1])
			args := &AppendEntries{
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  rf.log[realPrevLogIndex].Term,
				Term:         rf.currentTerm,
				Entries:      rf.log[realPrevLogIndex+1 : realPrevLogIndex+entriesLen+1],
				LeaderCommit: rf.commitIndex,
			}
			reply := &AppendEntriesReply{}
			rf.raftLock.Unlock()
			if len(args.Entries) == 0 {
				return
			}

			for isLeader && !rf.killed() {
				ok := rf.peers[peer].Call("Raft.RecvAppendEntries", args, reply)

				// reconstruct args
				rf.raftLock.Lock("raft.broadcastAppendEntries.updateState")
				if ok {
					// update nextIndex and matchIndex for follower
					if reply.Success {
						DPrintf("log rep: leader %d, peer %d successfully update from %d to %d\n",
							rf.me, peer, prevLogIndex+1, prevLogIndex+entriesLen)
						callback <- true
						rf.nextIndex[peer] = prevLogIndex + entriesLen + 1
						rf.matchIndex[peer] = prevLogIndex + entriesLen
						rf.raftLock.Unlock()
						break
					} else {
						DPrintf("%d leader send to %d, but conflictIndex %d, term %d",
							rf.me, peer, reply.ConflictIndex, reply.ConflictTerm)
						// the leader should first search its log for conflictTerm.
						// If it finds an entry in its log with that term,
						// it should set nextIndex to be the one beyond the index of the last entry in that term in its log.
						rf.nextIndex[peer] = reply.ConflictIndex
						for i := prevLogIndex; i >= 0; i-- {
							if rf.getRealIndex(i) < 0 {
								// todo: need to send snapshot
								rf.nextIndex[peer] = rf.lastIncludedIndex
								break
							}
							if rf.log[rf.getRealIndex(i)].Term == reply.ConflictTerm {
								rf.nextIndex[peer] = i + 1
								break
							}
						}
						if rf.nextIndex[peer] <= 0 { // smallest nextIndex value is 1
							rf.nextIndex[peer] = 1
						}
					}
				} else {
					// If followers crash or run slowly, or if network packets are lost,
					// the leader retries Append Entries RPCs indefinitely (even after it has responded to the client)
					// until all followers eventually store all log entries
					DPrintf("%d send append entries to %d failed %v\n", rf.me, peer, args.Entries)
				}

				prevLogIndex = rf.nextIndex[peer] - 1
				realPrevLogIndex = rf.getRealIndex(prevLogIndex)
				rf.raftLock.Unlock()
				if realPrevLogIndex < 0 {
					rf.raftLock.Lock("raft.broadcastAppendEntries.constructSnapshotArgs")
					DPrintf("%d send snapshot to %d after failed, Prevlog %d, lastSnapshot %d",
						rf.me, peer, prevLogIndex, rf.lastIncludedIndex)
					snapshotArgs := &InstallSnapshotArgs{
						Term:              rf.currentTerm,
						LeaderId:          rf.me,
						LastIncludedIndex: rf.lastIncludedIndex,
						LastIncludedTerm:  rf.lastIncludedTerm,
						Data:              rf.persister.ReadSnapshot(),
					}
					snapshotReply := &InstallSnapshotReply{}
					rf.raftLock.Unlock()

					rf.SendInstallSnapshot(peer, snapshotArgs, snapshotReply)

					rf.raftLock.Lock("raft.broadcastAppendEntries.updateNextIndex")
					rf.nextIndex[peer] = rf.lastIncludedIndex + 1
					rf.matchIndex[peer] = rf.lastIncludedIndex
					if snapshotReply.Term > rf.currentTerm {
						rf.beFollower(snapshotReply.Term, -1) // should I set votedFor to -1?
						rf.raftLock.Unlock()
						return
					}
					rf.raftLock.Unlock()
				}

				rf.raftLock.Lock("raft.broadcastAppendEntries.constructArgs")
				prevLogIndex = rf.nextIndex[peer] - 1
				realPrevLogIndex = rf.getRealIndex(prevLogIndex) // reassign values, since they might be changed
				entriesLen = len(rf.log) - 1 - realPrevLogIndex
				isLeader = rf.isLeader()
				args = &AppendEntries{
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  rf.log[realPrevLogIndex].Term,
					Term:         rf.currentTerm,
					Entries:      rf.log[realPrevLogIndex+1 : realPrevLogIndex+entriesLen+1],
					LeaderCommit: rf.commitIndex,
				}
				reply = &AppendEntriesReply{}
				rf.raftLock.Unlock()
			}
		}()
	}

	for {
		select {
		case <-callback:
			DPrintf("log rep: leader %d, ready to update commit index\n", rf.me)
			// If there exists an N such that N > commitIndex,
			// a majority of matchIndex[i] ≥ N,
			// and log[N].term == currentTerm:
			// set commitIndex = N (also apply the log to state machine)
			rf.updateCommitIndex(rf.currentTerm)

			numCallback++
			if numCallback == len(rf.peers) {
				return true
			}
		}
	}
}

func (rf *Raft) updateCommitIndex(currentTerm int) {
	// If there exists an N such that N > commitIndex,
	// a majority of matchIndex[i] ≥ N,
	// and log[N].term == currentTerm:
	// set commitIndex = N (also apply the log to state machine)
	rf.raftLock.Lock("raft.updateCommitIndex")
	defer rf.raftLock.Unlock()
	for i := rf.getOriginalIndex(len(rf.log) - 1); i > rf.commitIndex; i-- { // todo: getOriginalIndex(len(rf.log) - 1)
		if rf.log[rf.getRealIndex(i)].Term == currentTerm {
			count := 1
			for j := 0; j < len(rf.peers); j++ {
				if rf.matchIndex[j] >= i {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = i
				DPrintf("%d try apply log from %d to %d\n",
					rf.me, rf.lastApplied+1, rf.commitIndex)
				rf.applySig <- true
				return
			}
		}
	}

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

// 		if entry is nil means heartbeat OR just append no entries(cuz leader and follower's log are already synced)
// in either case above, reply.Success = true.
// 		else need to do log replication
//
// leader does not send heartbeat to itself
func (rf *Raft) RecvAppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
	rf.raftLock.Lock("raft.RecvAppendEntries")
	defer rf.raftLock.Unlock()
	reply.Success = true // heartbeat doesn't care about success
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

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
		DPrintf("%d term %d recv entries from %d term %d with %v \n",
			rf.me, rf.currentTerm, args.LeaderId, args.Term, args.Entries)

		// if args.PrevLogIndex > len(rf.log)-1 then
		// 	"it should return with conflictIndex = len(log) and conflictTerm = None."
		// else, trying to replicate the log
		// 	namely: "If a follower does have prevLogIndex in its log, but the term does not match,
		//	it should return conflictTerm = log[prevLogIndex].Term,
		//	and then search its log for the first index whose entry has term equal to conflictTerm"
		realPrevLogIndex := rf.getRealIndex(args.PrevLogIndex)
		if realPrevLogIndex > len(rf.log)-1 {
			reply.Success = false
			reply.ConflictIndex = rf.getOriginalIndex(len(rf.log))
			reply.ConflictTerm = -1
			return
		} else {
			if realPrevLogIndex > 0 && rf.log[realPrevLogIndex].Term != args.PrevLogTerm { // todo: delete args.PrevLogIndex != -1?
				reply.Success = false
				DPrintf("%d conflict term", rf.me)
				reply.ConflictTerm = rf.log[realPrevLogIndex].Term
				for i := realPrevLogIndex; i >= 0; i-- {
					if rf.log[i].Term != reply.ConflictTerm {
						reply.ConflictIndex = rf.getOriginalIndex(i + 1)
						break
					}
				}
				//rf.raftLock.Unlock()
				return
			}

			for i := 0; i < len(args.Entries); i++ {
				// From paper: If an existing entry conflicts with a new one (same index but different terms),
				// delete the existing entry and all that follow it

				// this situation happens because of snapshot,
				// when a follower takes snapshot at a recent index and leader send logs before that index(this is possible due to our log conflict policy)
				if realPrevLogIndex+1+i < 0 {
					continue
				}

				if len(rf.log)-1 >= realPrevLogIndex+1+i && // args.PrevLogIndex+1+i is the index of the new entry
					rf.log[realPrevLogIndex+1+i].Term != args.Entries[i].Term {
					rf.log = rf.log[:realPrevLogIndex+i+1] // args.PrevLogIndex+i+1 is not included! todo: if prevLogIndex is not in the log, call snapshot
				}

				// Append any new entries not already in the log
				if len(rf.log)-1 < realPrevLogIndex+1+i {
					rf.log = append(rf.log, args.Entries[i])
				}
			}
			reply.Success = true

			// now the log is replicated, we can persist log,
			// I could probably call changeLog() each time in for-loop, but I think persisting once is better
			rf.persist()
		}
		DPrintf("rf.me %d with term %d logindex %d log %v ",
			rf.me, rf.currentTerm, rf.getOriginalIndex(len(rf.log)-1), rf.log)
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry) (WHY?)
	// make sure the log is replicated before updating commitIndex, then you can apply the log
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getOriginalIndex(len(rf.log)-1))
		if rf.log[rf.getRealIndex(rf.commitIndex)].Term == rf.currentTerm { //todo: think about why commitIndex can guarantee that the log is synced
			DPrintf("%d ready apply log from %d to %d ", rf.me, rf.lastApplied+1, rf.commitIndex)
			rf.applySig <- true
			return
		}
	}
}

func (rf *Raft) applyLogAsync() {
	for {
		select {
		case <-rf.applySig:
			// DPrintf("%d commit snapshot instead of log from %d to %d\n", rf.me, start, end)
			// keep doing commit, until no log entries to commit
			for {
				rf.raftLock.Lock("raft.applyLogAsync")
				start := rf.lastApplied + 1
				end := rf.commitIndex
				if rf.getRealIndex(start) <= 0 {
					rf.raftLock.Unlock()
					rf.applySnapshot()
					continue
				}
				if start > end {
					rf.raftLock.Unlock()
					break
				}
				logCopy := rf.log[rf.getRealIndex(start) : rf.getRealIndex(end)+1]
				DPrintf("%d apply log from %d to %d, real index from %d to %d\n",
					rf.me, start, end, rf.getRealIndex(start), rf.getRealIndex(end))
				rf.raftLock.Unlock()
				for i := 0; i < len(logCopy); i++ {
					rf.raftLock.Lock("raft.applyLogAsync")
					DPrintf("%d apply entry %d", rf.me, start+i)
					msg := ApplyMsg{
						CommandValid: true,
						Command:      logCopy[i].Command,
						CommandIndex: start + i,
					}
					rf.raftLock.Unlock()
					rf.applyCh <- msg

					rf.raftLock.Lock("raft.applyLogAsync")
					rf.lastApplied++
					rf.raftLock.Unlock()
				}
			}
		}
	}
}

func (rf *Raft) applySnapshot() {
	rf.raftLock.Lock("raft.applySnapshotAsync")
	DPrintf("%d apply snapshot at %d", rf.me, rf.lastIncludedIndex)
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.persister.ReadSnapshot(),
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}
	rf.lastApplied = rf.lastIncludedIndex
	rf.raftLock.Unlock()
	rf.applyCh <- msg
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
	rf.raftLock.Lock("raft.Start.getIsLeaderSate")
	isLeader := rf.getState() == LEADER
	rf.raftLock.Unlock()

	if isLeader {
		// appends the command to its log as a new entry
		rf.raftLock.Lock("raft.Start.changeLog")
		rf.changeLog(append(rf.log, LogEntry{Term: rf.currentTerm, Command: command}))
		index = rf.getOriginalIndex(len(rf.log) - 1)
		term = rf.currentTerm
		DPrintf("leader %d term %d start command %v on index %d\n", rf.me, rf.currentTerm, command, index)
		rf.raftLock.Unlock()

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
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		randomDelay := r.Int63n(rf.maxDelay-rf.minDelay) + rf.minDelay
		time.Sleep(time.Duration(randomDelay) * time.Millisecond)
		rf.raftLock.Lock("raft.ticker")
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
		rf.raftLock.Unlock()
	}
}

func (rf *Raft) election() {
	// should I unlock until first round heartbeat is sent?
	// 1. increment current term
	rf.raftLock.Lock("raft.election.changeTerm")
	rf.currentTerm += 1
	// 2. vote for self
	rf.votedFor = rf.me
	rf.persist() // persist above 2 changes
	rf.raftLock.Unlock()
	// 3. reset election timeout
	// because I use time.Sleep() to simulate the timeout, all I need is just to return to ticker immediately

	// 4. send request vote to all peers
	go func() {
		// vote for itself
		var votes = 1
		rf.raftLock.Lock("raft.election.voteForSelf")
		electionTerm := rf.currentTerm
		prevLogEntryTerm := rf.log[len(rf.log)-1].Term
		lastLogEntryIndex := rf.getOriginalIndex(len(rf.log) - 1)
		rf.raftLock.Unlock()

		callback := make(chan RequestVoteReply, len(rf.peers))
		args := &RequestVoteArgs{
			CandidateTerm:     electionTerm,
			CandidateId:       rf.me,
			PrevLogEntryTerm:  prevLogEntryTerm,
			LastLogEntryIndex: lastLogEntryIndex,
		}
		// run vote requests in parallel
		for i := 0; i < len(rf.peers); i++ {
			// a intermediate variable is needed here, cuz the value of i could be changed
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
		for !rf.killed() {
			select {
			case reply := <-callback:
				if reply.VoteGranted {
					votes++
				}
				// after receiving a vote result (might get a vote, or not), check if the election is still valid
				rf.raftLock.Lock("raft.election.checkElection")
				if rf.state != CANDIDATE || electionTerm != rf.currentTerm {
					//fmt.Printf("raft err: id %d in %d: terms %d votes %d\n", rf.me, rf.currentTerm, electionTerm, votes)
					rf.raftLock.Unlock()
					return
				}
				if reply.Term > rf.currentTerm {
					rf.beFollower(reply.Term, -1)
					rf.raftLock.Unlock()
					return
				}
				DPrintf("raft: id %d in %d: terms %d votes %d", rf.me, rf.currentTerm, electionTerm, votes)
				rf.raftLock.Unlock()

				// If votes received from the majority of servers: become leader
				if votes > len(rf.peers)/2 {
					rf.raftLock.Lock("raft.election.becomeLeader")
					rf.beLeader()
					rf.raftLock.Unlock()
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
	num := runtime.NumGoroutine()
	fmt.Printf("current active goroutine: %d\n", num)

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.heartbeat = 125 // 8 times per second
	rf.votedFor = -1   // -1 means not yet voted for anyone in current term
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.recvHeartbeat = 0
	rf.minDelay = int64(300)
	rf.maxDelay = int64(600)
	rf.peerLock = make([]sync.Mutex, len(peers))
	rf.applyCh = applyCh
	rf.applySig = make(chan bool, 100)

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	rf.log = make([]LogEntry, 1, 100)

	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex { // log index starts from 1, so nextIndex init as 1
		rf.nextIndex[i] = 1
	}
	// initialize matchIndex to 0
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.raftLock.Lock("raft.make.readPersist")
	rf.readPersist(persister.ReadRaftState())
	rf.raftLock.Unlock()

	// start ticker goroutine to start elections
	go rf.ticker()
	// start apply log goroutine
	go rf.applyLogAsync()
	return rf
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
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
