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
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	LEADER    int = 0
	FOLLOWER  int = 1
	CANDIDATE int = 2
)

const (
	ELECTION_TIMEOUT        = 600 * time.Millisecond
	RANDOM_ELECTION_TIMEOUT = 100 * time.Millisecond
	HEARTBEAT_FREQUENCY     = ELECTION_TIMEOUT / 2
)

var debugStart time.Time
var debug int

func init() {
	debugStart = time.Now()
	debug = 2

	log.SetFlags(log.Flags()&^(log.Ldate|log.Ltime) | log.Lshortfile)
}

func debog(format string, a ...interface{}) {
	if debug >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d ", time)
		format = prefix + format
		log.Printf(format, a...)
	}
}

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

type logentry struct {
	Command string
	Term    int
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm                   int
	votedFor                      int
	log                           []logentry
	lastAppendEntriesReceivedTime time.Time
	lastAppendEntriesSentTime     time.Time
	electionTime                  time.Time
	state                         int
	voteReplies                   []RequestVoteReply
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.state == LEADER {
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
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if args.Term == rf.currentTerm {
		if rf.votedFor == -1 {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		} else {
			reply.VoteGranted = false
		}
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
	reply.Term = rf.currentTerm
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

func (rf *Raft) start_election() {
	debog("R%d In start_election", rf.me)
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.electionTime = time.Now()

	for peer_id, _ := range rf.peers {
		if peer_id != rf.me {
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.log) - 1,
			}
			// if len rf.log == 0 it means rf.log is null, and it has no defined term
			if len(rf.log) > 0 {
				args.LastLogTerm = rf.log[len(rf.log)-1].Term
			}
			rf.voteReplies[peer_id] = RequestVoteReply{VoteGranted: false, Term: -1}

			go rf.sendRequestVote(peer_id, &args, &rf.voteReplies[peer_id])
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	debog("R%d In ticker! Btw I've been told I should start at least 2...", rf.me)

	rand.Seed(time.Now().UnixNano())

	// Note this time duration cast of intn 100 is strange, but
	// seems it's the way it's supposed to work to be able to
	// multiply with a time.timeunit type...
	// timeout := (ELECTION_TIMEOUT + time.Duration(rand.Intn(RANDOM_ELECTION_TIMEOUT))) * time.Millisecond
	timeout := (ELECTION_TIMEOUT + RANDOM_ELECTION_TIMEOUT)

	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// Section 9.3 of extended raft :
		// with 50ms of randomness ...
		// We recommend using a conservative
		// election timeout such as 150–300ms
		//Because the tester limits you to 10 heartbeats per second,
		// you will have to use an election timeout larger than the
		// paper's 150 to 300 milliseconds, but not too large, because
		// then you may fail to elect a leader within five seconds.

		time.Sleep(10 * time.Millisecond)
		t := time.Now()

		switch rf.state {
		case FOLLOWER:
			if t.Sub(rf.lastAppendEntriesReceivedTime) > timeout {
				rf.state = CANDIDATE
				rf.start_election()
			}
		case CANDIDATE:
			// count votes
			votes := 1
			for peer_id := 0; peer_id < len(rf.peers); peer_id++ {
				if rf.voteReplies[peer_id].VoteGranted {
					votes++
				}
			}

			// if vote majority, become leader
			if votes > len(rf.peers)/2 {
				debog("R%d %v Got majority of votes (%d), becoming LEADER!", rf.me, rf.state, votes)
				rf.state = LEADER
				rf.lastAppendEntriesSentTime = time.Now()
				// copied from case leader. Update at both places if needed.
				for peer_id := 0; peer_id < len(rf.peers); peer_id++ {
					if peer_id != rf.me {
						args := AppendEntriesArgs{
							Entries: make([]logentry, 0), Term: rf.currentTerm}
						reply := AppendEntriesReply{}
						go rf.peers[peer_id].Call("Raft.AppendEntries", &args, &reply)
					}
				}
			} else {
				// if elections timeout, restart elections
				if t.Sub(rf.electionTime) > timeout {
					debog("R%d %v Election timeout, restarting elections", rf.me, rf.state)
					rf.start_election()
				}
			}
			// if AppendEntries RPC received from new leader: convert to
			// follower
			// already managed inside appendentries
			// if rf.lastAppendEntriesTime.After(rf.electionTime) {
			//	rf.state = FOLLOWER
			// }

		case LEADER:
			// send heartbeat
			// should only send them at HEARTBEATFREQUENCY THOUGH
			// Checking that a more recent term is seen nin a reply is managed in the AppendEntries server function
			// block copied in case candidate, update also there id needed
			if rf.lastAppendEntriesSentTime.Sub(time.Now()) > HEARTBEAT_FREQUENCY {
				debog("R%d %v I'm the leader, sending heartbeats", rf.me, rf.state)
				rf.lastAppendEntriesSentTime = time.Now()
				for peer_id := 0; peer_id < len(rf.peers); peer_id++ {
					if peer_id != rf.me {
						args := AppendEntriesArgs{
							Entries: make([]logentry, 0), Term: rf.currentTerm}
						reply := AppendEntriesReply{}
						go rf.peers[peer_id].Call("Raft.AppendEntries", &args, &reply)
					}
				}
			}
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastAppendEntriesReceivedTime = time.Now()
	rf.lastAppendEntriesSentTime = time.Now()
	rf.state = FOLLOWER
	rf.voteReplies = make([]RequestVoteReply, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

//
// AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logentry
	LeaderCommit int
}

//
// AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.lastAppendEntriesReceivedTime = time.Now()

	if rf.state == CANDIDATE {
		if args.Term >= rf.currentTerm {
			rf.state = FOLLOWER
		}
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}
	reply.Term = rf.currentTerm

	// If args.Entries is empty it means it's a heartbeat.
	// I decided to return true. Not sure it's good...
	if len(args.Entries) == 0 {
		reply.Success = true
		return
		//return true
	}

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
		//return false
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm

	// checks that rf.log is at least large enough, rf.log starts at 1
	if len(rf.log) > args.PrevLogIndex {
		if rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
			// good
			reply.Success = true

		} else {
			//return false
			reply.Success = false
		}
	} else {
		//return false
		reply.Success = false
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	delete_from := -1
	for entry_index, entry := range rf.log {
		if len(args.Entries) > entry_index {
			if args.Entries[entry_index].Term != entry.Term {
				delete_from = entry_index
				break
			}
		} else {
			break
		}
	}
	var new_log []logentry
	if delete_from >= 0 {
		new_log = make([]logentry, delete_from)
		for i := 0; i < delete_from; i++ {
			new_log[i] = rf.log[i]
		}
	}
	rf.log = new_log

	// Append any new entries not already in the log
	for i := len(rf.log); i < len(args.Entries); i++ {
		rf.log = append(rf.log, args.Entries[i])
	}

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)

	//return true

}
