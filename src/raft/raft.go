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

import ("sync"
		"labrpc"
		"time"
		"math/rand")

// import "bytes"
// import "encoding/gob"


const (
	FOLLOWER = 0
	CANDIDATE = 1
	LEADER = 2
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	CurrentTerm 	int 
	VotedFor 		int //stores the id that this server voted for during the currentTerm, -1 if didn't vote for any one
	Log 			[]LogEntry

	CommitIndex 	int // index of last log entry that is committed (replicated over the majority of the servers)
	LastApplied 	int //index of the last log entry that applied on the state machine of the server

	State 			int
	NextIndex 		[]int //index of the netry to be sent for each peer server to check matching of the terms 
	VoteCount 		int

	HeartBeatChan 	chan bool //stores the heartbeats when received by the AppendEntriesRequest
	LeaderChan 		chan bool //cotnains true if the server wins the election
}

type LogEntry struct {
	Command 	interface{}
	Term 		int //term at which the command is received by the leader
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term 			int 
	CandidateId 	int
	LastLogIndex 	int //to check if the requester log is at least up to date as the voter log
	LastLogTerm 	int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (3A).
	Term 			int
	VoteGranted 	bool
}

type AppendEntriesArgs struct { //used by the leader to send heartbeats (empty entries list) or to send entries to be appended at the followers log
	Term         		int //term of the leader to check with the follower term, if not up to date, the follower will reject the request
	LeaderId     		int 
	PrevLogIndex 		int //index and term of the match entry at the log to check if it match with the followers log
	PrevLogTerm  		int
	Entries      		[]LogEntry //entries to be appended to the log of the follower
	LeaderCommit 		int //index of last log entry commited at the leader, to let the followers update their commit index too
}
 
type AppendEntriesReply struct {
	Term 		int //term of the follower (as if it is larger than the requester term, let the requester updates its term)
	NextIndex 	int // nextindex to update the NextIndex matrix at the leader
	Success 	bool //determines if the matching check succeeded and the entries appended successfully or not
}



// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.CurrentTerm
	isleader = (rf.State == LEADER)
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
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	lastLogIndex := len(rf.Log) - 1
	lastLogTerm := rf.Log[lastLogIndex].Term

	if rf.CurrentTerm > args.Term { //if my term is more up to date, reply with false
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	} 

	if rf.CurrentTerm < args.Term { //if my term is old, update my term
		rf.mu.Lock()
		rf.State = FOLLOWER
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.mu.Unlock()
	}

	if rf.VotedFor != -1 && rf.VotedFor != args.CandidateId { //if voted for another candidate, reply with false
		reply.Term= rf.CurrentTerm
		reply.VoteGranted = false
		return

	} else { //if voted for itself (Votedfor = candidateID) or didn't vote at all (Voted for = -1)
		//check
		requesterLogUpToDate := (lastLogTerm == args.LastLogTerm  && lastLogIndex <= args.LastLogIndex) || 
								(lastLogTerm < args.LastLogTerm)

		if !requesterLogUpToDate {
			reply.Term = rf.CurrentTerm
			reply.VoteGranted = false
			return
		}
		
		if requesterLogUpToDate { //vote only if requester log is more or up to date as my log
			rf.mu.Lock()
			rf.VotedFor = args.CandidateId
			rf.State = FOLLOWER
			rf.mu.Unlock()
			reply.Term = rf.CurrentTerm
			reply.VoteGranted = true
			return
		}
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
	if ok {
		if rf.CurrentTerm < reply.Term {
			rf.mu.Lock()
			rf.CurrentTerm = reply.Term
			rf.State = FOLLOWER
			rf.VotedFor = -1
			rf.mu.Unlock()
		} else if reply.VoteGranted {
			rf.mu.Lock()
			rf.VoteCount++
			rf.mu.Unlock()
			if rf.State == CANDIDATE && rf.VoteCount > len(rf.peers)/2 {
				rf.LeaderChan <- true
				rf.mu.Lock()
				rf.VoteCount = 0 //to avoid putting multiple trues in the LeaderChan for one election 
				rf.mu.Unlock()
			}
		}
	} 
	return ok
}

func (rf *Raft) BroadcastRequestVote() {

	var args RequestVoteArgs
	args.CandidateId = rf.me
	args.Term = rf.CurrentTerm
	args.LastLogIndex = len(rf.Log) - 1
	args.LastLogTerm = rf.Log[args.LastLogIndex].Term

	for p := range rf.peers {
		if(rf.State == CANDIDATE) { //need to check each iteration as the state may change
			if p == rf.me {
				continue
			}
			go func(server int) {
				var reply RequestVoteReply
				rf.sendRequestVote(server, &args, &reply)
			}(p)
		}		
	}
}

func (rf *Raft) AppendEntriesRequest(args AppendEntriesArgs, reply *AppendEntriesReply) {

	if rf.CurrentTerm > args.Term { //if my term is more up to date than the requester Term then reject the request 
		reply.Term = rf.CurrentTerm
		reply.Success = false
		reply.NextIndex = len(rf.Log) - 1
		return
	}

	rf.HeartBeatChan <- true // if the requester(leader) term is up tp date, then it as a heartbeat from the leader

	if rf.CurrentTerm < args.Term { //if my term is out of date, update it
		rf.mu.Lock()
		rf.CurrentTerm = args.Term
		rf.State = FOLLOWER
		rf.VotedFor = -1
		rf.mu.Unlock()
	}

	if rf.State == CANDIDATE { //as the sate may be leader and it receiving an appendEntries request and the terms are equal, in this case don't change the state to follower
		rf.mu.Lock()
		rf.State = FOLLOWER
		rf.mu.Unlock()
	}
	
	reply.Term = rf.CurrentTerm
	// check index and term of the entry before replication
	if len(rf.Log) > args.PrevLogIndex && rf.Log[args.PrevLogIndex].Term == args.PrevLogTerm {
		rf.mu.Lock()
		//if the entry at the PrevLogIndex matched with the entry at the leader, then ovewrite any followwing entries at the follower by the candidate entry
		rf.Log = rf.Log[:args.PrevLogIndex+1] // ignore any entries after PREVLOGINDEX
		for i := 0; i < len(args.Entries); i++ {
			rf.Log = append(rf.Log, args.Entries[i])
		}

		if args.LeaderCommit > rf.CommitIndex {
			rf.CommitIndex = min(args.LeaderCommit, len(rf.Log) - 1) //the commit index at one of the followers shouldn't exceed the commit index of the leader
		}
		rf.mu.Unlock()
		reply.NextIndex = len(rf.Log) - 1
		reply.Success = true
	} else { //the logEntrey doesn't match with the one at the server, so need to decrement the start of the entries to append at the server
		reply.NextIndex = args.PrevLogIndex - 1
		reply.Success = false
	}
	return
}

func (rf *Raft) sendAppendEntriesRequest(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesRequest", args, reply)
	if ok {
		rf.mu.Lock()
		rf.NextIndex[server] = reply.NextIndex
		rf.mu.Unlock()
		if rf.CurrentTerm < reply.Term { //that the success is false, entries aren't applied
			rf.mu.Lock()
			rf.CurrentTerm = reply.Term
			rf.State = FOLLOWER
			rf.VotedFor = -1
			rf.mu.Unlock()
		}
	} else {
		rf.NextIndex[server] = 0 //the server failed, need to start log replication from first entry
	}
	return ok
}

func (rf *Raft) BroadcastAppendEntriesRequest() { //log consistency is maintained by sending regular heartbeats that contains the log entries to be replicated
	for p := range rf.peers {
		if p != rf.me && rf.State == LEADER{
			var args AppendEntriesArgs
			args.Term = rf.CurrentTerm
			args.LeaderId = rf.me
			args.LeaderCommit = rf.CommitIndex
			args.PrevLogIndex = rf.NextIndex[p]
			args.PrevLogTerm = rf.Log[args.PrevLogIndex].Term
			args.Entries = rf.Log[args.PrevLogIndex+1:] //copy the entries from after PrevLogIndex to the end of the log
		
			go func(server int, args AppendEntriesArgs) {
				var reply AppendEntriesReply
				rf.sendAppendEntriesRequest(server, args, &reply)
			}(p, args)
		}
	}
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
//get command from the client, add it to my log if I am the leader and send it to all other servers to append it to their logs
func (rf *Raft) Start(command interface{}) (int, int, bool) { 
	index := rf.CommitIndex //index of the last entry that is guaranteed to be replicated over majority of the servers 
	term := rf.CurrentTerm //term of this server
	var isLeader bool //true if this server is the leader
	if rf.State == LEADER { //accept commands from the user if this server is the leader only
		entry := new(LogEntry)
		entry.Command = command
		entry.Term = rf.CurrentTerm
		rf.mu.Lock()
		rf.Log = append(rf.Log, *entry) // append the entry to the client
		rf.NextIndex[rf.me] = len(rf.Log) - 1 //update my nextIndex to point for the last entry in the log
		rf.mu.Unlock()
		go rf.BroadcastAppendEntriesRequest() //takes all the not added entries and send it to the followers in form of AppendEntriesRequest
		isLeader = true
	} else {
		isLeader = false
	}
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

	// Your initialization code here (3A, 3B, 3C).
	rf.LeaderChan = make(chan bool)
	rf.HeartBeatChan = make(chan bool)
	rf.State = FOLLOWER
	rf.VotedFor = -1
	rf.CurrentTerm = 0
	if len(rf.Log) == 0 {
		firstLog := new(LogEntry)
		rf.Log = []LogEntry{*firstLog}
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			if rf.State == FOLLOWER {
				 //in the follower state , wait for a heartbeat or time out and move to the candidate state
				var electionTimeOut int
				electionTimeOut = rand.Intn(200) + 500
				select {
				case <-rf.HeartBeatChan: //this heartbeat may contains 
				case <-time.After(time.Duration(electionTimeOut) * time.Millisecond):
					if rf.State != LEADER {
						rf.mu.Lock()
						rf.State = CANDIDATE
						rf.mu.Unlock()
					}
				}

			} else if rf.State == CANDIDATE {
				//in the candidate state, increment term and start new election
				rf.StartElection()

			} else {
				//in the leader state, send heartbeats regularly
	 			//broadcast heartbeats to all other servers, 
				//don't send more thant 10 heartbeats in the second
				time.Sleep(130 * time.Millisecond)
				go rf.BroadcastAppendEntriesRequest()
			}
		}
	}()

	return rf
}

func (rf *Raft) StartElection() {
	// in the cnandidate state, increment the term and send request vote 
	rf.mu.Lock()
	rf.CurrentTerm = rf.CurrentTerm + 1 //increment my term to start a new election
	rf.VotedFor = rf.me
	rf.VoteCount = 1
	rf.mu.Unlock()
	
	go rf.BroadcastRequestVote() //broadcast request vote for all other servers

	var electionTimeOut int
	electionTimeOut = rand.Intn(200) + 500
	select {
	case  <-rf.LeaderChan: 
		//collected majority of votes and become leader
		rf.mu.Lock()
		rf.State = LEADER

		//start matching from the best possible value as I can't get the length of the followers log to start from it
		rf.NextIndex = []int{}
		for i := 0; i < len(rf.peers); i++ {
			rf.NextIndex = append(rf.NextIndex, len(rf.Log)-1)
		}
		rf.mu.Unlock()
		go rf.BroadcastAppendEntriesRequest()
		return
		
	case <-time.After(time.Duration(electionTimeOut) * time.Millisecond):
		//time out, can't collect majority votes, may split vote happen or another server win the election
		//return to follower state and then candidate state to start a new election
		if rf.State != LEADER {
			rf.mu.Lock()
			rf.State = FOLLOWER
			rf.mu.Unlock()
		}
		return

	case <- rf.HeartBeatChan:
		//receives heartBeat from valid leader, return to follower state again
		if rf.State != LEADER {
			rf.mu.Lock()
			rf.State = FOLLOWER
			rf.mu.Unlock()
		}
		return
	}
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}