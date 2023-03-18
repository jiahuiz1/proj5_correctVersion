package surfstore

import (
	context "context"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"sync"
	"google.golang.org/grpc"
	"fmt"
	"math"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log         []*UpdateOperation
	commitIndex int64
	lastApplied int64
	nextIndex []int64
	matchIndex []int64

	metaStore *MetaStore

	id             int64
	peers          []string
	pendingCommits []*chan bool

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) checkForAllServers(ctx context.Context, check chan bool){
	totalWork := 0
	totalChecks := 0
	responses := make(chan bool, len(s.peers))

	for {
		for idx, addr := range s.peers {
			if int64(idx) == s.id && !s.isCrashed{
				responses <- true
				continue
			}
			go s.checkFollower(ctx, addr, responses, idx)
		}


		for {
			result := <- responses
			totalChecks++
			if result {
				totalWork++
			}
			if totalChecks == len(s.peers){
				break
			}
		}

		if totalWork > len(s.peers)/2{
			check <- true
			return
		}
	}
}

func (s *RaftSurfstore) checkFollower(ctx context.Context, addr string, responses chan bool, idx int){
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		fmt.Println(err.Error())
	}
	client := NewRaftSurfstoreClient(conn)

	var e *emptypb.Empty = new(emptypb.Empty)
	_, errs := client.SendHeartbeat(ctx, e)
	if errs == ERR_SERVER_CRASHED {
		responses <- false
	} else {
		responses <- true
	}
}


func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	if !s.isLeader{
		return nil, ERR_NOT_LEADER
	}

	check := make(chan bool)
	go s.checkForAllServers(ctx, check)

	checkResult := <- check
	if checkResult {
		res, err := s.metaStore.GetFileInfoMap(ctx, empty)
		return res, err
	}

    return nil, nil
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	if !s.isLeader{
		fmt.Println("Test Not leader")
		return nil, ERR_NOT_LEADER
	}

	check := make(chan bool)
	go s.checkForAllServers(ctx, check)

	checkResult := <- check
	if checkResult {
		fmt.Println("majority of servers working")
		res, err := s.metaStore.GetBlockStoreMap(ctx, hashes)
		return res, err
	}

	return nil, nil
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	if !s.isLeader{
		return nil, ERR_NOT_LEADER
	}

	check := make(chan bool)
	go s.checkForAllServers(ctx, check)

	checkResult := <- check
	if checkResult {
		res, err := s.metaStore.GetBlockStoreAddrs(ctx, empty)
		return res, err
	}

	return nil, nil
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
    // append entry to our log
	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	})
	commitChan := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, &commitChan)

	// send entry to all followers in parallel
	go s.sendToAllFollowersInParallel(ctx)

	// keep trying indefinitely (even after responding) ** rely on sendheartbeat
	go func(){
		for {
			var e *emptypb.Empty = new(emptypb.Empty)
			succ, _ := s.SendHeartbeat(ctx, e) // check if this is correct for sending indefinitely
			// fmt.Println(succ.Flag)
			if succ.Flag{
				fmt.Println("Success")
				lateIndex := len(s.pendingCommits) - 1
				*s.pendingCommits[lateIndex] <- true
				return
			}
		}
	}()

	// commit the entry once majority of followers have it in their log
	commit := <-commitChan

	// once committed, apply to the state machine
	if commit {
		return s.metaStore.UpdateFile(ctx, filemeta)
	}
    return nil, nil
}


func (s *RaftSurfstore) sendToAllFollowersInParallel(ctx context.Context) {
	// send entry to all my followers and count the replies
	responses := make(chan bool, len(s.peers)-1)
	// contact all the follower, send some AppendEntries call
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}

		go s.sendToFollower(ctx, addr, responses, idx)
	}

	totalResponses := 1
	totalAppends := 1

	// wait in loop for responses
	for {
		result := <-responses
		totalResponses++
		if result {
			totalAppends++
		}
		if totalResponses == len(s.peers) {
			break
		}
	}

	// majority of followers agree and replicate their logs
	if totalAppends > len(s.peers)/2 {
		// TODO put on correct channel
		lateIndex := len(s.pendingCommits) - 1
		*s.pendingCommits[lateIndex] <- true
		// TODO update commit Index correctly
		// s.commitIndex += 1 
		for _, v1 := range s.matchIndex {
			if v1 > s.commitIndex {
				majority := 0
				for _, v2 := range s.matchIndex {
					if v2 >= v1 {
						majority += 1
					}
				}

				if majority > len(s.matchIndex)/2{
					s.commitIndex = v1
				}
			} 
		}
	}
}


func (s *RaftSurfstore) sendToFollower(ctx context.Context, addr string, responses chan bool, index int) {
	var prevLogIndex int64 
	var prevLogTerm int64
	var entries []*UpdateOperation

	if s.commitIndex < 0{
		prevLogIndex = -1
		prevLogTerm = -1
	} else{
		prevLogIndex = s.nextIndex[index] - 1
		prevLogTerm = s.log[prevLogIndex].Term
	}

	// TODO check all errors
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil{
		fmt.Println("Dial to follower error: ", err.Error())
	}
	client := NewRaftSurfstoreClient(conn)

	// get follower state
	var e *emptypb.Empty = new(emptypb.Empty)
	state, errs := client.GetInternalState(ctx, e)
	// followerLog := state.Log
	// fmt.Println("Test1: ", len(followerLog))
	//fmt.Println("Test2: ", len(state.Log))
	if errs != nil {
		fmt.Println("Get internal state failed: ", errs.Error())
	}
	if state == nil {
		fmt.Println("Follower internal state nil")
	}

	// check if last log index >= nextIndex for the follower
	if state != nil && int64(len(state.Log) - 1) >= s.nextIndex[index]{
		entries = s.log[s.nextIndex[index]:]
	} else {
		entries = s.log
	}

	dummyAppendEntriesInput := AppendEntryInput{
		Term: s.term,
		// TODO put the right values
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: s.commitIndex,
	}
	res, erra := client.AppendEntries(ctx, &dummyAppendEntriesInput) // change this
	
	// TODO check output
	
	// need to be done
	// if appendEntries successful, update nextIndex, matchIndex
	// if fail, decrement nextIndex, retry
	if erra != nil {
		fmt.Println(erra.Error())
	}

	if res == nil {
		fmt.Println("Append Entries result nil")
	} else if res.Success {
		// fmt.Println("Success")
		s.nextIndex[index] = res.MatchedIndex + 1
		s.matchIndex[index] = res.MatchedIndex
		responses <- true
	} else if !res.Success{
		s.nextIndex[index] -= 1
		responses <- false
	}
	// responses <- true

}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3) 
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	// fmt.Println("Server: ", s.id)
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	// 1
	if input.Term > s.term {
		s.isLeaderMutex.Lock()
		defer s.isLeaderMutex.Unlock()
		s.isLeader = false
		s.term = input.Term
	} else if input.Term < s.term {
		// reply false(reject)
		return &AppendEntryOutput{
			ServerId: s.id,
			Term: s.term,
			Success: false,
			MatchedIndex: 0, // may change this
		}, nil
	}

	// 2
	if input.PrevLogIndex >= int64(len(s.log)) || (input.PrevLogIndex >= 0 && s.log[input.PrevLogIndex].Term != input.PrevLogTerm){
		// consistencyCheck = false
		return &AppendEntryOutput{
			ServerId: s.id,
			Term: s.term,
			Success: false,
			MatchedIndex: 0,
		}, nil
	}

	// if !consistencyCheck && s.log[s.nextIndex[s.id]-1].Term != input.Entries[s.nextIndex[s.id]-1].Term{
	// 	s.nextIndex[s.id]--
	// 	return &AppendEntryOutput{
	// 		ServerId: s.id,
	// 		Term: s.term,
	// 		Success: false,
	// 		MatchedIndex: 0, // may change this
	// 	}, nil
	// }

	// 3 : check if existing entries in follower conflict with new entries(after prevLogIndex)
	if int64(len(s.log) - 1) > input.PrevLogIndex {
		s.log = s.log[:input.PrevLogIndex+1]
	}
	

	// 4 : append entries to the log
	for i := input.PrevLogIndex+1; i < int64(len(input.Entries)); i++{
		s.log = append(s.log, input.Entries[i])
	}

	// 5: update follower's commitIndex
	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))
	}

	// fmt.Println("Check log length: ", len(s.log))

	// TODO actually check entries

	for s.lastApplied < input.LeaderCommit {
		entry := s.log[s.lastApplied+1]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		s.lastApplied++
	}

	return &AppendEntryOutput{
		ServerId: s.id,
		Term: s.term,
		Success: true,
		MatchedIndex: int64(len(s.log)-1),
	}, nil

}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED // check if this is correct
	}
	
	fmt.Printf("Server %d is the leader\n", s.id)
	s.isLeaderMutex.Lock()
	defer s.isLeaderMutex.Unlock()
	s.isLeader = true
	s.term++

	// update state	

	// s.lastApplied = int64(0)
	nextIndex := make([]int64, 0)
	matchIndex := make([]int64, 0)
	for i := 0; i < len(s.peers); i++{
		nextIndex = append(nextIndex, int64(len(s.log))) // check this
		matchIndex = append(matchIndex, int64(0))
	}
	s.nextIndex = nextIndex
	s.matchIndex = matchIndex

    return nil, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED 
	}

	if !s.isLeader {
		return &Success{Flag: false}, ERR_NOT_LEADER;
	}
	var errt error
	var success bool = true
	// contact all the follower, send some AppendEntries call
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}

		var prevLogIndex int64 
		var prevLogTerm int64
		var entries []*UpdateOperation
	
		if s.commitIndex < 0{
			prevLogIndex = -1 
			prevLogTerm = -1
		} else{
			prevLogIndex = s.nextIndex[idx] - 1
			prevLogTerm = s.log[prevLogIndex].Term
		}

		// TODO check all errors
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil{
			fmt.Println("Dial to follower error: ", err.Error())
			errt = err 
			success = false
			continue
		}

		client := NewRaftSurfstoreClient(conn)

		// get follower's internal state
		var e *emptypb.Empty = new(emptypb.Empty)
		state, _ := client.GetInternalState(ctx, e)
		// if errs != nil {
		// 	fmt.Println("Get internal state failed: ", errs.Error())
		// }
		// if state == nil {
		// 	fmt.Println("Follower internal state nil")
		// }

		if state != nil && int64(len(state.Log) - 1) >= s.nextIndex[idx] {
			entries = s.log[s.nextIndex[idx]:]
		} else {
			entries = s.log
		}

		dummyAppendEntriesInput := AppendEntryInput{
			Term: s.term,
			// TODO put the right values
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: s.commitIndex,
		}

		res, _ := client.AppendEntries(ctx, &dummyAppendEntriesInput)
		// if errc != nil{
		// 	fmt.Println("Append Entries error:", errc.Error())
		// 	success = false
		// }
		if res == nil {
			//fmt.Println("Append Entries result nil")
			success = false
		} else if res.Success {
			s.nextIndex[idx] = res.MatchedIndex + 1
			s.matchIndex[idx] = res.MatchedIndex
		} else if !res.Success{
			s.nextIndex[idx] -= 1
			success = false
		}
	}

	return &Success{Flag: success}, errt;
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
