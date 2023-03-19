package SurfTest

import (
	"cse224/proj5/pkg/surfstore"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"testing"
	"time"
	"fmt"
)

func TestRaftSetLeader(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state == nil {
			t.Fatalf("Could not get state")
		}
		if state.Term != int64(1) {
			t.Fatalf("Server %d should be in term %d", idx, 1)
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Fatalf("Server %d should be the leader", idx)
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Fatalf("Server %d should not be the leader", idx)
			}
		}
	}

	leaderIdx = 2
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state == nil {
			t.Fatalf("Could not get state")
		}
		if state.Term != int64(2) {
			t.Fatalf("Server should be in term %d", 2)
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Fatalf("Server %d should be the leader", idx)
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Fatalf("Server %d should not be the leader", idx)
			}
		}
	}
}

func TestRaftFollowersGetUpdates(t *testing.T) {
	t.Log("leader1 gets a request")
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// set node 0 to be the leader
	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// here node 0 should be the leader, all other nodes should be followers
	// all logs and metastores should be empty
	// ^ TODO check

	// update a file on node 0
	val := []string{"1", "2"}
	filemeta := &surfstore.FileMetaData{
		Filename:      "testfile",
		Version:       1,
		BlockHashList: val,
	}

	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// one final call to sendheartbeat (from spec)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// check that all the nodes are in the right state
	// leader and all followers should have the entry in the log
	// everything should be term 1
	// entry should be applied to all metastores
	// only node 0 should be leader
	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenMeta[filemeta.Filename] = filemeta

	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta,
	})

	term := int64(1)
	var leader bool
	for idx, server := range test.Clients {
		if idx == leaderIdx {
			leader = bool(true)
		} else {
			leader = bool(false)
		}

		_, err := CheckInternalState(&leader, &term, goldenLog, goldenMeta, server, test.Context)

		if err != nil {
			t.Fatalf("Error checking state for server %d: %s", idx, err.Error())
		}
	}
}

func TestRaftServerIsCrashable(t *testing.T){
	t.Log("a request is sent to a crashed server.")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	leaderIdx := 0

	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].Crash(test.Context, &emptypb.Empty{})

	_, err := test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	if err == nil {
		fmt.Println("test:", err.Error())
		t.Fatalf("Server should return ERR_SERVER_CRASHED")
	}
}

func TestRaftRecoverable(t *testing.T){
	t.Log("leader1 gets a request while all other nodes are crashed. the crashed nodes recover.")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	leaderIdx := 0
	fileMeta1 := &surfstore.FileMetaData{
		Filename:      "testfile1",
		Version:       1,
		BlockHashList: nil,
	}


	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

	test.Clients[leaderIdx].UpdateFile(test.Context, fileMeta1)
	test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	_, err := test.Clients[leaderIdx].UpdateFile(test.Context, fileMeta1)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	
	if err != nil {
		t.Fatalf("Did not complete request")
	}
}

func TestRaftLogsConsistent(t *testing.T) {
	t.Log("leader1 gets a request while a minority of the cluster is down. leader1 crashes. the other crashed nodes are restored. leader2 gets a request. leader1 is restored.")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	leaderIdx := 0
	fileMeta1 := &surfstore.FileMetaData{
		Filename:      "testfile1",
		Version:       1,
		BlockHashList: nil,
	}
	fileMeta2 := &surfstore.FileMetaData{
		Filename:      "testfile2",
		Version:       1,
		BlockHashList: nil,
	}
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

	version, err := test.Clients[leaderIdx].UpdateFile(test.Context, fileMeta1)
	if err != nil || version.Version != 1 {
		t.FailNow()
	}
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	test.Clients[leaderIdx].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})
	leaderIdx = 1
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	v2, err := test.Clients[leaderIdx].UpdateFile(test.Context, fileMeta2)
	if err != nil || v2.Version != 1 {
		t.FailNow()
	}

	test.Clients[0].Restore(test.Context, &emptypb.Empty{})

	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenMeta[fileMeta1.Filename] = fileMeta1
	goldenMeta[fileMeta2.Filename] = fileMeta2
	goldenLog := make([]*surfstore.UpdateOperation, 0)

	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: fileMeta1,
	})
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         2,
		FileMetaData: fileMeta2,
	})

	term := int64(2)
	for idx, server := range test.Clients {
		_, err := CheckInternalState(nil, &term, goldenLog, goldenMeta, server, test.Context)
		if err != nil {
			t.Fatalf("Error checking state for server %d: %s", idx, err.Error())
		}
	}
}

func TestRaftNewLeaderPushesUpdates(t *testing.T) {
	t.Log("leader1 gets a request while the majority of the cluster is down. leader1 crashes. the other nodes come back. leader2 is elected")
	cfgPath := "./config_files/5nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)
	leaderIdx := 0
	fileMeta1 := &surfstore.FileMetaData{
		Filename:      "testfile1",
		Version:       1,
		BlockHashList: nil,
	}
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})
	test.Clients[3].Crash(test.Context, &emptypb.Empty{})

	go test.Clients[leaderIdx].UpdateFile(test.Context, fileMeta1)
	// test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	time.Sleep(100 * time.Millisecond)

	test.Clients[leaderIdx].Crash(test.Context, &emptypb.Empty{})
	test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})
	test.Clients[3].Restore(test.Context, &emptypb.Empty{})

	leaderIdx = 4
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{Term: 1, FileMetaData: fileMeta1})
	for idx, server := range test.Clients {
		var term int64
		if idx == 0 {
			term = 1
		} else {
			term = 2
		}
		_, err := CheckInternalState(nil, &term, goldenLog, goldenMeta, server, test.Context)
		if err != nil {
			t.Fatalf("Error checking state of server %d: %s", idx, err.Error())
		}
	}
}