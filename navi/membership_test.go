package navi

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestEncodeDecodeConfigChangeRoundTrip(t *testing.T) {
	members := []ClusterMember{
		{Id: 1, Address: "node1:9001"},
		{Id: 2, Address: "node2:9002"},
		{Id: 3, Address: "node3:9003"},
	}

	payload, err := encodeConfigChange(members)
	if err != nil {
		t.Fatalf("encode failed: %s", err)
	}
	if !isConfigChangeCommand(payload) {
		t.Fatal("encoded payload not recognized as config-change command")
	}

	decoded := decodeConfigChange(payload)
	if len(decoded) != len(members) {
		t.Fatalf("got %d members, want %d", len(decoded), len(members))
	}
	for i, m := range members {
		if decoded[i].Id != m.Id || decoded[i].Address != m.Address {
			t.Fatalf("member %d: got %+v, want %+v", i, decoded[i], m)
		}
	}
}

func TestEncodeConfigChangeTooLarge(t *testing.T) {
	members := make([]ClusterMember, 20)
	for i := range members {
		members[i] = ClusterMember{Id: uint64(i + 1), Address: fmt.Sprintf("node%d:9000", i+1)}
	}

	if _, err := encodeConfigChange(members); err == nil {
		t.Fatal("expected error for oversized config change, got nil")
	}
}

func TestMergeClusterConfigPreservesBookkeeping(t *testing.T) {
	current := []ClusterMember{
		{Id: 1, Address: "node1", nextIndex: 5, matchIndex: 4, votedFor: 1},
		{Id: 2, Address: "node2", nextIndex: 3, matchIndex: 2, votedFor: 0},
	}
	incoming := []ClusterMember{
		{Id: 1, Address: "node1"},
		{Id: 2, Address: "node2"},
		{Id: 3, Address: "node3"},
	}

	merged := mergeClusterConfig(current, incoming, 7, 1, 1)

	if len(merged) != 3 {
		t.Fatalf("got %d members, want 3", len(merged))
	}
	if merged[0] != current[0] {
		t.Fatalf("existing member 1 bookkeeping changed: got %+v, want %+v", merged[0], current[0])
	}
	if merged[1] != current[1] {
		t.Fatalf("existing member 2 bookkeeping changed: got %+v, want %+v", merged[1], current[1])
	}

	want := ClusterMember{Id: 3, Address: "node3", nextIndex: 7, matchIndex: 0, votedFor: 0}
	if merged[2] != want {
		t.Fatalf("new member: got %+v, want %+v", merged[2], want)
	}
}

func TestAddServerReplicatesAndCommits(t *testing.T) {
	cluster, network, clock := newTestCluster(t, 3)
	leader := waitForLeader(t, cluster, clock)

	joining := NewServer([]ClusterMember{{Id: 4, Address: "node4"}}, noopStateMachine{}, "", 0, NewMemoryTransport(network), clock)
	joining.storage = NewMemStorage()
	joining.Passive = true
	go joining.Start()
	t.Cleanup(func() { killNode(joining) })

	addErrCh := make(chan error, 1)
	go func() { addErrCh <- leader.AddServer(4, "node4") }()

	full := append(append([]*Server{}, cluster...), joining)
	ok := pollUntil(clock, func() bool {
		for _, s := range full {
			s.mu.Lock()
			n := len(s.cluster)
			s.mu.Unlock()
			if n != 4 {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatal("not all nodes converged on a 4-member cluster")
	}

	if err := <-addErrCh; err != nil {
		t.Fatalf("AddServer failed: %s", err)
	}

	joining.mu.Lock()
	gotId := joining.cluster[joining.clusterIndex].Id
	joining.mu.Unlock()
	if gotId != 4 {
		t.Fatalf("joining node's self id resolved incorrectly: got %d, want 4", gotId)
	}
}

func TestAddServerRejectsSecondInFlight(t *testing.T) {
	cluster, _, clock := newTestCluster(t, 3)
	leader := waitForLeader(t, cluster, clock)

	followers := exclude(cluster, leader.id)
	for _, f := range followers {
		pauseNode(t, f)
	}
	t.Cleanup(func() {
		for _, f := range followers {
			resumeNode(t, f)
		}
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- leader.AddServer(4, "node4")
	}()

	pending := pollUntil(clock, func() bool {
		leader.mu.Lock()
		defer leader.mu.Unlock()
		return leader.pendingConfigChangeIndex != 0
	})
	if !pending {
		t.Fatal("first AddServer never appended its entry")
	}

	if err := leader.AddServer(5, "node5"); !errors.Is(err, ErrConfigChangeInProgress) {
		t.Fatalf("expected ErrConfigChangeInProgress, got %v", err)
	}

	for _, f := range followers {
		resumeNode(t, f)
	}

	committed := pollUntil(clock, func() bool {
		leader.mu.Lock()
		defer leader.mu.Unlock()
		return leader.pendingConfigChangeIndex == 0
	})
	if !committed {
		t.Fatal("first AddServer never committed once followers resumed")
	}
	if err := <-errCh; err != nil {
		t.Fatalf("first AddServer failed once unblocked: %s", err)
	}
}

func TestAddServerNonLeaderRejected(t *testing.T) {
	cluster, _, clock := newTestCluster(t, 3)
	leader := waitForLeader(t, cluster, clock)
	follower := exclude(cluster, leader.id)[0]

	if err := follower.AddServer(4, "node4"); !errors.Is(err, ErrApplyToLeader) {
		t.Fatalf("expected ErrApplyToLeader, got %v", err)
	}
}

func TestTruncationRevertsClusterMembership(t *testing.T) {
	network := NewMemoryNetwork()
	clock := NewManualClock(time.Unix(0, 0))

	base := []ClusterMember{{Id: 1, Address: "leader"}, {Id: 2, Address: "follower"}}
	follower := NewServer(base, noopStateMachine{}, "", 1, NewMemoryTransport(network), clock)
	follower.storage = NewMemStorage()
	follower.restore()

	follower.mu.Lock()
	follower.state = followerState
	follower.currentTerm = 1
	follower.mu.Unlock()

	configPayload, err := encodeConfigChange(append(append([]ClusterMember{}, base...), ClusterMember{Id: 3, Address: "new"}))
	if err != nil {
		t.Fatalf("encodeConfigChange failed: %s", err)
	}

	var rsp AppendEntriesResponse
	err = follower.HandleAppendEntriesRequest(AppendEntriesRequest{
		RPCMessage:   RPCMessage{Term: 1},
		LeaderId:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []Entry{{Term: 1, Command: configPayload}},
	}, &rsp)
	if err != nil || !rsp.Success {
		t.Fatalf("first AppendEntries failed: err=%v, success=%v", err, rsp.Success)
	}

	follower.mu.Lock()
	gotLen := len(follower.cluster)
	follower.mu.Unlock()
	if gotLen != 3 {
		t.Fatalf("after config-change entry appended: got %d members, want 3", gotLen)
	}

	// A new leader at a higher term, whose log at index 1 diverges (never saw
	// the config-change entry), forces a truncate-on-conflict.
	err = follower.HandleAppendEntriesRequest(AppendEntriesRequest{
		RPCMessage:   RPCMessage{Term: 2},
		LeaderId:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []Entry{{Term: 2, Command: []byte("unrelated command")}},
	}, &rsp)
	if err != nil || !rsp.Success {
		t.Fatalf("second AppendEntries failed: err=%v, success=%v", err, rsp.Success)
	}

	follower.mu.Lock()
	gotCluster := append([]ClusterMember{}, follower.cluster...)
	follower.mu.Unlock()

	if len(gotCluster) != 2 {
		t.Fatalf("after truncation: got %d members, want 2 (reverted to base): %+v", len(gotCluster), gotCluster)
	}
	for i, m := range base {
		if gotCluster[i].Id != m.Id || gotCluster[i].Address != m.Address {
			t.Fatalf("after truncation, member %d: got %+v, want %+v", i, gotCluster[i], m)
		}
	}
}

func TestRestorePreservesClusterMembership(t *testing.T) {
	cluster, _, clock := newTestCluster(t, 3)
	leader := waitForLeader(t, cluster, clock)

	addErrCh := make(chan error, 1)
	go func() { addErrCh <- leader.AddServer(4, "node4") }()

	ok := pollUntil(clock, func() bool {
		leader.mu.Lock()
		defer leader.mu.Unlock()
		return leader.pendingConfigChangeIndex == 0 && len(leader.cluster) == 4
	})
	if !ok {
		t.Fatal("AddServer never committed")
	}
	if err := <-addErrCh; err != nil {
		t.Fatalf("AddServer failed: %s", err)
	}

	founding := make([]ClusterMember, len(cluster))
	leaderIndex := -1
	for i, s := range cluster {
		founding[i] = ClusterMember{Id: s.id, Address: s.address}
		if s.id == leader.id {
			leaderIndex = i
		}
	}

	restored := NewServer(founding, noopStateMachine{}, "", leaderIndex, NewMemoryTransport(NewMemoryNetwork()), clock)
	restored.storage = leader.storage
	restored.restore()

	if len(restored.cluster) != 4 {
		t.Fatalf("restored cluster has %d members, want 4: %+v", len(restored.cluster), restored.cluster)
	}

	found := false
	for _, m := range restored.cluster {
		if m.Id == 4 && m.Address == "node4" {
			found = true
		}
	}
	if !found {
		t.Fatalf("restored cluster missing added member 4: %+v", restored.cluster)
	}
}

func TestPassiveNodeDoesNotSelfElect(t *testing.T) {
	network := NewMemoryNetwork()
	clock := NewManualClock(time.Unix(0, 0))

	s := NewServer([]ClusterMember{{Id: 9, Address: "solo"}}, noopStateMachine{}, "", 0, NewMemoryTransport(network), clock)
	s.storage = NewMemStorage()
	s.Passive = true
	go s.Start()
	t.Cleanup(func() { killNode(s) })

	for i := 0; i < 50; i++ {
		clock.Advance(2 * time.Second)
		time.Sleep(time.Millisecond)
	}

	s.mu.Lock()
	state := s.state
	s.mu.Unlock()

	if state == leaderState {
		t.Fatal("passive node self-elected leader while waiting to be added")
	}
}
