package navi

import (
	"fmt"
	"testing"
	"time"
)

type noopStateMachine struct{}

func (noopStateMachine) Apply(cmd []byte) ([]byte, error) { return nil, nil }

func newTestCluster(t *testing.T, n int) ([]*Server, *MemoryNetwork, *ManualClock) {
	t.Helper()

	network := NewMemoryNetwork()
	clock := NewManualClock(time.Unix(0, 0))

	members := make([]ClusterMember, n)
	for i := range members {
		members[i] = ClusterMember{Id: uint64(i + 1), Address: fmt.Sprintf("node%d", i+1)}
	}

	cluster := make([]*Server, n)
	for i := range members {
		transport := NewMemoryTransport(network)
		s := NewServer(members, noopStateMachine{}, "", i, transport, clock)
		s.storage = NewMemStorage()
		cluster[i] = s
	}

	for _, s := range cluster {
		go s.Start()
	}

	t.Cleanup(func() {
		for _, s := range cluster {
			killNode(s)
		}
	})

	return cluster, network, clock
}

func TestKillLeaderElectsNewLeader(t *testing.T) {
	cluster, _, clock := newTestCluster(t, 3)

	leader := waitForLeader(t, cluster, clock)

	killNode(leader)

	remaining := make([]*Server, 0, len(cluster)-1)
	for _, s := range cluster {
		if s.id != leader.id {
			remaining = append(remaining, s)
		}
	}

	newLeader := waitForLeader(t, remaining, clock)

	if newLeader.id == leader.id {
		t.Fatalf("expected a new leader distinct from killed leader %d, got %d", leader.id, newLeader.id)
	}
}

func TestLogReplication(t *testing.T) {
	cluster, _, clock := newTestCluster(t, 3)
	leader := waitForLeader(t, cluster, clock)

	command := []byte("set foo bar")

	leader.mu.Lock()
	targetIndex := uint64(len(leader.log))
	leader.mu.Unlock()

	applyDone := make(chan []ApplyResult, 1)
	go func() {
		results, err := leader.Apply([][]byte{command})
		if err != nil {
			t.Errorf("leader.Apply failed: %s", err)
		}
		applyDone <- results
	}()

	waitForCommit(t, cluster, clock, targetIndex)
	waitForAllApplied(t, cluster, clock)

	results := <-applyDone
	if len(results) != 1 {
		t.Fatalf("expected 1 apply result, got %d", len(results))
	}
	if results[0].Error != nil {
		t.Fatalf("apply result error: %s", results[0].Error)
	}

	assertCommittedLogMatches(t, cluster)

	for _, s := range cluster {
		s.mu.Lock()
		got := string(s.log[targetIndex].Command)
		s.mu.Unlock()

		if got != string(command) {
			t.Fatalf("server %d log at index %d: got %q, want %q", s.id, targetIndex, got, command)
		}
	}

	leader.mu.Lock()
	leaderTerm := leader.log[targetIndex].Term
	leader.mu.Unlock()

	restored := NewServer(
		[]ClusterMember{{Id: leader.id, Address: leader.address}},
		noopStateMachine{},
		"",
		0,
		NewMemoryTransport(NewMemoryNetwork()),
		clock,
	)
	restored.storage = leader.storage
	restored.restore()

	if uint64(len(restored.log)) <= targetIndex {
		t.Fatalf("restored log too short: len %d, want > %d", len(restored.log), targetIndex)
	}
	if string(restored.log[targetIndex].Command) != string(command) {
		t.Fatalf("restored log at index %d: got %q, want %q", targetIndex, restored.log[targetIndex].Command, command)
	}
	if restored.log[targetIndex].Term != leaderTerm {
		t.Fatalf("restored log at index %d: got term %d, want %d", targetIndex, restored.log[targetIndex].Term, leaderTerm)
	}
}

func TestClusterTolerateOneNodeDown(t *testing.T) {
	cluster, _, clock := newTestCluster(t, 3)
	leader := waitForLeader(t, cluster, clock)

	var dead *Server
	for _, s := range cluster {
		if s.id != leader.id {
			dead = s
			break
		}
	}
	killNode(dead)

	remaining := make([]*Server, 0, len(cluster)-1)
	for _, s := range cluster {
		if s.id != dead.id {
			remaining = append(remaining, s)
		}
	}

	command := []byte("set foo bar")

	leader.mu.Lock()
	targetIndex := uint64(len(leader.log))
	leader.mu.Unlock()

	applyDone := make(chan []ApplyResult, 1)
	go func() {
		results, err := leader.Apply([][]byte{command})
		if err != nil {
			t.Errorf("leader.Apply failed: %s", err)
		}
		applyDone <- results
	}()

	waitForCommit(t, remaining, clock, targetIndex)
	waitForAllApplied(t, remaining, clock)

	results := <-applyDone
	if len(results) != 1 {
		t.Fatalf("expected 1 apply result, got %d", len(results))
	}
	if results[0].Error != nil {
		t.Fatalf("apply result error: %s", results[0].Error)
	}

	assertCommittedLogMatches(t, remaining)

	for _, s := range remaining {
		s.mu.Lock()
		got := string(s.log[targetIndex].Command)
		s.mu.Unlock()

		if got != string(command) {
			t.Fatalf("server %d log at index %d: got %q, want %q", s.id, targetIndex, got, command)
		}
	}

	dead.mu.Lock()
	deadLogLen := uint64(len(dead.log))
	dead.mu.Unlock()

	if deadLogLen > targetIndex {
		t.Fatalf("dead node %d should not have received entry at index %d, but its log has length %d", dead.id, targetIndex, deadLogLen)
	}
}
