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

	remaining := exclude(cluster, leader.id)
	newLeader := waitForLeader(t, remaining, clock)

	if newLeader.id == leader.id {
		t.Fatalf("expected a new leader distinct from killed leader %d, got %d", leader.id, newLeader.id)
	}
}

func TestLogReplication(t *testing.T) {
	cluster, _, clock := newTestCluster(t, 3)
	leader := waitForLeader(t, cluster, clock)

	command := []byte("set foo bar")

	targetIndex, applyDone := submitAsync(t, leader, command)

	waitForCommit(t, cluster, clock, targetIndex)
	waitForAllApplied(t, cluster, clock)

	awaitSingleResult(t, applyDone)

	assertCommittedLogMatches(t, cluster)
	assertLogAt(t, cluster, targetIndex, command)

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

	remaining := exclude(cluster, dead.id)
	command := []byte("set foo bar")

	targetIndex, applyDone := submitAsync(t, leader, command)

	waitForCommit(t, remaining, clock, targetIndex)
	waitForAllApplied(t, remaining, clock)

	awaitSingleResult(t, applyDone)

	assertCommittedLogMatches(t, remaining)
	assertLogAt(t, remaining, targetIndex, command)

	dead.mu.Lock()
	deadLogLen := uint64(len(dead.log))
	dead.mu.Unlock()

	if deadLogLen > targetIndex {
		t.Fatalf("dead node %d should not have received entry at index %d, but its log has length %d", dead.id, targetIndex, deadLogLen)
	}
}

func TestClusterCannotCommitWithTwoNodesDown(t *testing.T) {
	cluster, _, clock := newTestCluster(t, 3)
	leader := waitForLeader(t, cluster, clock)

	for _, s := range cluster {
		if s.id != leader.id {
			killNode(s)
		}
	}

	command := []byte("set foo bar")

	targetIndex, applyDone := submitAsync(t, leader, command)

	committed := pollUntil(clock, func() bool {
		leader.mu.Lock()
		defer leader.mu.Unlock()
		return leader.commitIndex >= targetIndex
	})
	if committed {
		t.Fatalf("commit index reached %d with only 1/3 nodes alive; quorum should be unreachable", targetIndex)
	}

	select {
	case <-applyDone:
		t.Fatal("leader.Apply returned despite commit being unreachable with quorum lost")
	default:
	}

	leader.mu.Lock()
	commitIndex := leader.commitIndex
	leader.mu.Unlock()

	if commitIndex >= targetIndex {
		t.Fatalf("leader commit index is %d, want < %d", commitIndex, targetIndex)
	}
}
