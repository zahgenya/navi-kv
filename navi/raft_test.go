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
