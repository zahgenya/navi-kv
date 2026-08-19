package navi

import (
	"testing"
	"time"
)

const (
	testTick     = 20 * time.Millisecond
	testMaxTicks = 500
)

func pollUntil(clock *ManualClock, cond func() bool) bool {
	for i := 0; i < testMaxTicks; i++ {
		if cond() {
			return true
		}
		clock.Advance(testTick)
		time.Sleep(time.Millisecond)
	}
	return cond()
}

func waitForLeader(t *testing.T, cluster []*Server, clock *ManualClock) *Server {
	t.Helper()

	var leader *Server
	ok := pollUntil(clock, func() bool {
		leader = nil
		for _, s := range cluster {
			s.mu.Lock()
			isLeader := s.state == leaderState
			s.mu.Unlock()

			if isLeader {
				if leader != nil {
					leader = nil
					return false
				}
				leader = s
			}
		}
		return leader != nil
	})

	if !ok {
		t.Fatal("no leader elected before tick budget exhausted")
	}
	return leader
}

func waitForCommit(t *testing.T, cluster []*Server, clock *ManualClock, idx uint64) {
	t.Helper()

	ok := pollUntil(clock, func() bool {
		committed := 0
		for _, s := range cluster {
			s.mu.Lock()
			ci := s.commitIndex
			s.mu.Unlock()

			if ci >= idx {
				committed++
			}
		}
		return committed >= len(cluster)/2+1
	})

	if !ok {
		t.Fatalf("commit index %d not reached by majority before tick budget exhausted", idx)
	}
}

func waitForAllApplied(t *testing.T, cluster []*Server, clock *ManualClock) {
	t.Helper()

	ok := pollUntil(clock, func() bool {
		for _, s := range cluster {
			s.mu.Lock()
			caughtUp := s.lastApplied >= s.commitIndex
			s.mu.Unlock()

			if !caughtUp {
				return false
			}
		}
		return true
	})

	if !ok {
		t.Fatal("not all servers caught up applying their commit index before tick budget exhausted")
	}
}

func submitAsync(t *testing.T, leader *Server, command []byte) (targetIndex uint64, done <-chan []ApplyResult) {
	t.Helper()

	leader.mu.Lock()
	targetIndex = uint64(len(leader.log))
	leader.mu.Unlock()

	resultCh := make(chan []ApplyResult, 1)
	go func() {
		results, err := leader.Apply([][]byte{command})
		if err != nil {
			t.Errorf("leader.Apply failed: %s", err)
		}
		resultCh <- results
	}()

	return targetIndex, resultCh
}

func awaitSingleResult(t *testing.T, done <-chan []ApplyResult) ApplyResult {
	t.Helper()

	results := <-done
	if len(results) != 1 {
		t.Fatalf("expected 1 apply result, got %d", len(results))
	}
	if results[0].Error != nil {
		t.Fatalf("apply result error: %s", results[0].Error)
	}
	return results[0]
}

func assertLogAt(t *testing.T, cluster []*Server, idx uint64, want []byte) {
	t.Helper()

	for _, s := range cluster {
		s.mu.Lock()
		got := string(s.log[idx].Command)
		s.mu.Unlock()

		if got != string(want) {
			t.Fatalf("server %d log at index %d: got %q, want %q", s.id, idx, got, want)
		}
	}
}

func exclude(cluster []*Server, id uint64) []*Server {
	remaining := make([]*Server, 0, len(cluster)-1)
	for _, s := range cluster {
		if s.id != id {
			remaining = append(remaining, s)
		}
	}
	return remaining
}

func assertCommittedLogMatches(t *testing.T, cluster []*Server) {
	t.Helper()

	type snapshot struct {
		id     uint64
		commit uint64
		log    []Entry
	}

	snapshots := make([]snapshot, len(cluster))
	for i, s := range cluster {
		s.mu.Lock()
		logCopy := make([]Entry, len(s.log))
		copy(logCopy, s.log)
		snapshots[i] = snapshot{id: s.id, commit: s.commitIndex, log: logCopy}
		s.mu.Unlock()
	}

	minCommit := snapshots[0].commit
	for _, snap := range snapshots[1:] {
		if snap.commit < minCommit {
			minCommit = snap.commit
		}
	}

	base := snapshots[0]
	for _, snap := range snapshots[1:] {
		for i := uint64(0); i <= minCommit; i++ {
			if snap.log[i].Term != base.log[i].Term {
				t.Fatalf("log mismatch between server %d and %d at index %d: term %d vs %d", base.id, snap.id, i, base.log[i].Term, snap.log[i].Term)
			}
			if string(snap.log[i].Command) != string(base.log[i].Command) {
				t.Fatalf("log mismatch between server %d and %d at index %d: command %q vs %q", base.id, snap.id, i, base.log[i].Command, snap.log[i].Command)
			}
		}
	}
}

func partitionNode(network *MemoryNetwork, s *Server) {
	network.Partition(s.address)
}

func healNode(network *MemoryNetwork, s *Server) {
	network.Heal(s.address)
}

func dropMessages(network *MemoryNetwork, from, to *Server) {
	network.Drop(from.address, to.address)
}

func restoreMessages(network *MemoryNetwork, from, to *Server) {
	network.Undrop(from.address, to.address)
}

func delayMessages(network *MemoryNetwork, d time.Duration) {
	network.Delay(d)
}

func pauseNode(t *testing.T, s *Server) {
	t.Helper()
	s.transport.(*MemoryTransport).Pause()
}

func resumeNode(t *testing.T, s *Server) {
	t.Helper()
	s.transport.(*MemoryTransport).Resume()
}

func killNode(s *Server) {
	s.Stop()
}

func restartNode(s *Server) {
	go s.Start()
}
