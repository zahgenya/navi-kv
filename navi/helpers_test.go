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

// Fault injection helpers. Require servers built with a MemoryTransport
// sharing one MemoryNetwork (see transport_memory.go).

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

// pauseNode makes s stop responding to RPCs, as if hung, without killing its
// background loop. resumeNode undoes it.
func pauseNode(t *testing.T, s *Server) {
	t.Helper()
	s.transport.(*MemoryTransport).Pause()
}

func resumeNode(t *testing.T, s *Server) {
	t.Helper()
	s.transport.(*MemoryTransport).Resume()
}

// killNode simulates a crash: stops the background loop and closes the
// transport. restartNode simulates recovery on the same Server, reloading
// state via restore().
func killNode(s *Server) {
	s.Stop()
}

func restartNode(s *Server) {
	go s.Start()
}
