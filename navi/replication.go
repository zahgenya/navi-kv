package navi

import (
	"errors"
	"sync"
	"time"
)

var ErrApplyToLeader = errors.New("cannot apply message to follower, apply to leader")

func (s *Server) Apply(commands [][]byte) ([]ApplyResult, error) {
	s.mu.Lock()
	if s.state != leaderState {
		s.mu.Unlock()
		return nil, ErrApplyToLeader
	}
	s.debugf("processing %d new entry", len(commands))

	resultChans := make([]chan ApplyResult, len(commands))
	for i, command := range commands {
		resultChans[i] = make(chan ApplyResult)
		s.log = append(s.log, Entry{
			Term:    s.currentTerm,
			Command: command,
			result:  resultChans[i],
		})
	}

	s.persist(true, len(commands))
	s.debug("waiting to be applied")
	s.mu.Unlock()

	s.appendEntries()
	// TODO: what to do if it takes too long
	results := make([]ApplyResult, len(commands))
	var wg sync.WaitGroup
	wg.Add(len(commands))
	for i, ch := range resultChans {
		go func(i int, c chan ApplyResult) {
			results[i] = <-c
			wg.Done()
		}(i, ch)
	}

	wg.Wait()

	return results, nil
}

type AppendEntriesRequest struct {
	RPCMessage

	// so follower can redirect clients
	LeaderId uint64

	// index of log entry immediately preceding new ones
	PrevLogIndex uint64

	// term of prevLogIndex entry
	PrevLogTerm uint64

	// log entries to store. Empty for heartbeat
	Entries []Entry

	// leader's commitIndex
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	RPCMessage

	Success bool
}

const MAX_APPEND_ENTRIES_BATCH = 8_000

func (s *Server) appendEntries() {
	for i := range s.cluster {
		// no need to send message to self
		if i == s.clusterIndex {
			continue
		}

		go func(i int) {
			s.mu.Lock()

			next := s.cluster[i].nextIndex
			prevLogIndex := next - 1
			prevLogTerm := s.log[prevLogIndex].Term
			address := s.cluster[i].Address

			var entries []Entry
			if uint64(len(s.log)-1) >= s.cluster[i].nextIndex {
				s.debugf("len: %d, next: %d, server: %d", len(s.log), next, s.cluster[i].Id)
				entries = s.log[next:]
			}

			// keep latency down by only applying N at a time
			if len(entries) > MAX_APPEND_ENTRIES_BATCH {
				entries = entries[:MAX_APPEND_ENTRIES_BATCH]
			}

			lenEntries := uint64(len(entries))
			req := AppendEntriesRequest{
				RPCMessage: RPCMessage{
					Term: s.currentTerm,
				},
				LeaderId:     s.cluster[s.clusterIndex].Id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: s.commitIndex,
			}

			s.mu.Unlock()

			var rsp AppendEntriesResponse
			s.debugf("sending %d entries to %d for term %d", len(entries), s.cluster[i].Id, req.Term)
			if err := s.transport.AppendEntries(address, req, &rsp); err != nil {
				s.warnf("error calling AppendEntries on %d: %s", s.cluster[i].Id, err)
				return
			}

			s.mu.Lock()
			defer s.mu.Unlock()
			if s.updateTerm(rsp.RPCMessage) {
				return
			}

			dropStaleResponse := rsp.Term != req.Term && s.state == leaderState
			if dropStaleResponse {
				return
			}

			if rsp.Success {
				prev := s.cluster[i].nextIndex
				s.cluster[i].nextIndex = max(req.PrevLogIndex+lenEntries+1, 1)
				s.cluster[i].matchIndex = s.cluster[i].nextIndex - 1
				s.debugf("message accepted for %d. Prev Index: %d, Next Index: %d, Match Index: %d", s.cluster[i].Id, prev, s.cluster[i].nextIndex, s.cluster[i].matchIndex)
			} else {
				s.cluster[i].nextIndex = max(s.cluster[i].nextIndex-1, 1)
				s.warnf("forced to go back to %d for: %d", s.cluster[i].nextIndex, s.cluster[i].Id)
			}
		}(i)
	}
}

func (s *Server) HandleAppendEntriesRequest(req AppendEntriesRequest, rsp *AppendEntriesResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.updateTerm(req.RPCMessage)

	// if AppendEntries RPC received from new leader: convert to follower
	if req.Term == s.currentTerm && s.state == candidateState {
		s.state = followerState
	}

	rsp.Term = s.currentTerm
	rsp.Success = false

	if s.state != followerState {
		s.warnf("non-follower cannot append entries")
		return nil
	}

	if req.Term < s.currentTerm {
		s.warnf("dropping request from old leader %d: term %d", req.LeaderId, req.Term)
		return nil
	}

	s.resetElectionTimeout()

	logLen := uint64(len(s.log))
	validPreviousLog := req.PrevLogIndex == 0 || // induction step
		(req.PrevLogIndex < logLen &&
			s.log[req.PrevLogIndex].Term == req.PrevLogTerm)
	if !validPreviousLog {
		s.warnf("not a valid log")
		return nil
	}

	next := req.PrevLogIndex + 1
	nNewEntries := 0

	for i := next; i < next+uint64(len(req.Entries)); i++ {
		e := req.Entries[i-next]
		// result must stay nil on follower entries (see advanceCommitIndex):
		// MemoryTransport delivers Entry by direct struct copy, so without this,
		// e.result still aliases the leader's channel and both nodes' apply
		// loops race to send on it, permanently wedging the loser's s.mu.
		e.result = nil
		if i >= uint64(cap(s.log)) {
			newTotal := next + uint64(len(req.Entries))
			// second argument must be `i`
			// not `0` otherwise the copy after this
			// doesn't work.
			// Only copy until `i`, not `newTotal` since
			// we'll continue appending after this.
			newLog := make([]Entry, i, newTotal*2)
			copy(newLog, s.log)
			s.log = newLog
		}

		if i < uint64(len(s.log)) && s.log[i].Term != e.Term {
			prevCap := cap(s.log)
			// if an existing entry conflicts with a new
			// one(same index but different terms),
			// delete the existing entry and all that follow it
			s.log = s.log[:i]
			ServerAssert(s, "capacity remains the same while we truncate", cap(s.log), prevCap)
		}

		s.debugf("appending entry: %s. At index: %d", string(e.Command), len(s.log))

		if i < uint64(len(s.log)) {
			ServerAssert(s, "existing log is the same as new log", s.log[i].Term, e.Term)
		} else {
			s.log = append(s.log, e)
			ServerAssert(s, "length is directly related to the index", uint64(len(s.log)), i+1)
			nNewEntries++
		}
	}

	if req.LeaderCommit > s.commitIndex {
		s.commitIndex = min(req.LeaderCommit, uint64(len(s.log)-1))
	}

	s.persist(nNewEntries != 0, nNewEntries)

	rsp.Success = true
	return nil
}

func (s *Server) advanceCommitIndex() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state == leaderState {
		lastLogIndex := uint64(len(s.log) - 1)

		for i := lastLogIndex; i > s.commitIndex; i-- {
			quorum := len(s.cluster)/2 + 1

			for j := range s.cluster {
				if quorum == 0 {
					break
				}

				isLeader := j == s.clusterIndex
				if s.cluster[j].matchIndex >= i || isLeader {
					quorum--
				}
			}

			if quorum == 0 {
				s.commitIndex = i
				s.debugf("new commit index: %d", i)
				break
			}
		}
	}

	if s.lastApplied <= s.commitIndex {
		log := s.log[s.lastApplied]

		if len(log.Command) != 0 {
			s.debugf("entry applied: %d", s.lastApplied)
			res, err := s.statemachine.Apply(log.Command)
			if err != nil {
				s.errorf("apply failed at index %d: %s", s.lastApplied, err)
			}

			// will be nil for follower entries and for no op entries.
			// Not nil for all user submitted messages
			if log.result != nil {
				log.result <- ApplyResult{
					Result: res,
					Error:  err,
				}
			}
		}

		s.lastApplied++
	}
}

func (s *Server) heartbeat() {
	s.mu.Lock()
	defer s.mu.Unlock()

	timeForHeartbeat := s.clock.Now().After(s.heartbeatTimeout)
	if timeForHeartbeat {
		s.heartbeatTimeout = s.clock.Now().Add(time.Duration(s.heartbeatMs) * time.Millisecond)
		s.debugf("sending heartbeat")
		s.appendEntries()
	}
}
