package navi

import (
	"time"
)

func (s *Server) resetElectionTimeout() {
	interval := time.Duration(s.rand.Intn(s.heartbeatMs*2) + s.heartbeatMs*2)
	s.electionTimeout = s.clock.Now().Add(interval * time.Millisecond)
	s.debugf("new interval: %s", interval*time.Millisecond)
}

func (s *Server) timeout() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.Passive {
		return
	}

	hasTimedOut := s.clock.Now().After(s.electionTimeout)
	if hasTimedOut {
		s.warn("timed out, starting new election")
		s.state = candidateState
		s.currentTerm++
		for i := range s.cluster {
			if i == s.clusterIndex {
				s.cluster[i].votedFor = s.id
			} else {
				s.cluster[i].votedFor = 0
			}
		}

		s.resetElectionTimeout()
		s.persist(false, 0)
		s.requestVote()
	}
}

type RequestVoteRequest struct {
	RPCMessage
	// candidate requesting vote
	CandidateId uint64
	// index of candidate's last log entry
	LastLogIndex uint64
	// term of candidate's last log entry
	LastLogTerm uint64
}

type RequestVoteResponse struct {
	RPCMessage
	// true means candidate got vote
	VoteGranted bool
}

func (s *Server) requestVote() {
	for i := range s.cluster {
		if i == s.clusterIndex {
			continue
		}

		go func() {
			s.mu.Lock()

			s.debugf("requesting vote from %d", s.cluster[i].Id)

			lastLogIndex := uint64(len(s.log) - 1)
			lastLogTerm := s.log[len(s.log)-1].Term
			address := s.cluster[i].Address
			id := s.cluster[i].Id

			req := RequestVoteRequest{
				RPCMessage: RPCMessage{
					Term: s.currentTerm,
				},
				CandidateId:  s.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			s.mu.Unlock()

			var rsp RequestVoteResponse
			if err := s.transport.RequestVote(address, req, &rsp); err != nil {
				s.warnf("error calling RequestVote on %d: %s", id, err)
				return
			}

			s.mu.Lock()
			defer s.mu.Unlock()

			if s.updateTerm(rsp.RPCMessage) {
				return
			}

			dropStaleResponse := rsp.Term != req.Term
			if dropStaleResponse {
				return
			}

			if rsp.VoteGranted {
				s.debugf("vote granted by %d", s.cluster[i].Id)
				s.cluster[i].votedFor = s.id
			}
		}()
	}
}

func (s *Server) updateTerm(msg RPCMessage) bool {
	transitioned := false
	if msg.Term > s.currentTerm {
		s.currentTerm = msg.Term
		s.state = followerState
		s.setVotedFor(0)
		s.pendingConfigChangeIndex = 0
		transitioned = true
		s.warn("transitioned to follower")
		s.resetElectionTimeout()
		s.persist(false, 0)
	}
	return transitioned
}

func (s *Server) HandleRequestVoteRequest(req RequestVoteRequest, rsp *RequestVoteResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.updateTerm(req.RPCMessage)

	s.debugf("recieved vote request message from %d", req.CandidateId)

	rsp.VoteGranted = false
	rsp.Term = s.currentTerm

	if req.Term < s.currentTerm {
		s.warnf("not granting vote request from %d.", req.CandidateId)
		ServerAssert(s, "VoteGranted = false", rsp.VoteGranted, false)
		return nil
	}

	lastLogTerm := s.log[len(s.log)-1].Term
	logLen := uint64(len(s.log) - 1)
	logOk := req.LastLogTerm > lastLogTerm ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= logLen)
	grant := req.Term == s.currentTerm &&
		logOk &&
		(s.getVotedFor() == 0 || s.getVotedFor() == req.CandidateId)
	if grant {
		s.debugf("voted for %d", req.CandidateId)
		s.setVotedFor(req.CandidateId)
		rsp.VoteGranted = true
		s.resetElectionTimeout()
		s.persist(false, 0)
	} else {
		s.warnf("not granting vote request from %d", +req.CandidateId)
	}

	return nil
}

func (s *Server) becomeLeader() {
	s.mu.Lock()
	defer s.mu.Unlock()

	quorum := len(s.cluster)/2 + 1
	for i := range s.cluster {
		if s.cluster[i].votedFor == s.id && quorum > 0 {
			quorum--
		}
	}

	if quorum == 0 {
		// reset all cluster state
		for i := range s.cluster {
			s.cluster[i].nextIndex = uint64(len(s.log) + 1)
			// even match index is reset. Address Figure 2 from Raft
			// it shows both new index and match index are reset after every election
			s.cluster[i].matchIndex = 0
		}

		s.debug("new leader")
		s.state = leaderState

		// ref: Section 8 Client Interaction:
		// First a leader must have the latest information
		// on which entries are committed. The Leader
		// Completeness Property gurantees that a leader
		// has all committed entries, but at the
		// start of its term, it may not know which those are.
		// To find out, it needs to commit an entry
		// from its term. Raft handles this by having each
		// leader commit a blank no-op entry into the log
		// at the start of its term
		s.log = append(s.log, Entry{Term: s.currentTerm, Command: nil})
		s.persist(true, 1)

		s.heartbeatTimeout = s.clock.Now()
	}
}
