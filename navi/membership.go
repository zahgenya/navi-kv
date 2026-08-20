package navi

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const configChangeMarker byte = 0xFF

var (
	ErrConfigChangeInProgress = errors.New("a config change is already in progress")
	ErrInvalidServerId        = errors.New("id must not be 0")
)

func isConfigChangeCommand(cmd []byte) bool {
	return len(cmd) > 0 && cmd[0] == configChangeMarker
}

// encodeConfigChange encodes the full new cluster membership (Id + Address
// only; nextIndex/matchIndex/votedFor are runtime-only bookkeeping and never
// serialized) as a config-change Command payload:
//
//	byte 0:      configChangeMarker
//	byte 1:      member count N (max 255)
//	per member:  8 bytes little-endian Id, 1 byte address length L, L bytes address
//
// Returns an error rather than panicking if the result would exceed
// ENTRY_SIZE-ENTRY_HEADER bytes, so callers can reject an oversized cluster
// before ever touching s.log
func encodeConfigChange(members []ClusterMember) ([]byte, error) {
	if len(members) > 255 {
		return nil, fmt.Errorf("too many cluster members to encode: %d (max 255)", len(members))
	}

	buf := []byte{configChangeMarker, byte(len(members))}
	for _, m := range members {
		if len(m.Address) > 255 {
			return nil, fmt.Errorf("address too long to encode: %d bytes (max 255): %s", len(m.Address), m.Address)
		}

		var header [9]byte
		binary.LittleEndian.PutUint64(header[:8], m.Id)
		header[8] = byte(len(m.Address))
		buf = append(buf, header[:]...)
		buf = append(buf, m.Address...)
	}

	if len(buf) > ENTRY_SIZE-ENTRY_HEADER {
		return nil, fmt.Errorf("config change too large to encode: %d bytes (max %d)", len(buf), ENTRY_SIZE-ENTRY_HEADER)
	}

	return buf, nil
}

func decodeConfigChange(cmd []byte) []ClusterMember {
	if len(cmd) < 2 || cmd[0] != configChangeMarker {
		panic(fmt.Sprintf("decodeConfigChange: not a config-change command: %x", cmd))
	}

	count := int(cmd[1])
	members := make([]ClusterMember, count)
	pos := 2
	for i := 0; i < count; i++ {
		id := binary.LittleEndian.Uint64(cmd[pos : pos+8])
		addrLen := int(cmd[pos+8])
		pos += 9
		members[i] = ClusterMember{Id: id, Address: string(cmd[pos : pos+addrLen])}
		pos += addrLen
	}

	return members
}

func mergeClusterConfig(current, incoming []ClusterMember, newMemberNextIndex, selfID, selfVotedFor uint64) []ClusterMember {
	merged := make([]ClusterMember, len(current))
	copy(merged, current)

	known := make(map[uint64]bool, len(current))
	for _, m := range current {
		known[m.Id] = true
	}

	for _, m := range incoming {
		if known[m.Id] {
			continue
		}

		votedFor := uint64(0)
		if m.Id == selfID {
			votedFor = selfVotedFor
		}

		merged = append(merged, ClusterMember{
			Id:         m.Id,
			Address:    m.Address,
			nextIndex:  newMemberNextIndex,
			matchIndex: 0,
			votedFor:   votedFor,
		})
	}

	return merged
}

func resolveClusterIndex(cluster []ClusterMember, selfID uint64) int {
	for i, m := range cluster {
		if m.Id == selfID {
			return i
		}
	}

	panic(fmt.Sprintf("self (id %d) must be present in cluster after a config-change merge", selfID))
}

func (s *Server) rebuildClusterFromLog(selfVotedFor uint64) {
	cluster := make([]ClusterMember, len(s.baseCluster))
	copy(cluster, s.baseCluster)

	for i, e := range s.log {
		if isConfigChangeCommand(e.Command) {
			incoming := decodeConfigChange(e.Command)
			cluster = mergeClusterConfig(cluster, incoming, uint64(i+1), s.id, selfVotedFor)
		}
	}

	s.cluster = cluster
	s.clusterIndex = resolveClusterIndex(s.cluster, s.id)
}

func (s *Server) AddServer(id uint64, address string) error {
	if id == 0 {
		return ErrInvalidServerId
	}

	s.mu.Lock()

	if s.state != leaderState {
		s.mu.Unlock()
		return ErrApplyToLeader
	}

	if s.pendingConfigChangeIndex != 0 {
		s.mu.Unlock()
		return ErrConfigChangeInProgress
	}

	for _, m := range s.cluster {
		if m.Id == id {
			s.mu.Unlock()
			return nil
		}
	}

	newMembers := make([]ClusterMember, len(s.cluster), len(s.cluster)+1)
	copy(newMembers, s.cluster)
	newMembers = append(newMembers, ClusterMember{Id: id, Address: address})

	payload, err := encodeConfigChange(newMembers)
	if err != nil {
		s.mu.Unlock()
		return err
	}

	resultCh, newIndex := s.appendLocked(payload)

	selfVotedFor := s.getVotedFor()
	s.cluster = mergeClusterConfig(s.cluster, newMembers, newIndex+1, s.id, selfVotedFor)
	s.clusterIndex = resolveClusterIndex(s.cluster, s.id)
	s.pendingConfigChangeIndex = newIndex

	s.persist(true, 1)
	s.mu.Unlock()

	s.appendEntries()

	res := <-resultCh
	return res.Error
}
