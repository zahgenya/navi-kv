package navi

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

// Writing our own encoding with simple binary encoding.
// encoding/gob uses 68 bytes to store that data for when
// B has two entries. If we wrote the encoder/decoder ourselves
// we could store that struct in 33 bytes:
// (8 (sizeof(A)) + 8 (sizeof(len(B))) + 16 (len(B) * sizeof(B)) + 1 (sizeof(C)))

const (
	PAGE_SIZE    = 4096
	ENTRY_HEADER = 16
	ENTRY_SIZE   = 128
)

// must be called within mutex lock
func (s *Server) getVotedFor() uint64 {
	for i := range s.cluster {
		if i == s.clusterIndex {
			return s.cluster[i].votedFor
		}
	}

	ServerAssert(s, "invalid cluster", true, false)
	return 0
}

// must be called within mutex lock
func (s *Server) persist(writeLog bool, nNewEntries int) {
	t := time.Now()

	if nNewEntries == 0 && writeLog {
		nNewEntries = len(s.log)
	}

	s.storage.Seek(0, 0)

	var page [PAGE_SIZE]byte
	// bytes 0-8: current term
	// bytes 8-16: voted for
	// bytes 16-24: log length
	// bytes: 4096-N: log

	binary.LittleEndian.PutUint64(page[:8], s.currentTerm)
	binary.LittleEndian.PutUint64(page[8:16], s.getVotedFor())
	binary.LittleEndian.PutUint64(page[16:24], uint64(len(s.log)))
	n, err := s.storage.Write(page[:])
	if err != nil {
		panic(err)
	}
	ServerAssert(s, "Wrote full page", n, PAGE_SIZE)

	if writeLog && nNewEntries > 0 {
		newLogOffset := max(len(s.log)-nNewEntries, 0)

		s.storage.Seek(int64(PAGE_SIZE+ENTRY_SIZE*newLogOffset), 0)
		bw := bufio.NewWriter(s.storage)

		var entryBytes [ENTRY_SIZE]byte
		for i := newLogOffset; i < len(s.log); i++ {
			// bytes 0-8: entry term
			// bytes 8-16: entry command length
			// bytes 16-ENTRY_SIZE: entry command

			if len(s.log[i].Command) > ENTRY_SIZE-ENTRY_HEADER {
				panic(fmt.Sprintf("Command is too large (%d). Must be at most %d bytes", len(s.log[i].Command), ENTRY_SIZE-ENTRY_HEADER))
			}

			binary.LittleEndian.PutUint64(entryBytes[:8], s.log[i].Term)
			binary.LittleEndian.PutUint64(entryBytes[8:16], uint64(len(s.log[i].Command)))
			copy(entryBytes[16:], []byte(s.log[i].Command))

			n, err := bw.Write(entryBytes[:])
			if err != nil {
				panic(err)
			}
			ServerAssert(s, "wrote full page", n, ENTRY_SIZE)
		}

		err = bw.Flush()
		if err != nil {
			panic(err)
		}
	}

	if err = s.storage.Sync(); err != nil {
		panic(err)
	}
	s.debugf("persisted in %s. Term: %d. Log Len: %d (%d new). Voted For: %d", time.Now().Sub(t), s.currentTerm, len(s.log), nNewEntries, s.getVotedFor())
}

func min[T ~int | ~uint64](a, b T) T {
	if a < b {
		return a
	}
	return b
}

func max[T ~int | ~uint64](a, b T) T {
	if a > b {
		return a
	}
	return b
}

func (s *Server) restore() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.storage == nil {
		var err error
		s.storage, err = NewFileStorage(s.metadataDir, s.id)
		if err != nil {
			panic(err)
		}
	}

	s.storage.Seek(0, 0)

	// Bytes 0-8: current term
	// Bytes 8-16: voted for
	// Bytes 16-24: log length
	// Bytes 4096-N: log
	var page [PAGE_SIZE]byte
	n, err := s.storage.Read(page[:])
	if err == io.EOF {
		s.ensureLog()
		return
	} else if err != nil {
		panic(err)
	}
	ServerAssert(s, "read full page", n, PAGE_SIZE)

	s.currentTerm = binary.LittleEndian.Uint64(page[:8])
	s.setVotedFor(binary.LittleEndian.Uint64(page[8:16]))
	lenLog := binary.LittleEndian.Uint64(page[16:24])
	s.log = nil

	if lenLog > 0 {
		s.storage.Seek(int64(PAGE_SIZE), 0)

		var e Entry
		for i := 0; uint64(i) < lenLog; i++ {
			var entryBytes [ENTRY_SIZE]byte
			n, err := s.storage.Read(entryBytes[:])
			if err != nil {
				panic(err)
			}
			ServerAssert(s, "read full entry", n, ENTRY_SIZE)

			// bytes 0-8: entry term
			// bytes 8-16: entry command length
			// bytes 16-ENTRY_SIZE: entry command
			e.Term = binary.LittleEndian.Uint64(entryBytes[:8])
			lenValue := binary.LittleEndian.Uint64(entryBytes[8:16])
			e.Command = entryBytes[16 : 16+lenValue]
			s.log = append(s.log, e)
		}
	}

	s.ensureLog()
}

func (s *Server) ensureLog() {
	if len(s.log) == 0 {
		s.log = append(s.log, Entry{})
	}
}

// Must be called within mutex lock
func (s *Server) setVotedFor(id uint64) {
	for i := range s.cluster {
		if i == s.clusterIndex {
			s.cluster[i].votedFor = id
			return
		}
	}

	ServerAssert(s, "Invalid cluster", true, false)
}
