package navi

import (
	"fmt"
	"io"
	"os"
	"path"
	"sync"
)

type Storage interface {
	io.ReadWriteSeeker
	Sync() error
	Close() error
}

func NewFileStorage(metadataDir string, id uint64) (Storage, error) {
	return os.OpenFile(
		path.Join(metadataDir, fmt.Sprintf("md_%d.dat", id)),
		os.O_SYNC|os.O_CREATE|os.O_RDWR,
		0o755)
}

type MemStorage struct {
	mu   sync.Mutex
	data []byte
	pos  int64
}

func NewMemStorage() *MemStorage {
	return &MemStorage{}
}

func (m *MemStorage) Seek(offset int64, origin int) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var newPos int64
	switch origin {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = m.pos + offset
	case io.SeekEnd:
		newPos = int64(len(m.data)) + offset
	default:
		return 0, fmt.Errorf("invalid origin: %d", origin)
	}
	if newPos < 0 {
		return 0, fmt.Errorf("negative seek position")
	}

	m.pos = newPos
	return m.pos, nil
}

func (m *MemStorage) Read(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.pos >= int64(len(m.data)) {
		return 0, io.EOF
	}

	n := copy(p, m.data[m.pos:])
	m.pos += int64(n)
	return n, nil
}

func (m *MemStorage) Write(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	end := m.pos + int64(len(p))
	if end > int64(len(m.data)) {
		grown := make([]byte, end)
		copy(grown, m.data)
		m.data = grown
	}

	n := copy(m.data[m.pos:end], p)
	m.pos += int64(n)
	return n, nil
}

func (m *MemStorage) Sync() error  { return nil }
func (m *MemStorage) Close() error { return nil }
