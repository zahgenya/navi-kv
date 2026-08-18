package navi

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrNoRoute     = errors.New("memory network: no route to address")
	ErrPartitioned = errors.New("memory network: node partitioned")
	ErrDropped     = errors.New("memory network: message dropped")
)

type MemoryNetwork struct {
	mu          sync.Mutex
	nodes       map[string]*MemoryTransport
	partitioned map[string]bool
	dropped     map[[2]string]bool
	delay       time.Duration
}

func NewMemoryNetwork() *MemoryNetwork {
	return &MemoryNetwork{
		nodes:       make(map[string]*MemoryTransport),
		partitioned: make(map[string]bool),
		dropped:     make(map[[2]string]bool),
	}
}

func (n *MemoryNetwork) register(address string, t *MemoryTransport) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[address] = t
}

func (n *MemoryNetwork) unregister(address string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.nodes, address)
}

func (n *MemoryNetwork) Partition(address string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partitioned[address] = true
}

func (n *MemoryNetwork) Heal(address string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.partitioned, address)
}

func (n *MemoryNetwork) Drop(from, to string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropped[[2]string{from, to}] = true
}

func (n *MemoryNetwork) Undrop(from, to string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.dropped, [2]string{from, to})
}

func (n *MemoryNetwork) Delay(d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.delay = d
}

func (n *MemoryNetwork) route(from, to string) (*MemoryTransport, time.Duration, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.partitioned[from] || n.partitioned[to] {
		return nil, 0, ErrPartitioned
	}
	if n.dropped[[2]string{from, to}] {
		return nil, 0, ErrDropped
	}

	t, ok := n.nodes[to]
	if !ok {
		return nil, 0, ErrNoRoute
	}
	return t, n.delay, nil
}

type MemoryTransport struct {
	network *MemoryNetwork
	address string

	mu       sync.Mutex
	server   *Server
	paused   bool
	resumeCh chan struct{}
}

func NewMemoryTransport(network *MemoryNetwork) *MemoryTransport {
	return &MemoryTransport{network: network}
}

func (t *MemoryTransport) Serve(s *Server) error {
	t.mu.Lock()
	t.address = s.address
	t.server = s
	t.mu.Unlock()

	t.network.register(s.address, t)
	return nil
}

func (t *MemoryTransport) Close() error {
	t.network.unregister(t.address)
	return nil
}

func (t *MemoryTransport) Pause() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.paused {
		return
	}
	t.paused = true
	t.resumeCh = make(chan struct{})
}

func (t *MemoryTransport) Resume() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.paused {
		return
	}
	t.paused = false
	close(t.resumeCh)
	t.resumeCh = nil
}

func (t *MemoryTransport) deliver(to string, call func(*Server) error) error {
	target, delay, err := t.network.route(t.address, to)
	if err != nil {
		return err
	}

	if delay > 0 {
		time.Sleep(delay)
	}

	target.mu.Lock()
	server := target.server
	resumeCh := target.resumeCh
	target.mu.Unlock()

	if server == nil {
		return ErrNoRoute
	}

	if resumeCh != nil {
		<-resumeCh
	}

	return call(server)
}

func (t *MemoryTransport) RequestVote(address string, req RequestVoteRequest, rsp *RequestVoteResponse) error {
	return t.deliver(address, func(s *Server) error {
		return s.HandleRequestVoteRequest(req, rsp)
	})
}

func (t *MemoryTransport) AppendEntries(address string, req AppendEntriesRequest, rsp *AppendEntriesResponse) error {
	return t.deliver(address, func(s *Server) error {
		return s.HandleAppendEntriesRequest(req, rsp)
	})
}
