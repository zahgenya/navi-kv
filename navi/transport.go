package navi

import (
	"net"
	"net/http"
	"net/rpc"
	"sync"
)

type RPCMessage struct {
	Term uint64
}

// Transport carries Raft RPCs between nodes and exposes a node's own
// endpoints to the rest of the cluster
type Transport interface {
	RequestVote(address string, req RequestVoteRequest, rsp *RequestVoteResponse) error
	AppendEntries(address string, req AppendEntriesRequest, rsp *AppendEntriesResponse) error

	Serve(s *Server) error
	Close() error
}

type RPCTransport struct {
	mu      sync.Mutex
	clients map[string]*rpc.Client

	server *http.Server
}

func NewRPCTransport() *RPCTransport {
	return &RPCTransport{clients: make(map[string]*rpc.Client)}
}

func (t *RPCTransport) dial(address string) (*rpc.Client, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if c, ok := t.clients[address]; ok {
		return c, nil
	}

	c, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		return nil, err
	}
	t.clients[address] = c
	return c, nil
}

func (t *RPCTransport) call(address, method string, req, rsp any) error {
	c, err := t.dial(address)
	if err != nil {
		return err
	}

	err = c.Call(method, req, rsp)
	if err != nil {
		// drop the cached client so the next call redials instead of
		// reusing a connection that just proved dead
		t.mu.Lock()
		delete(t.clients, address)
		t.mu.Unlock()
	}
	return err
}

func (t *RPCTransport) RequestVote(address string, req RequestVoteRequest, rsp *RequestVoteResponse) error {
	return t.call(address, "Server.HandleRequestVoteRequest", req, rsp)
}

func (t *RPCTransport) AppendEntries(address string, req AppendEntriesRequest, rsp *AppendEntriesResponse) error {
	return t.call(address, "Server.HandleAppendEntriesRequest", req, rsp)
}

func (t *RPCTransport) Serve(s *Server) error {
	rpcServer := rpc.NewServer()
	if err := rpcServer.Register(s); err != nil {
		return err
	}

	l, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, rpcServer)

	t.server = &http.Server{Handler: mux}
	go t.server.Serve(l)
	return nil
}

func (t *RPCTransport) Close() error {
	if t.server == nil {
		return nil
	}
	return t.server.Close()
}
