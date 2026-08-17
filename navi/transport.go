package navi

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

const (
	dialTimeout = 500 * time.Millisecond
	callTimeout = 1 * time.Second
)

type RPCMessage struct {
	Term uint64
}

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

	c, err := dialHTTPTimeout(address, dialTimeout)
	if err != nil {
		return nil, err
	}
	t.clients[address] = c
	return c, nil
}

func dialHTTPTimeout(address string, timeout time.Duration) (*rpc.Client, error) {
	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return nil, err
	}

	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		conn.Close()
		return nil, err
	}

	io.WriteString(conn, "CONNECT "+rpc.DefaultRPCPath+" HTTP/1.0\n\n")
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err != nil || resp.Status != "200 Connected to Go RPC" {
		conn.Close()
		if err == nil {
			err = fmt.Errorf("unexpected HTTP response from %s: %s", address, resp.Status)
		}
		return nil, err
	}

	if err := conn.SetDeadline(time.Time{}); err != nil {
		conn.Close()
		return nil, err
	}

	return rpc.NewClient(conn), nil
}

func (t *RPCTransport) call(address, method string, req, rsp any) error {
	c, err := t.dial(address)
	if err != nil {
		return err
	}

	call := c.Go(method, req, rsp, make(chan *rpc.Call, 1))
	select {
	case <-call.Done:
		if call.Error != nil {
			t.mu.Lock()
			delete(t.clients, address)
			t.mu.Unlock()
		}
		return call.Error
	case <-time.After(callTimeout):
		t.mu.Lock()
		delete(t.clients, address)
		t.mu.Unlock()
		c.Close()
		return fmt.Errorf("rpc call %s to %s timed out after %s", method, address, callTimeout)
	}
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
