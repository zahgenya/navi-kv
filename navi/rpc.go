package navi

import (
	"net/rpc"
)

type RPCMessage struct {
	Term uint64
}

func (s *Server) rpcCall(i int, name string, req, rsp any) bool {
	s.mu.Lock()
	c := s.cluster[i]
	var err error
	var rpcClient *rpc.Client = c.rpcClient
	if c.rpcClient == nil {
		c.rpcClient, err = rpc.DialHTTP("tcp", c.Address)
		rpcClient = c.rpcClient
	}
	s.mu.Unlock()

	// TODO: implement reconnection

	if err == nil {
		err = rpcClient.Call(name, req, rsp)
	}

	if err != nil {
		s.warnf("error calling %s on %d: %s", name, c.Id, err)
	}

	return err == nil
}
