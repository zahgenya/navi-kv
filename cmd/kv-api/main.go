// State machine which have 2 operations: get a value from a key, and set a key to a value.
package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
)

type statemachine struct {
	db     *sync.Map
	server int
}

type commandKind uint8

const (
	setCommand commandKind = iota
	getCommand
)

type command struct {
	kind  commandKind
	key   string
	value string
}

func encodeCommand(c command) []byte {
	msg := bytes.NewBuffer(nil)
	err := msg.WriteByte(uint8(c.kind))
	if err != nil {
		panic(err)
	}

	err = binary.Write(msg, binary.LittleEndian, uint64(len(c.key)))
	if err != nil {
		panic(err)
	}

	msg.WriteString(c.key)

	err = binary.Write(msg, binary.LittleEndian, uint64(len(c.value)))
	if err != nil {
		panic(err)
	}

	msg.WriteString(c.value)

	return msg.Bytes()
}

func decodeCommand(msg []byte) command {
	var c command
	c.kind = commandKind(msg[0])

	keyLen := binary.LittleEndian.Uint64(msg[1:9])
	c.key = string(msg[9 : 9+keyLen])

	if c.kind == setCommand {
		valLen := binary.LittleEndian.Uint64(msg[9+keyLen : 9+keyLen+8])
		c.value = string(msg[9+keyLen+8 : 9+keyLen+8+valLen])
	}

	return c
}

func (s *statemachine) Apply(cmd []byte) ([]byte, error) {
	c := decodeCommand(cmd)

	switch c.kind {
	case setCommand:
		s.db.Store(c.key, c.value)
	case getCommand:
		value, ok := s.db.Load(c.key)
		if !ok {
			return nil, fmt.Errorf("key not found")
		}

		return []byte(value.(string)), nil
	default:
		return nil, fmt.Errorf("unknown command: %x", cmd)
	}

	return nil, nil
}

type httpServer struct {
	raft *goraft.Server
	db   *sync.Map
}

// HTTP endpoint for setting value. Taking key and value and calling Apply() on them
// Example usage: curl http://localhost:2020/set?key=x&value=1
func (hs httpServer) setHandler(w http.ResponseWriter, r *http.Request) {
	var c command

	c.kind = setCommand
	c.key = r.URL.Query().Get("key")
	c.value = r.URL.Query().Get("value")

	_, err := hs.raft.Apply([][]byte{encodeCommand(c)})
	if err != nil {
		log.Printf("could not write key-value: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
}

// HTTP endpoint for getting value. More trickier:
// There are two ways to do this, we can just read from emdedded local copy of Map
// in the current process. But this might be not up-to-date or correct. (but fast!)
// But trully only one correct way to read from a Raft cluster is to pass the read
// through the log replication tool
// Example usage: curl http://localhost:2020/get?key=x
// And from memory is: curl http://localhost:202/get?key=x?relaxed=true
func (hs httpServer) getHandler(w http.ResponseWriter, r *http.Request) {
	var c command

	c.kind = getCommand
	c.key = r.URL.Query().Get("key")

	var value []byte
	var err error

	if r.URL.Query().Get("relaxed") == "true" {
		v, ok := hs.db.Load(c.key)

		if !ok {
			err = fmt.Errorf("key not found")
		} else {
			value = []byte(v.(string))
		}
	} else {
		var results []goraft.ApplyResult

		results, err = hs.raft.Apply([][]byte{encodeCommand(c)})
		if err == nil {
			if len(results) != 1 {
				err = fmt.Errorf("expected single response from Raft, got: %s", len(results))
			} else if results[0].Error != nil {
				err = results[0].Error
			} else {
				value = results[0].Result
			}
		}
	}

	if err != nil {
		log.Printf("Could not encode key-value in HTTP response: %s", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	written := 0
	for written < len(value) {
		n, err := w.Write(value[written:])
		if err != nil {
			log.Printf("could not encode key-value in HTTP response: %s", err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}

		written += n
	}
}

type config struct {
	cluster []goraft.ClusterMember
	index   int
	id      string
	address string
	http    string
}

func getConfig() config {
	cfg := config{}
	var node string

	for i, arg := range os.Args[1:] {
		if arg == "--node" {
			var err error
			node = os.Args[i+2]
			cfg.index, err = strconv.Atio(node)
			if err != nil {
				log.Fatal("expected $value to be a valid integer in `--node $value`, got: %s", node)
			}
			i++
			continue
		}

		if arg == "--cluster" {
			cluster := os.Args[i+2]
			var clusterEntry goraft.ClusterMember
			for _, part := range strings.Split(cluster, ";") {
				idAddress := strings.Split(part, ",")
				var err error
				clusterEntry.Id, err = strconv.ParseUint(idAddress[0], 10, 64)
				if err != nil {
					log.Fatal("expected $id to be a valid integer in `--cluster $id,$ip`, got: %s", idAddress)
				}

				clusterEntry.Address = idAddress[1]
				cfg.cluster = append(cfg.cluster, clusterEntry)
			}

			i++
			continue
		}
	}

	if node == "" {
		log.Fatal("missing required parameter: --node $index")
	}

	if cfg.http == "" {
		log.Fatal("missing required parameter: --http $address")
	}

	if len(cfg.cluster) == 0 {
		log.Fatal("missing required parameter: --cluster $node1Id,$node1Address;...;$nodeNId,$nodeNAddress")
	}

	return cfg
}

func main() {
	// var b [8]byte
	// _, err := rand.Read(b[:])
	// if err != nil {
	// 	panic("cannot seed local random number generator")
	// }
	//
	// seed := int64(binary.LittleEndian.Uint64(b[:]))
	// nodeRand := mathrand.New(mathrand.NewSource(seed))
	// This whole part will take a place in a Raft server itself.
	// #TODO: add rand to server, since global random number generator is deprecated

	cfg := getConfig()

	var db sync.Map

	var sm statemachine
	sm.db = &db
	sm.server = cfg.index

	s := goraft.NewServer(cfg.cluster, &sm, ".", cfg.index)
	go s.Start()

	hs := httpServer{s, &db}

	http.HandleFunc("/set", hs.setHandler)
	http.HandleFunc("/get", hs.getHandler)
	err = http.ListenAndServe(cfg.http, nil)
	if err != nil {
		panic(err)
	}
}
