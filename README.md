# navi-kv

Distributed key-value store built on Raft consensus, written in Go.

## Raft overview

Raft keeps a cluster of nodes agreeing on a single, ordered log despite crashes and network hiccups. Each node is Follower, Candidate, or Leader.

- **Leader election** — nodes start as Followers. If a Follower hears no heartbeat before its election timeout, it becomes Candidate, bumps the term, and requests votes. Majority vote wins leadership for that term.
- **Log replication** — the Leader accepts writes, appends them to its log, and replicates entries to Followers. An entry is committed once a majority of nodes have it, then applied to the state machine (the KV store).
- **Safety** — terms and log indexes prevent split-brain: a node only votes for a candidate whose log is at least as up to date as its own, and committed entries are never overwritten.

This repo implements the core Raft loop (elections, term updates, heartbeats, batched log replication with catch-up) in `navi/`, exposed as a KV HTTP API in `cmd/kv-api/`, with a TUI (`cmd/navi-cli/`) for spinning up local clusters and watching them run.

More will land here, I am planning on implementing test suite. And probably switch from RPC calls to higher protocol like in [rafthttp](https://pkg.go.dev/go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp).

## Dependencies:

- Go 1.26+
- Docker + Docker Compose (navi-cli's `start-navi` drives a generated compose file to spin up local clusters)
- [Task](https://taskfile.dev) (`go-task`) to run the commands below

## Build and run the CLI:

```
task build     # go build ./cmd/navi-cli
task run-cli   # ./navi-cli
```

Inside navi-cli: `start-navi -n N [--debug]` to boot an N-node cluster, `status`/`set`/`get` to talk to it through key-value storage API, `stop-navi`/`reset-navi` to tear it down, `help` for the full command list.

## References

- https://raft.github.io/raft.pdf (Raft paper)
- https://www.youtube.com/watch?v=Ry2fIFIThP8 (Explanation of consensus algorithm by Ben Dicken from PlanetScale)
- https://notes.eatonphil.com/2023-05-25-raft.html (Blogpost about golang implementation of Raft by Phil Eaton)
