# navi-kv

Distributed key-value store built on Raft consensus, written in Go.

## What it does

`navi-kv` implements the core Raft protocol from scratch — leader election, log
replication, term/vote persistence, cluster membership changes — exposed as a
KV HTTP API, with a terminal UI for spinning up and driving local clusters via
Docker Compose.

- **Leader election** — Followers become Candidates on election timeout, bump term,
  request votes; majority wins leadership for that term.
- **Log replication** — Leader accepts writes, appends to its log, replicates to
  Followers; an entry commits once a majority has it, then applies to the KV
  state machine.
- **Safety** — a node only votes for a candidate whose log is at least as
  up-to-date as its own; committed entries are never overwritten.

## Technology stack

- **Go 1.26+** — `net/rpc` for inter-node transport (gob wire encoding),
  hand-rolled binary encodings for on-disk Raft
  log persistence and KV commands (no `encoding/json`/`encoding/gob` at the
  application level)
- [**Bubble Tea**](https://github.com/charmbracelet/bubbletea) /
  [**bubbles**](https://github.com/charmbracelet/bubbles) /
  [**lipgloss**](https://github.com/charmbracelet/lipgloss) — the `navi-cli` TUI
- **Docker + Docker Compose** — local multi-node clusters, driven by `navi-cli`
  via a generated compose file (`docker-compose.generated.yml`)
- [**Task**](https://taskfile.dev) (`go-task`) — build/test/run commands
- `golangci-lint` (`staticcheck`, `errcheck`, `govet`) — not wired into CI (there is
  none in this repo) run manually

## Architecture

Three components:

- **`navi/`** — the Raft core (no sub-packages). `Server` owns all mutable Raft
  state behind a single mutex, a busy-spin state loop drives leader/candidate/
  follower behavior with no ticker or sleep (`server.go`). Pluggable `Transport`
  (real RPC or an in-process, fault-injectable `MemoryTransport` for tests) and
  `Clock` (real or manually-advanced, for deterministic tests) interfaces.
- **`cmd/kv-api/`** — the KV server binary. Wraps a `sync.Map` state machine
  behind `navi.Server`, exposes it over HTTP (`/set`, `/get`, `/kill`,
  `/add-server`).
- **`cmd/navi-cli/`** — a Bubble Tea TUI for local development: boots clusters via
  Docker Compose, streams per-node logs, and drives `set`/`get`/`status`/
  `kill-node`/`add-node` against a running cluster.

## Getting started

### Prerequisites

- Go 1.26+
- Docker + Docker Compose
- [Task](https://taskfile.dev)

### Build and run the CLI

```sh
task build     # go build ./cmd/navi-cli
task run-cli   # ./navi-cli
```

Inside `navi-cli`:

```
start-navi -n N [--debug]    # boot an N-node cluster
status                       # show cluster/leader state
set / get                    # talk to the cluster's KV API
kill-node / add-node         # simulate a node crash / grow the cluster
stop-navi / reset-navi       # tear the cluster down
help                         # full command list
```


## Testing

`navi/helpers_test.go` is fault-injection scaffolding (`MemoryTransport`/
`MemoryNetwork` wrappers, polling helpers like `waitForLeader`/`waitForCommit`).
Built on top of it:

- **`raft_test.go`** — cluster-level tests: `TestKillLeaderElectsNewLeader`,
  `TestLogReplication`, `TestClusterTolerateOneNodeDown`,
  `TestClusterCannotCommitWithTwoNodesDown`
- **`membership_test.go`** — `AddServer`/config-change tests: encode/decode
  round-trip, oversize rejection, real add-replicate-commit, rejecting
  concurrent/non-leader `AddServer`, truncation reverting an uncommitted change,
  `restore()` reconstructing membership from a log

Run with:

```sh
task test   # go test ./... -race
```

Always run with `-race` — this codebase is goroutine-heavy and lock-discipline
bugs pass silently without it.

## References

- [The Raft paper](https://raft.github.io/raft.pdf)
- [Consensus algorithm explanation](https://www.youtube.com/watch?v=Ry2fIFIThP8) — Ben Dicken, PlanetScale
- [Raft in Go, a blog post](https://notes.eatonphil.com/2023-05-25-raft.html) — Phil Eaton
