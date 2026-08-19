package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
)

var helpLines = []string{
	"start-navi -n N [--debug]  start an N-node cluster (Note: you should start ODD number of nodes in one cluster.)",
	"stop-navi                  stop the running cluster",
	"reset-navi                 stop the cluster, drop volumes, delete .dat files",
	"status                     show leader and per-node health",
	"set KEY VALUE              write a key on the cluster leader",
	"get KEY [--relaxed]        read a key (relaxed = local, possibly stale)",
	"kill-node                  pick a node to kill (raft layer only; container stays up)",
	"help                       show this message",
	"quit / exit                leave the CLI",
}

func runCommand(m model, line string) (tea.Model, tea.Cmd) {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return m, nil
	}
	name, args := fields[0], fields[1:]

	switch name {
	case "quit", "exit":
		if m.logCancel != nil {
			m.logCancel()
		}
		return m, tea.Quit

	case "help":
		m.history = append(m.history, helpLines...)
		return m, nil

	case "start-navi":
		return startNaviCommand(m, args)

	case "stop-navi":
		return stopNaviCommand(m)

	case "reset-navi":
		return resetNaviCommand(m)

	case "status":
		return statusCommand(m)

	case "set":
		return handleSet(m, args)

	case "get":
		return handleGet(m, args)

	case "kill-node":
		return killNodeCommand(m)

	default:
		m.history = append(m.history, errorStyle.Render(fmt.Sprintf("unknown command: %s (try `help`)", name)))
		return m, nil
	}
}

func startNaviCommand(m model, args []string) (tea.Model, tea.Cmd) {
	if m.logCancel != nil {
		m.history = append(m.history, errorStyle.Render("cluster already running (stop-navi first)"))
		return m, nil
	}

	n := 0
	debug := false
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-n":
			if i+1 >= len(args) {
				m.history = append(m.history, errorStyle.Render("-n requires a value"))
				return m, nil
			}
			var err error
			n, err = strconv.Atoi(args[i+1])
			if err != nil {
				m.history = append(m.history, errorStyle.Render(fmt.Sprintf("invalid -n value: %s", args[i+1])))
				return m, nil
			}
			i++
		case "--debug":
			debug = true
		}
	}

	if n < 1 {
		m.history = append(m.history, errorStyle.Render("usage: start-navi -n N [--debug]"))
		return m, nil
	}

	m.history = append(m.history, fmt.Sprintf("starting %d-node cluster%s...", n, debugSuffix(debug)))

	return m, func() tea.Msg {
		path, ports, err := GenerateCompose(n, debug)
		if err == nil {
			err = ComposeUp(path)
		}
		return clusterStartedMsg{path: path, ports: ports, err: err}
	}
}

func debugSuffix(debug bool) string {
	if debug {
		return " (debug)"
	}
	return ""
}

func stopNaviCommand(m model) (tea.Model, tea.Cmd) {
	if m.composePath == "" {
		m.history = append(m.history, errorStyle.Render("no cluster running"))
		return m, nil
	}

	path := m.composePath
	return m, func() tea.Msg {
		return clusterStoppedMsg{err: ComposeDown(path, false)}
	}
}

func resetNaviCommand(m model) (tea.Model, tea.Cmd) {
	path := composeFileName
	if _, err := os.Stat(path); err != nil {
		m.history = append(m.history, errorStyle.Render("no cluster to reset"))
		return m, nil
	}

	return m, func() tea.Msg {
		err := ComposeDown(path, true)
		if err == nil {
			if matches, globErr := filepath.Glob("cmd/kv-api/*.dat"); globErr == nil {
				for _, f := range matches {
					_ = os.Remove(f)
				}
			}
		}
		return clusterStoppedMsg{reset: true, err: err}
	}
}

func statusCommand(m model) (tea.Model, tea.Cmd) {
	if len(m.nodePorts) == 0 {
		m.history = append(m.history, errorStyle.Render("no cluster running"))
		return m, nil
	}

	ports := m.nodePorts
	return m, func() tea.Msg {
		leader, leaderErr := probeLeader(ports)

		lines := make([]string, 0, len(ports)+1)
		for i, port := range ports {
			state := "down"
			if nodeUp(port) {
				state = "up"
			}
			role := ""
			if leaderErr == nil && port == leader {
				role = " (leader)"
			}
			lines = append(lines, fmt.Sprintf("node%d :%d  %s%s", i+1, port, state, role))
		}
		if leaderErr != nil {
			lines = append(lines, errorStyle.Render(leaderErr.Error()))
		}

		return commandResultMsg{lines: lines}
	}
}

func handleSet(m model, args []string) (tea.Model, tea.Cmd) {
	if len(m.nodePorts) == 0 {
		m.history = append(m.history, errorStyle.Render("no cluster running"))
		return m, nil
	}
	if len(args) != 2 {
		m.history = append(m.history, errorStyle.Render("usage: set KEY VALUE"))
		return m, nil
	}

	ports := m.nodePorts
	key, value := args[0], args[1]
	return m, func() tea.Msg {
		leader, err := probeLeader(ports)
		if err == nil {
			err = doSet(leader, key, value)
		}
		if err != nil {
			return commandResultMsg{lines: []string{errorStyle.Render(err.Error())}}
		}
		return commandResultMsg{lines: []string{fmt.Sprintf("ok: %s = %s", key, value)}}
	}
}

func killNodeCommand(m model) (tea.Model, tea.Cmd) {
	if len(m.nodePorts) == 0 {
		m.history = append(m.history, errorStyle.Render("no cluster running"))
		return m, nil
	}

	ports := m.nodePorts
	return m, func() tea.Msg {
		nodes := make([]killNodeInfo, len(ports))
		for i, p := range ports {
			nodes[i] = killNodeInfo{node: fmt.Sprintf("node%d", i+1), port: p, up: nodeUp(p)}
		}
		return killNodesLoadedMsg{nodes: nodes}
	}
}

func handleGet(m model, args []string) (tea.Model, tea.Cmd) {
	if len(m.nodePorts) == 0 {
		m.history = append(m.history, errorStyle.Render("no cluster running"))
		return m, nil
	}
	if len(args) < 1 {
		m.history = append(m.history, errorStyle.Render("usage: get KEY [--relaxed]"))
		return m, nil
	}

	ports := m.nodePorts
	key := args[0]
	relaxed := len(args) > 1 && args[1] == "--relaxed"

	return m, func() tea.Msg {
		leader, err := probeLeader(ports)
		var value string
		if err == nil {
			value, err = doGet(leader, key, relaxed)
		}
		if err != nil {
			return commandResultMsg{lines: []string{errorStyle.Render(err.Error())}}
		}
		return commandResultMsg{lines: []string{fmt.Sprintf("%s = %s", key, value)}}
	}
}
