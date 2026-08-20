package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"text/template"
)

const composeFileName = "docker-compose.generated.yml"

const composeTemplate = `services:
{{- range .Nodes }}
  node{{ .Index }}:
    build: .
    container_name: kv-node{{ .Index }}
    hostname: node{{ .Index }}
    command: ["--node", "{{ .NodeArg }}", "--http", ":2020", "--cluster", "{{ .Cluster }}"{{ if .PendingJoin }}, "--pending-join"{{ end }}]
    ports:
      - "{{ .HostPort }}:2020"
    networks:
      - raft
    deploy:
      resources:
        limits:
          cpus: "1"
          memory: 3G
    environment:
      DEBUG: "{{ $.Debug }}"
{{- end }}

networks:
  raft:
    driver: bridge
`

type composeNode struct {
	Index       int
	NodeArg     int
	HostPort    int
	Cluster     string
	PendingJoin bool
}

type composeData struct {
	Nodes []composeNode
	Debug bool
}

func renderCompose(data composeData) error {
	tmpl, err := template.New("compose").Parse(composeTemplate)
	if err != nil {
		return fmt.Errorf("could not parse compose template: %w", err)
	}

	f, err := os.Create(composeFileName)
	if err != nil {
		return fmt.Errorf("could not create %s: %w", composeFileName, err)
	}

	if err := tmpl.Execute(f, data); err != nil {
		_ = f.Close()
		return fmt.Errorf("could not render compose template: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("could not write %s: %w", composeFileName, err)
	}

	return nil
}

func raftAddress(index int) string {
	return fmt.Sprintf("node%d:3030", index)
}

func clusterSpec(index int) string {
	return fmt.Sprintf("%d,%s", index, raftAddress(index))
}

func clusterString(n int) string {
	parts := make([]string, 0, n)
	for i := 1; i <= n; i++ {
		parts = append(parts, clusterSpec(i))
	}
	return strings.Join(parts, ";")
}

func GenerateCompose(n int, debug bool) (path string, ports []int, err error) {
	if n < 1 {
		return "", nil, fmt.Errorf("node count must be at least 1, got %d", n)
	}

	cluster := clusterString(n)

	nodes := make([]composeNode, 0, n)
	for i := 1; i <= n; i++ {
		hostPort := 2020 + i
		nodes = append(nodes, composeNode{Index: i, NodeArg: i - 1, HostPort: hostPort, Cluster: cluster})
		ports = append(ports, hostPort)
	}

	if err := renderCompose(composeData{Nodes: nodes, Debug: debug}); err != nil {
		return "", nil, err
	}

	return composeFileName, ports, nil
}

func AddComposeNode(existingN int, debug bool) (path string, newIndex, newPort int, nodeAddress string, err error) {
	if existingN < 1 {
		return "", 0, 0, "", fmt.Errorf("no existing nodes to add to (existingN=%d)", existingN)
	}

	oldCluster := clusterString(existingN)

	newIndex = existingN + 1
	nodeAddress = raftAddress(newIndex)
	newCluster := clusterSpec(newIndex)

	nodes := make([]composeNode, 0, newIndex)
	for i := 1; i <= existingN; i++ {
		nodes = append(nodes, composeNode{Index: i, NodeArg: i - 1, HostPort: 2020 + i, Cluster: oldCluster})
	}
	newPort = 2020 + newIndex
	nodes = append(nodes, composeNode{Index: newIndex, NodeArg: 0, HostPort: newPort, Cluster: newCluster, PendingJoin: true})

	if err := renderCompose(composeData{Nodes: nodes, Debug: debug}); err != nil {
		return "", 0, 0, "", err
	}

	return composeFileName, newIndex, newPort, nodeAddress, nil
}

func ComposeUp(path string) error {
	out, err := exec.Command("docker", "compose", "-f", path, "up", "-d", "--build").CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker compose up failed: %w\n%s", err, out)
	}
	return nil
}

func ComposeDown(path string, volumes bool) error {
	args := []string{"compose", "-f", path, "down"}
	if volumes {
		args = append(args, "-v")
	}

	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker compose down failed: %w\n%s", err, out)
	}
	return nil
}
