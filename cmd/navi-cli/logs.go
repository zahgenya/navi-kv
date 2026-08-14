package main

import (
	"bufio"
	"context"
	"io"
	"os/exec"
	"strings"
)

type logLine struct {
	Node  string
	Level string
	Text  string
}

func classifyLevel(line string) string {
	lower := strings.ToLower(line)
	switch {
	case strings.Contains(line, "[WARN]"):
		return "WARN"
	case strings.Contains(lower, "error") || strings.Contains(lower, "panic"):
		return "ERROR"
	default:
		return "INFO"
	}
}

func streamContainerLogs(ctx context.Context, container, node string, out chan<- logLine) {
	cmd := exec.CommandContext(ctx, "docker", "logs", "-f", "--timestamps", container)

	pr, pw := io.Pipe()
	cmd.Stdout = pw
	cmd.Stderr = pw

	go func() {
		_ = cmd.Run()
		_ = pw.Close()
	}()

	scanner := bufio.NewScanner(pr)
	for scanner.Scan() {
		text := scanner.Text()
		out <- logLine{Node: node, Level: classifyLevel(text), Text: text}
	}
}
