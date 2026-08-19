package main

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

var probeClient = &http.Client{Timeout: 2 * time.Second}

func probeLeader(ports []int) (int, error) {
	for _, port := range ports {
		url := fmt.Sprintf("http://localhost:%d/set?key=__leader_probe__&value=1", port)
		resp, err := probeClient.Post(url, "", nil)
		if err != nil {
			continue
		}
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return port, nil
		}
	}
	return 0, fmt.Errorf("no leader found among %d node(s)", len(ports))
}

func probeLeaderWithRetry(ports []int, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	leader, err := probeLeader(ports)
	for err != nil && time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
		leader, err = probeLeader(ports)
	}
	return leader, err
}

func nodeUp(port int) bool {
	url := fmt.Sprintf("http://localhost:%d/get?key=__health_probe__", port)
	resp, err := probeClient.Get(url)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return true
}

func doSet(port int, key, value string) error {
	url := fmt.Sprintf("http://localhost:%d/set?key=%s&value=%s", port, key, value)
	resp, err := probeClient.Post(url, "", nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("set failed: %s: %s", resp.Status, body)
	}
	return nil
}

func doKill(port int) error {
	url := fmt.Sprintf("http://localhost:%d/kill", port)
	resp, err := probeClient.Get(url)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("kill failed: %s: %s", resp.Status, body)
	}
	return nil
}

func doAddServer(leaderPort int, id uint64, address string) error {
	url := fmt.Sprintf("http://localhost:%d/add-server?id=%d&address=%s", leaderPort, id, address)
	resp, err := probeClient.Get(url)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("add-server failed: %s: %s", resp.Status, body)
	}
	return nil
}

func waitForNodeUp(port int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if nodeUp(port) {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return nodeUp(port)
}

func doGet(port int, key string, relaxed bool) (string, error) {
	url := fmt.Sprintf("http://localhost:%d/get?key=%s", port, key)
	if relaxed {
		url += "&relaxed=true"
	}

	resp, err := probeClient.Get(url)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("get failed: %s: %s", resp.Status, body)
	}
	return string(body), nil
}
