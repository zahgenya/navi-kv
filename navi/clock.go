package navi

import (
	"sync"
	"time"
)

type Clock interface {
	Now() time.Time
}

type realClock struct{}

func NewRealClock() Clock { return realClock{} }

func (realClock) Now() time.Time { return time.Now() }

type ManualClock struct {
	mu  sync.Mutex
	now time.Time
}

func NewManualClock(start time.Time) *ManualClock {
	return &ManualClock{now: start}
}

func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *ManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
