// Package channel implements the buffering stage between sources and sinks.
// Channels decouple producer (source) speed from consumer (sink) speed and
// provide backpressure: Put blocks (respecting the caller's context) once a
// channel is full, rather than silently dropping data.
package channel

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

var ErrClosed = errors.New("channel: closed")

// Event is the internal representation of a single log record as it moves
// through the pipeline.
type Event struct {
	Body      []byte
	Headers   map[string]string
	Timestamp time.Time
}

// Stats is a point-in-time snapshot of a channel's counters.
type Stats struct {
	Capacity     int64
	Size         int64
	PutCount     int64
	TakeCount    int64
	DroppedCount int64
}

// Channel is the interface implemented by all channel backends (memory,
// file-backed/durable, etc). Implementations must be safe for concurrent use
// by many source goroutines (Put) and many sink goroutines (Take).
type Channel interface {
	Name() string
	// Put enqueues an event, blocking under backpressure until space is
	// available, the context is cancelled, or the channel is closed.
	Put(ctx context.Context, e *Event) error
	// Take dequeues a single event, blocking until one is available, the
	// context is cancelled, or the channel is closed and drained.
	Take(ctx context.Context) (*Event, error)
	Stats() Stats
	Close() error
}

// counters is embedded by implementations for lock-free stat tracking.
type counters struct {
	size    int64
	putCnt  int64
	takeCnt int64
	dropped int64
}

func (c *counters) onPut()  { atomic.AddInt64(&c.size, 1); atomic.AddInt64(&c.putCnt, 1) }
func (c *counters) onTake() { atomic.AddInt64(&c.size, -1); atomic.AddInt64(&c.takeCnt, 1) }
func (c *counters) onDrop() { atomic.AddInt64(&c.dropped, 1) }
func (c *counters) snapshot(cap int64) Stats {
	return Stats{
		Capacity:     cap,
		Size:         atomic.LoadInt64(&c.size),
		PutCount:     atomic.LoadInt64(&c.putCnt),
		TakeCount:    atomic.LoadInt64(&c.takeCnt),
		DroppedCount: atomic.LoadInt64(&c.dropped),
	}
}
