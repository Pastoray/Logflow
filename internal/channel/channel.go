package channel

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

var ErrClosed = errors.New("channel: closed")

type Event struct {
	Body      []byte
	Headers   map[string]string
	Timestamp time.Time
}

type Stats struct {
	Capacity     int64
	Size         int64
	PutCount     int64
	TakeCount    int64
	DroppedCount int64
}

type Channel interface {
	Name() string

	Put(ctx context.Context, e *Event) error

	Take(ctx context.Context) (*Event, error)
	Stats() Stats
	Close() error
}

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
