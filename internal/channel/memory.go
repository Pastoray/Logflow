package channel

import (
	"context"
	"reflect"
	"sync/atomic"
)

// MemoryChannel is a bounded, sharded, in-memory queue. Sharding spreads
// producer/consumer contention across N independent Go channels instead of
// funneling every goroutine through a single lock/queue, which matters once
// dozens of source/sink goroutines share a channel. Events are not strictly
// FIFO across shards (only within a shard), which is an acceptable trade for
// throughput in a log-aggregation pipeline.
//
// Data is not persisted: a process crash loses whatever is currently
// buffered. Use the file channel when durability matters more than raw
// throughput.
type MemoryChannel struct {
	name     string
	shards   []chan *Event
	capacity int
	closed   int32
	closeCh  chan struct{}
	putSeq   int64
	counters
}

func NewMemoryChannel(name string, capacity, numShards int) *MemoryChannel {
	if numShards < 1 {
		numShards = 1
	}
	perShard := capacity / numShards
	if perShard < 1 {
		perShard = 1
	}
	shards := make([]chan *Event, numShards)
	for i := range shards {
		shards[i] = make(chan *Event, perShard)
	}
	return &MemoryChannel{
		name:     name,
		shards:   shards,
		capacity: perShard * numShards,
		closeCh:  make(chan struct{}),
	}
}

func (m *MemoryChannel) Name() string { return m.name }

func (m *MemoryChannel) Put(ctx context.Context, e *Event) error {
	if atomic.LoadInt32(&m.closed) == 1 {
		return ErrClosed
	}
	shard := m.shards[m.pickShard()]
	select {
	case shard <- e:
		m.onPut()
		return nil
	case <-ctx.Done():
		m.onDrop()
		return ctx.Err()
	case <-m.closeCh:
		return ErrClosed
	}
}

// TryPut attempts a non-blocking put and reports whether it succeeded. It
// never increments the dropped-event counter on failure - unlike Put, a
// failed TryPut isn't a lost event, since callers with a fallback path
// (SpillableMemoryChannel routing to a durable overflow) decide for
// themselves whether "primary channel currently full" counts as a drop.
func (m *MemoryChannel) TryPut(e *Event) bool {
	if atomic.LoadInt32(&m.closed) == 1 {
		return false
	}
	shard := m.shards[m.pickShard()]
	select {
	case shard <- e:
		m.onPut()
		return true
	default:
		return false
	}
}

func (m *MemoryChannel) pickShard() int {
	n := atomic.AddInt64(&m.putSeq, 1)
	return int(n % int64(len(m.shards)))
}

// Take fans in across all shards fairly using reflect.Select, which builds a
// single select statement over a dynamic number of channels (Go's static
// `select` cannot range over a slice of channels directly).
func (m *MemoryChannel) Take(ctx context.Context) (*Event, error) {
	cases := make([]reflect.SelectCase, 0, len(m.shards)+2)
	for _, s := range m.shards {
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(s)})
	}
	doneIdx := len(cases)
	cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())})
	closeIdx := len(cases)
	cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(m.closeCh)})

	chosen, val, ok := reflect.Select(cases)
	switch {
	case chosen == doneIdx:
		return nil, ctx.Err()
	case chosen == closeIdx:
		return nil, ErrClosed
	case !ok:
		return nil, ErrClosed
	default:
		e := val.Interface().(*Event)
		m.onTake()
		return e, nil
	}
}

func (m *MemoryChannel) Stats() Stats {
	return m.counters.snapshot(int64(m.capacity))
}

func (m *MemoryChannel) Close() error {
	if atomic.CompareAndSwapInt32(&m.closed, 0, 1) {
		close(m.closeCh)
	}
	return nil
}
