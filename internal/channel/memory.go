package channel

import (
	"context"
	"reflect"
	"sync/atomic"
)

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
