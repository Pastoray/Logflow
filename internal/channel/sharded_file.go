package channel

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
)

// ShardedFileChannel spreads a durable channel over N independent
// FileChannels ("parallel WALs"), each in its own subdirectory with its
// own active write segment, lock and fsync loop. A single FileChannel
// serializes every Put through one write segment; with N shards, N Puts
// proceed in parallel, so durable ingest throughput scales with N until
// the disk itself is the limit.
//
// Like MemoryChannel's shards, ordering is only preserved within a shard
// (events are assigned round-robin), not across the whole channel.
// Capacity is split evenly across shards.
type ShardedFileChannel struct {
	name   string
	shards []*FileChannel
	putSeq int64
	closed int32
	done   chan struct{}
}

// walShardDirPrefix names the per-shard subdirectories: dir/wal-0, wal-1...
const walShardDirPrefix = "wal-"

// NewShardedFileChannel opens (recovering any existing data) n parallel
// WALs under dir. n must be >= 2; use NewFileChannel for a single WAL.
//
// The on-disk layouts differ (a single WAL keeps segments directly in dir),
// so changing the shard count of an existing directory would silently
// strand its data. That is refused with an error instead: drain the old
// layout first (run it to empty with the old setting) or point at a new dir.
func NewShardedFileChannel(name, dir string, n, capacity int, maxSegmentBytes int64, retention RetentionPolicy) (*ShardedFileChannel, error) {
	if n < 2 {
		return nil, fmt.Errorf("sharded file channel %q: need at least 2 shards, got %d", name, n)
	}
	if err := checkWALLayout(dir, n); err != nil {
		return nil, fmt.Errorf("sharded file channel %q: %w", name, err)
	}
	per := capacity / n
	if per < 1 {
		per = 1
	}
	sc := &ShardedFileChannel{name: name, done: make(chan struct{})}
	for i := 0; i < n; i++ {
		fc, err := NewFileChannel(fmt.Sprintf("%s-wal%d", name, i), filepath.Join(dir, fmt.Sprintf("%s%d", walShardDirPrefix, i)), per, maxSegmentBytes, retention)
		if err != nil {
			sc.Close()
			return nil, err
		}
		sc.shards = append(sc.shards, fc)
	}
	return sc, nil
}

// checkWALLayout refuses a directory whose existing contents were written
// with a different shard count.
func checkWALLayout(dir string, n int) error {
	ents, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range ents {
		name := e.Name()
		if !e.IsDir() && strings.HasPrefix(name, "seg-") {
			return fmt.Errorf("%s holds single-WAL segments (%s); refusing to start with wal_shards=%d and strand them - drain the channel under the old setting first, or use a new dir", dir, name, n)
		}
		if e.IsDir() && strings.HasPrefix(name, walShardDirPrefix) {
			var idx int
			if _, err := fmt.Sscanf(strings.TrimPrefix(name, walShardDirPrefix), "%d", &idx); err == nil && idx >= n {
				return fmt.Errorf("%s has shard directory %s but wal_shards=%d; refusing to strand its data - keep the old shard count until it is drained, or use a new dir", dir, name, n)
			}
		}
	}
	return nil
}

func (s *ShardedFileChannel) Name() string { return s.name }

func (s *ShardedFileChannel) Put(ctx context.Context, e *Event) error {
	if atomic.LoadInt32(&s.closed) == 1 {
		return ErrClosed
	}
	i := int(atomic.AddInt64(&s.putSeq, 1) % int64(len(s.shards)))
	return s.shards[i].Put(ctx, e)
}

// Take waits on every shard's ready queue at once, so a busy shard is
// never starved behind an idle one.
func (s *ShardedFileChannel) Take(ctx context.Context) (*Event, error) {
	cases := make([]reflect.SelectCase, 0, len(s.shards)+2)
	for _, fc := range s.shards {
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(fc.ready)})
	}
	doneIdx := len(cases)
	cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())})
	closeIdx := len(cases)
	cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(s.done)})
	for {
		chosen, val, ok := reflect.Select(cases)
		switch {
		case chosen == doneIdx:
			return nil, ctx.Err()
		case chosen == closeIdx || !ok:
			return nil, ErrClosed
		}
		if e, ok := s.shards[chosen].consume(val.Interface().(token)); ok {
			return e, nil
		}
	}
}

func (s *ShardedFileChannel) Stats() Stats {
	var t Stats
	for _, fc := range s.shards {
		st := fc.Stats()
		t.Capacity += st.Capacity
		t.Size += st.Size
		t.PutCount += st.PutCount
		t.TakeCount += st.TakeCount
		t.DroppedCount += st.DroppedCount
	}
	return t
}

func (s *ShardedFileChannel) PurgedUnconsumed() int64 {
	var n int64
	for _, fc := range s.shards {
		n += fc.PurgedUnconsumed()
	}
	return n
}

// WALShards reports how many parallel WALs back this channel.
func (s *ShardedFileChannel) WALShards() int { return len(s.shards) }

func (s *ShardedFileChannel) Close() error {
	var first error
	if atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		close(s.done)
	}
	for _, fc := range s.shards {
		if err := fc.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
