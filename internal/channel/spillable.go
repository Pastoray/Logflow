package channel

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// DurableOverflow is what a spillable channel spills into: a single
// FileChannel or a ShardedFileChannel (parallel WALs).
type DurableOverflow interface {
	Channel
	PurgedUnconsumed() int64
}

// SpillableMemoryChannel is Flume's "spillable memory channel" pattern: a
// fast MemoryChannel is the primary path, but when it's full a Put doesn't
// block or drop - it spills to a durable FileChannel overflow instead.
// This gets you the low latency of an in-memory channel for the common
// case (a downstream sink keeping up) while still surviving a burst that
// outpaces the sink, without either OOMing an unbounded queue or losing
// data the way a plain bounded MemoryChannel would once full.
//
// Take drains the overflow with priority over the primary: if events are
// sitting in the durable overflow, they were put there because the system
// was already falling behind, so clearing that backlog is prioritized over
// serving freshly-arriving events - the same policy Flume's implementation
// uses. Under sustained overload this can still starve fresh events behind
// a growing backlog; it does not reorder events within either the primary
// or the overflow individually.
type SpillableMemoryChannel struct {
	name     string
	primary  *MemoryChannel
	overflow DurableOverflow

	// How long Take polls each source when neither has anything ready,
	// before checking the caller's context and trying again.
	pollInterval time.Duration

	spilled int64 // events routed to overflow instead of primary
}

func NewSpillableMemoryChannel(name string, primary *MemoryChannel, overflow DurableOverflow, pollInterval time.Duration) *SpillableMemoryChannel {
	if pollInterval <= 0 {
		pollInterval = 5 * time.Millisecond
	}
	return &SpillableMemoryChannel{
		name: name, primary: primary, overflow: overflow,
		pollInterval: pollInterval,
	}
}

func (s *SpillableMemoryChannel) Name() string { return s.name }

// Put tries the primary in-memory channel first with a single
// non-blocking attempt; if it has no room right now, the event spills to
// the durable overflow channel instead, which then applies its own
// (blocking) backpressure against the caller's context. A full primary is
// not itself counted as a drop - only genuine loss (the overflow also
// refusing the event) is.
//
// This deliberately does not retry the primary before spilling. Take()
// drains the overflow with priority (see type doc), so once the primary
// has filled and spilling has begun, it tends to stay full until the
// overflow backlog clears - retrying with a sleep in between would add
// that sleep's latency to every single event during exactly the period
// the system is most behind, which is the opposite of what a channel
// meant to absorb bursts should do.
func (s *SpillableMemoryChannel) Put(ctx context.Context, e *Event) error {
	if s.primary.TryPut(e) {
		return nil
	}
	atomic.AddInt64(&s.spilled, 1)
	return s.overflow.Put(ctx, e)
}

// Take prefers draining the overflow (see type doc), falling back to the
// primary, and polls both briefly rather than blocking indefinitely on
// either alone so a burst of new primary events doesn't get stuck behind
// an empty-but-still-open overflow poll.
func (s *SpillableMemoryChannel) Take(ctx context.Context) (*Event, error) {
	for {
		if e, err := s.tryTake(ctx, s.overflow); err == nil {
			return e, nil
		} else if !errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		if e, err := s.tryTake(ctx, s.primary); err == nil {
			return e, nil
		} else if !errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}
}

func (s *SpillableMemoryChannel) tryTake(parent context.Context, ch Channel) (*Event, error) {
	ctx, cancel := context.WithTimeout(parent, s.pollInterval)
	defer cancel()
	e, err := ch.Take(ctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, context.DeadlineExceeded // just empty right now
		}
		return nil, err
	}
	return e, nil
}

func (s *SpillableMemoryChannel) Stats() Stats {
	p := s.primary.Stats()
	o := s.overflow.Stats()
	return Stats{
		Capacity:     p.Capacity + o.Capacity,
		Size:         p.Size + o.Size,
		PutCount:     p.PutCount + o.PutCount,
		TakeCount:    p.TakeCount + o.TakeCount,
		DroppedCount: p.DroppedCount + o.DroppedCount,
	}
}

// Spilled reports how many events have been routed to the durable
// overflow instead of the fast in-memory primary - a useful signal that
// downstream consumers aren't keeping up with the ingest rate.
func (s *SpillableMemoryChannel) Spilled() int64 { return atomic.LoadInt64(&s.spilled) }

// PurgedUnconsumed passes through the overflow FileChannel's retention-purge
// count, so /stats can report it uniformly whether retention is configured
// on a plain file channel or on a spillable channel's overflow.
func (s *SpillableMemoryChannel) PurgedUnconsumed() int64 { return s.overflow.PurgedUnconsumed() }

func (s *SpillableMemoryChannel) Close() error {
	err1 := s.primary.Close()
	err2 := s.overflow.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
