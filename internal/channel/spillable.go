package channel

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

type SpillableMemoryChannel struct {
	name     string
	primary  *MemoryChannel
	overflow *FileChannel

	pollInterval time.Duration

	spilled int64
}

func NewSpillableMemoryChannel(name string, primary *MemoryChannel, overflow *FileChannel, pollInterval time.Duration) *SpillableMemoryChannel {
	if pollInterval <= 0 {
		pollInterval = 5 * time.Millisecond
	}
	return &SpillableMemoryChannel{
		name: name, primary: primary, overflow: overflow,
		pollInterval: pollInterval,
	}
}

func (s *SpillableMemoryChannel) Name() string { return s.name }

func (s *SpillableMemoryChannel) Put(ctx context.Context, e *Event) error {
	if s.primary.TryPut(e) {
		return nil
	}
	atomic.AddInt64(&s.spilled, 1)
	return s.overflow.Put(ctx, e)
}

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
			return nil, context.DeadlineExceeded
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

func (s *SpillableMemoryChannel) Spilled() int64 { return atomic.LoadInt64(&s.spilled) }

func (s *SpillableMemoryChannel) PurgedUnconsumed() int64 { return s.overflow.PurgedUnconsumed() }

func (s *SpillableMemoryChannel) Close() error {
	err1 := s.primary.Close()
	err2 := s.overflow.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
