// Package sink contains Sink implementations that drain a channel and
// forward events onward: to a downstream agent over gRPC, to rolling files
// on disk, or to stdout/discard for testing.
package sink

import (
	"context"
	"time"

	"logflow/internal/channel"
)

// drainBatch pulls events from ch until it has count events or lingerFor
// elapses since the first event arrived, whichever comes first. It returns
// a possibly-empty batch; callers should treat an empty batch as "nothing
// happened this tick" rather than an error.
func drainBatch(ctx context.Context, ch channel.Channel, count int, lingerFor time.Duration) []*channel.Event {
	first, err := ch.Take(ctx)
	if err != nil {
		return nil
	}
	batch := make([]*channel.Event, 0, count)
	batch = append(batch, first)

	deadline := time.NewTimer(lingerFor)
	defer deadline.Stop()

	for len(batch) < count {
		select {
		case <-deadline.C:
			return batch
		default:
		}
		// Use a short-lived context so a slow/empty channel doesn't block
		// past the linger deadline once we already have at least one event.
		takeCtx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
		e, err := ch.Take(takeCtx)
		cancel()
		if err != nil {
			select {
			case <-deadline.C:
				return batch
			case <-ctx.Done():
				return batch
			default:
				continue
			}
		}
		batch = append(batch, e)
	}
	return batch
}
