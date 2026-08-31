package sink

import (
	"context"
	"time"

	"logflow/internal/channel"
)

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
