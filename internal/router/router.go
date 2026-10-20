// Package router implements Flume-style channel selectors: the piece that
// decides, for each event a source admits, which channel(s) it goes to.
// A "replicating" selector fans every event out to every configured
// channel (e.g. one channel feeding a local disk sink, another feeding a
// remote aggregator, both getting every event). A "multiplexing" selector
// routes each event to a subset of channels chosen by the value of one of
// its headers (e.g. send events tagged env=prod to the prod channel and
// everything else to a default channel) - useful for splitting one
// firehose into per-destination or per-priority pipelines.
package router

import (
	"context"
	"fmt"

	"logflow/internal/channel"
)

// Putter is satisfied by channel.Channel and by Router itself, so sources
// can be handed either a single channel or a full router without caring
// which.
type Putter interface {
	Put(ctx context.Context, e *channel.Event) error
}

// Selector picks, for a given event, which of the named channels it should
// be written to.
type Selector interface {
	Select(e *channel.Event) []string
}

// Router fans an event out to whichever channels its Selector names.
type Router struct {
	channels map[string]channel.Channel
	selector Selector
}

func New(channels map[string]channel.Channel, selector Selector) *Router {
	return &Router{channels: channels, selector: selector}
}

// Put writes e to every channel the selector names, in order. It returns
// the first error encountered but still attempts every target, since a
// backpressured or failed write to one channel shouldn't silently skip
// writes to the others in a replicating fan-out.
func (r *Router) Put(ctx context.Context, e *channel.Event) error {
	names := r.selector.Select(e)
	var firstErr error
	for _, name := range names {
		ch, ok := r.channels[name]
		if !ok {
			if firstErr == nil {
				firstErr = fmt.Errorf("router: unknown channel %q", name)
			}
			continue
		}
		if err := ch.Put(ctx, e); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ReplicatingSelector sends every event to every one of a fixed set of
// channels.
type ReplicatingSelector struct {
	Channels []string
}

func (s *ReplicatingSelector) Select(_ *channel.Event) []string { return s.Channels }

// MultiplexingSelector routes an event based on the value of one header:
// Mapping[value] gives the channel(s) for that value, falling back to
// Default when the header is absent or its value has no mapping entry.
type MultiplexingSelector struct {
	Header  string
	Mapping map[string][]string
	Default []string
}

func (s *MultiplexingSelector) Select(e *channel.Event) []string {
	if e.Headers != nil {
		if v, ok := e.Headers[s.Header]; ok {
			if targets, ok := s.Mapping[v]; ok {
				return targets
			}
		}
	}
	return s.Default
}
