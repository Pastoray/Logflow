package router

import (
	"context"
	"fmt"

	"logflow/internal/channel"
)

type Putter interface {
	Put(ctx context.Context, e *channel.Event) error
}

type Selector interface {
	Select(e *channel.Event) []string
}

type Router struct {
	channels map[string]channel.Channel
	selector Selector
}

func New(channels map[string]channel.Channel, selector Selector) *Router {
	return &Router{channels: channels, selector: selector}
}

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

type ReplicatingSelector struct {
	Channels []string
}

func (s *ReplicatingSelector) Select(_ *channel.Event) []string { return s.Channels }

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
