// Package metrics wraps github.com/prometheus/client_golang so every agent
// exposes real Prometheus metrics - counters, gauges, and (new) histograms
// for batch latency - rather than the flat hand-rolled counter set this
// package started as. The exported API intentionally keeps the same shape
// call sites already used (Counter(name).Inc()/.Add(n)), so upgrading to
// client_golang didn't require touching every source/sink; it just adds
// Gauge/Histogram alongside Counter and swaps what's underneath all three.
package metrics

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry owns one Prometheus registry per agent process and lazily
// creates/registers a metric the first time each name is asked for, so call
// sites don't need to know about registration order or do it up front.
type Registry struct {
	reg *prometheus.Registry

	mu         sync.Mutex
	counters   map[string]prometheus.Counter
	gauges     map[string]prometheus.Gauge
	histograms map[string]prometheus.Histogram
}

func NewRegistry() *Registry {
	reg := prometheus.NewRegistry()
	// Standard process/Go-runtime metrics (goroutines, GC pauses, RSS,
	// open FDs, ...) - the usual "is this process healthy" baseline every
	// Prometheus-instrumented service exposes for free.
	reg.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	reg.MustRegister(prometheus.NewGoCollector())
	return &Registry{
		reg:        reg,
		counters:   make(map[string]prometheus.Counter),
		gauges:     make(map[string]prometheus.Gauge),
		histograms: make(map[string]prometheus.Histogram),
	}
}

// Counter returns the named counter, creating and registering it on first
// use. name should already be a valid, final Prometheus metric name (e.g.
// "logflow_events_dropped_total") - callers have always passed these as
// literal strings, so there's no templating/label-formatting step here.
func (r *Registry) Counter(name string) prometheus.Counter {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.counters[name]
	if !ok {
		c = prometheus.NewCounter(prometheus.CounterOpts{Name: name})
		r.reg.MustRegister(c)
		r.counters[name] = c
	}
	return c
}

// Gauge returns the named gauge, creating and registering it on first use.
func (r *Registry) Gauge(name string) prometheus.Gauge {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.gauges[name]
	if !ok {
		g = prometheus.NewGauge(prometheus.GaugeOpts{Name: name})
		r.reg.MustRegister(g)
		r.gauges[name] = g
	}
	return g
}

// Histogram returns the named histogram, creating and registering it (with
// buckets tuned for sub-second batch-send latencies rather than
// client_golang's web-request-shaped defaults) on first use.
func (r *Registry) Histogram(name string) prometheus.Histogram {
	r.mu.Lock()
	defer r.mu.Unlock()
	h, ok := r.histograms[name]
	if !ok {
		h = prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: name,
			// 1ms, 2ms, 5ms, 10ms ... 10s - batch sends in this codebase
			// run from sub-millisecond (localhost, small batch) to a few
			// hundred ms (loaded network sink with retries), so this
			// covers the realistic range with enough resolution for a
			// meaningful p50/p95/p99 without an absurd bucket count.
			Buckets: prometheus.ExponentialBucketsRange(0.001, 10, 20),
		})
		r.reg.MustRegister(h)
		r.histograms[name] = h
	}
	return h
}

// Registerer exposes the underlying prometheus.Registerer so callers that
// need a custom Collector (see agent.go's per-channel gauge collector) can
// register it directly rather than going through the name-based helpers
// above, which only fit flat (unlabeled) metrics.
func (r *Registry) Registerer() prometheus.Registerer { return r.reg }

// Handler returns the agent's /metrics endpoint: the standard Prometheus
// text exposition format via promhttp, instead of the hand-rolled encoder
// this package used to carry.
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{Registry: r.reg})
}
