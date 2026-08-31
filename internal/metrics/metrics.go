package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

type Counter struct {
	v int64
}

func (c *Counter) Inc() { atomic.AddInt64(&c.v, 1) }

func (c *Counter) Add(n int64) { atomic.AddInt64(&c.v, n) }

func (c *Counter) Value() int64 { return atomic.LoadInt64(&c.v) }

type Registry struct {
	mu       sync.Mutex
	counters map[string]*Counter
}

func NewRegistry() *Registry {
	return &Registry{counters: make(map[string]*Counter)}
}

func (r *Registry) Counter(name string) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.counters[name]
	if !ok {
		c = &Counter{}
		r.counters[name] = c
	}
	return c
}

func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		names := make([]string, 0, len(r.counters))
		for n := range r.counters {
			names = append(names, n)
		}
		sort.Strings(names)
		var sb strings.Builder
		for _, n := range names {
			fmt.Fprintf(&sb, "# TYPE %s counter\n%s %d\n", n, n, r.counters[n].Value())
		}
		r.mu.Unlock()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.Write([]byte(sb.String()))
	})
}
