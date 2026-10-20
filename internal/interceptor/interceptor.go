// Package interceptor implements Flume-style interceptors: a chain of
// pluggable transforms that runs on every event a source admits, before it
// reaches any channel. An interceptor can rewrite an event (e.g. add
// headers) or drop it entirely (return nil), which is how event filtering,
// enrichment, and tagging are done without touching source code.
package interceptor

import (
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"time"

	"logflow/internal/channel"
)

// Interceptor transforms or filters a single event. Returning nil drops the
// event - it will not reach any channel.
type Interceptor interface {
	Intercept(e *channel.Event) *channel.Event
}

// Chain applies a sequence of interceptors in order, short-circuiting as
// soon as one of them drops the event.
type Chain []Interceptor

func (c Chain) Intercept(e *channel.Event) *channel.Event {
	for _, ic := range c {
		if e == nil {
			return nil
		}
		e = ic.Intercept(e)
	}
	return e
}

// Spec is the generic, YAML-friendly description of one interceptor: a type
// name plus a small string-keyed parameter bag. Keeping config generic here
// (rather than one struct per interceptor type) means adding a new
// interceptor never requires a config schema change.
type Spec struct {
	Type   string            `yaml:"type"`
	Params map[string]string `yaml:"params"`
}

// Build turns a list of specs into a Chain, in order.
func Build(specs []Spec) (Chain, error) {
	chain := make(Chain, 0, len(specs))
	for _, s := range specs {
		ic, err := build(s)
		if err != nil {
			return nil, fmt.Errorf("interceptor %q: %w", s.Type, err)
		}
		chain = append(chain, ic)
	}
	return chain, nil
}

func build(s Spec) (Interceptor, error) {
	switch s.Type {
	case "timestamp":
		header := s.Params["header"]
		if header == "" {
			header = "timestamp"
		}
		preserve, _ := strconv.ParseBool(s.Params["preserve_existing"])
		return &TimestampInterceptor{Header: header, PreserveExisting: preserve}, nil

	case "host":
		header := s.Params["header"]
		if header == "" {
			header = "host"
		}
		useIP, _ := strconv.ParseBool(s.Params["use_ip"])
		return &HostInterceptor{Header: header, UseIP: useIP}, nil

	case "static":
		key := s.Params["key"]
		if key == "" {
			return nil, fmt.Errorf("static interceptor requires params.key")
		}
		return &StaticInterceptor{Key: key, Value: s.Params["value"]}, nil

	case "regex_filter":
		pattern := s.Params["pattern"]
		if pattern == "" {
			return nil, fmt.Errorf("regex_filter interceptor requires params.pattern")
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("compile pattern: %w", err)
		}
		exclude, _ := strconv.ParseBool(s.Params["exclude"])
		return &RegexFilterInterceptor{Regexp: re, Exclude: exclude}, nil

	case "regex_extractor":
		pattern := s.Params["pattern"]
		if pattern == "" {
			return nil, fmt.Errorf("regex_extractor interceptor requires params.pattern")
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("compile pattern: %w", err)
		}
		names := re.SubexpNames()
		return &RegexExtractorInterceptor{Regexp: re, GroupNames: names}, nil

	default:
		return nil, fmt.Errorf("unknown interceptor type %q", s.Type)
	}
}

// TimestampInterceptor stamps the event with the time the agent observed
// it, as milliseconds since epoch in a header - the conventional signal
// downstream sinks/indexers use for "when did this happen" absent a
// timestamp parsed from the event body itself.
type TimestampInterceptor struct {
	Header           string
	PreserveExisting bool
}

func (t *TimestampInterceptor) Intercept(e *channel.Event) *channel.Event {
	if e.Headers == nil {
		e.Headers = map[string]string{}
	}
	if t.PreserveExisting {
		if _, ok := e.Headers[t.Header]; ok {
			return e
		}
	}
	e.Headers[t.Header] = strconv.FormatInt(time.Now().UnixMilli(), 10)
	return e
}

// HostInterceptor tags every event with the agent's own hostname or first
// non-loopback IP, so events can be traced back to the machine that
// ingested them once they've been aggregated with events from many hosts.
type HostInterceptor struct {
	Header string
	UseIP  bool
}

func (h *HostInterceptor) Intercept(e *channel.Event) *channel.Event {
	if e.Headers == nil {
		e.Headers = map[string]string{}
	}
	e.Headers[h.Header] = hostValue(h.UseIP)
	return e
}

var cachedHostValue struct {
	ip   string
	name string
}

func hostValue(useIP bool) string {
	if useIP {
		if cachedHostValue.ip == "" {
			cachedHostValue.ip = firstNonLoopbackIP()
		}
		return cachedHostValue.ip
	}
	if cachedHostValue.name == "" {
		name, err := os.Hostname()
		if err != nil {
			name = "unknown"
		}
		cachedHostValue.name = name
	}
	return cachedHostValue.name
}

func firstNonLoopbackIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "unknown"
	}
	for _, a := range addrs {
		if ipNet, ok := a.(*net.IPNet); ok && !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
			return ipNet.IP.String()
		}
	}
	return "unknown"
}

// StaticInterceptor adds a fixed key/value header to every event - useful
// for tagging all events from one source with, e.g., an environment or
// tenant name.
type StaticInterceptor struct {
	Key   string
	Value string
}

func (s *StaticInterceptor) Intercept(e *channel.Event) *channel.Event {
	if e.Headers == nil {
		e.Headers = map[string]string{}
	}
	e.Headers[s.Key] = s.Value
	return e
}

// RegexFilterInterceptor drops events whose body does/doesn't match a
// pattern, depending on Exclude. This is how you'd, e.g., drop DEBUG-level
// lines before they ever reach a channel instead of paying to buffer and
// forward them.
type RegexFilterInterceptor struct {
	Regexp  *regexp.Regexp
	Exclude bool
}

func (r *RegexFilterInterceptor) Intercept(e *channel.Event) *channel.Event {
	matched := r.Regexp.Match(e.Body)
	if matched == r.Exclude {
		return nil
	}
	return e
}

// RegexExtractorInterceptor promotes regex capture groups from the event
// body into headers, keyed by the group's name (?P<name>...) if present,
// otherwise by its 1-based index - handy for pulling structured fields
// (level, request id, ...) out of semi-structured log lines for
// multiplexing or downstream filtering without a full parser.
type RegexExtractorInterceptor struct {
	Regexp     *regexp.Regexp
	GroupNames []string
}

func (r *RegexExtractorInterceptor) Intercept(e *channel.Event) *channel.Event {
	m := r.Regexp.FindSubmatch(e.Body)
	if m == nil {
		return e
	}
	if e.Headers == nil {
		e.Headers = map[string]string{}
	}
	for i := 1; i < len(m); i++ {
		name := ""
		if i < len(r.GroupNames) {
			name = r.GroupNames[i]
		}
		if name == "" {
			name = strconv.Itoa(i)
		}
		e.Headers[name] = string(m[i])
	}
	return e
}
