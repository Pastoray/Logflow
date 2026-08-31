package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"logflow/internal/interceptor"
)

type TLSConfig struct {
	Enabled bool `yaml:"enabled"`

	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`

	CAFile string `yaml:"ca_file"`

	ClientAuth bool `yaml:"client_auth"`

	ServerName string `yaml:"server_name"`

	InsecureSkipVerify bool `yaml:"insecure_skip_verify"`
}

type Config struct {
	AgentID    string            `yaml:"agent_id"`
	HTTPAddr   string            `yaml:"http_addr"`
	Sources    []SourceConfig    `yaml:"sources"`
	Channels   []ChannelConfig   `yaml:"channels"`
	Sinks      []SinkConfig      `yaml:"sinks"`
	SinkGroups []SinkGroupConfig `yaml:"sink_groups"`

	DrainTimeout time.Duration `yaml:"drain_timeout"`
}

type SourceConfig struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`

	Channel  string          `yaml:"channel"`
	Channels []string        `yaml:"channels"`
	Selector *SelectorConfig `yaml:"selector"`

	Interceptors []interceptor.Spec `yaml:"interceptors"`

	ListenAddr string `yaml:"listen_addr"`

	TLS       *TLSConfig `yaml:"tls"`
	AuthToken string     `yaml:"auth_token"`

	Path         string        `yaml:"path"`
	PollInterval time.Duration `yaml:"poll_interval"`
}

type SelectorConfig struct {
	Type string `yaml:"type"`

	Header  string              `yaml:"header"`
	Mapping map[string][]string `yaml:"mapping"`
	Default []string            `yaml:"default"`
}

type ChannelConfig struct {
	Name     string `yaml:"name"`
	Type     string `yaml:"type"`
	Capacity int    `yaml:"capacity"`
	Shards   int    `yaml:"shards"`

	Dir            string `yaml:"dir"`
	MaxSegmentSize int64  `yaml:"max_segment_bytes"`

	OverflowCapacity int           `yaml:"overflow_capacity"`
	PollInterval     time.Duration `yaml:"poll_interval"`

	RetentionMaxAge    time.Duration `yaml:"retention_max_age"`
	RetentionMaxBytes  int64         `yaml:"retention_max_bytes"`
	CompactionInterval time.Duration `yaml:"compaction_interval"`
}

type SinkConfig struct {
	Name    string `yaml:"name"`
	Type    string `yaml:"type"`
	Channel string `yaml:"channel"`

	BatchSize   int           `yaml:"batch_size"`
	BatchLinger time.Duration `yaml:"batch_linger"`
	Parallelism int           `yaml:"parallelism"`

	TargetAddr      string        `yaml:"target_addr"`
	Compression     bool          `yaml:"compression"`
	InFlightBatches int           `yaml:"in_flight_batches"`
	MaxRetries      int           `yaml:"max_retries"`
	RetryBackoff    time.Duration `yaml:"retry_backoff"`

	TLS       *TLSConfig `yaml:"tls"`
	AuthToken string     `yaml:"auth_token"`

	Dir          string        `yaml:"dir"`
	RollMaxBytes int64         `yaml:"roll_max_bytes"`
	RollInterval time.Duration `yaml:"roll_interval"`
}

type SinkGroupConfig struct {
	Name      string   `yaml:"name"`
	Channel   string   `yaml:"channel"`
	Processor string   `yaml:"processor"`
	Selector  string   `yaml:"selector"`
	Targets   []string `yaml:"targets"`

	BatchSize       int           `yaml:"batch_size"`
	BatchLinger     time.Duration `yaml:"batch_linger"`
	Parallelism     int           `yaml:"parallelism"`
	InFlightBatches int           `yaml:"in_flight_batches"`
	MaxRetries      int           `yaml:"max_retries"`
	RetryBackoff    time.Duration `yaml:"retry_backoff"`
	Compression     bool          `yaml:"compression"`

	TLS       *TLSConfig `yaml:"tls"`
	AuthToken string     `yaml:"auth_token"`

	RecoveryProbeInterval time.Duration `yaml:"recovery_probe_interval"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	c.applyDefaults()
	if err := c.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return &c, nil
}

func (c *Config) applyDefaults() {
	if c.AgentID == "" {
		c.AgentID = "agent-1"
	}
	if c.HTTPAddr == "" {
		c.HTTPAddr = ":9100"
	}
	if c.DrainTimeout == 0 {
		c.DrainTimeout = 30 * time.Second
	}
	for i := range c.Channels {
		ch := &c.Channels[i]
		if ch.Type == "" {
			ch.Type = "memory"
		}
		if ch.Capacity == 0 {
			ch.Capacity = 10000
		}
		if ch.Shards == 0 {
			ch.Shards = 4
		}
		if ch.MaxSegmentSize == 0 {
			ch.MaxSegmentSize = 64 << 20
		}
		if ch.Type == "spillable" {
			if ch.OverflowCapacity == 0 {
				ch.OverflowCapacity = 200000
			}
			if ch.PollInterval == 0 {
				ch.PollInterval = 5 * time.Millisecond
			}
		}
		if (ch.Type == "file" || ch.Type == "spillable") && (ch.RetentionMaxAge > 0 || ch.RetentionMaxBytes > 0) && ch.CompactionInterval == 0 {
			ch.CompactionInterval = 30 * time.Second
		}
	}
	for i := range c.Sources {
		s := &c.Sources[i]
		if s.PollInterval == 0 {
			s.PollInterval = 500 * time.Millisecond
		}
	}
	for i := range c.Sinks {
		s := &c.Sinks[i]
		if s.BatchSize == 0 {
			s.BatchSize = 500
		}
		if s.BatchLinger == 0 {
			s.BatchLinger = 200 * time.Millisecond
		}
		if s.Parallelism == 0 {
			s.Parallelism = 2
		}
		if s.InFlightBatches == 0 {
			s.InFlightBatches = 8
		}
		if s.MaxRetries == 0 {
			s.MaxRetries = 5
		}
		if s.RetryBackoff == 0 {
			s.RetryBackoff = 500 * time.Millisecond
		}
		if s.RollMaxBytes == 0 {
			s.RollMaxBytes = 128 << 20
		}
		if s.RollInterval == 0 {
			s.RollInterval = 1 * time.Minute
		}
	}
	for i := range c.SinkGroups {
		g := &c.SinkGroups[i]
		if g.Processor == "" {
			g.Processor = "load_balance"
		}
		if g.Selector == "" {
			g.Selector = "round_robin"
		}
		if g.BatchSize == 0 {
			g.BatchSize = 500
		}
		if g.BatchLinger == 0 {
			g.BatchLinger = 200 * time.Millisecond
		}
		if g.Parallelism == 0 {
			g.Parallelism = len(g.Targets)
			if g.Parallelism == 0 {
				g.Parallelism = 1
			}
		}
		if g.InFlightBatches == 0 {
			g.InFlightBatches = 8
		}
		if g.MaxRetries == 0 {
			g.MaxRetries = 5
		}
		if g.RetryBackoff == 0 {
			g.RetryBackoff = 500 * time.Millisecond
		}
		if g.RecoveryProbeInterval == 0 {
			g.RecoveryProbeInterval = 10 * time.Second
		}
	}
}

func (c *Config) validate() error {
	if len(c.Channels) == 0 {
		return fmt.Errorf("at least one channel is required")
	}
	names := map[string]bool{}
	for _, ch := range c.Channels {
		if ch.Name == "" {
			return fmt.Errorf("channel missing name")
		}
		names[ch.Name] = true
		switch ch.Type {
		case "memory":
		case "file", "spillable":
			if ch.Dir == "" {
				return fmt.Errorf("channel %q: dir is required for type %q", ch.Name, ch.Type)
			}
			if ch.RetentionMaxAge < 0 || ch.RetentionMaxBytes < 0 {
				return fmt.Errorf("channel %q: retention_max_age and retention_max_bytes must not be negative", ch.Name)
			}
		default:
			return fmt.Errorf("channel %q: unknown type %q", ch.Name, ch.Type)
		}
	}
	for _, s := range c.Sources {
		targets := s.Channel
		list := s.Channels
		if targets != "" {
			list = append(list, targets)
		}
		if len(list) == 0 {
			return fmt.Errorf("source %q must set channel or channels", s.Name)
		}
		for _, n := range list {
			if !names[n] {
				return fmt.Errorf("source %q references unknown channel %q", s.Name, n)
			}
		}
		if s.Selector != nil {
			switch s.Selector.Type {
			case "replicating", "":
			case "multiplexing":
				for _, targets := range s.Selector.Mapping {
					for _, n := range targets {
						if !names[n] {
							return fmt.Errorf("source %q selector mapping references unknown channel %q", s.Name, n)
						}
					}
				}
				for _, n := range s.Selector.Default {
					if !names[n] {
						return fmt.Errorf("source %q selector default references unknown channel %q", s.Name, n)
					}
				}
			default:
				return fmt.Errorf("source %q: unknown selector type %q", s.Name, s.Selector.Type)
			}
		}
		if err := validateServerTLS(s.TLS); err != nil {
			return fmt.Errorf("source %q tls: %w", s.Name, err)
		}
	}
	for _, s := range c.Sinks {
		if !names[s.Channel] {
			return fmt.Errorf("sink %q references unknown channel %q", s.Name, s.Channel)
		}
		if err := validateClientTLS(s.TLS); err != nil {
			return fmt.Errorf("sink %q tls: %w", s.Name, err)
		}
	}
	for _, g := range c.SinkGroups {
		if !names[g.Channel] {
			return fmt.Errorf("sink group %q references unknown channel %q", g.Name, g.Channel)
		}
		if len(g.Targets) < 2 {
			return fmt.Errorf("sink group %q needs at least 2 targets", g.Name)
		}
		switch g.Processor {
		case "load_balance", "failover":
		default:
			return fmt.Errorf("sink group %q: unknown processor %q", g.Name, g.Processor)
		}
		if err := validateClientTLS(g.TLS); err != nil {
			return fmt.Errorf("sink group %q tls: %w", g.Name, err)
		}
	}
	return nil
}

func validateServerTLS(t *TLSConfig) error {
	if t == nil || !t.Enabled {
		return nil
	}
	if t.CertFile == "" || t.KeyFile == "" {
		return fmt.Errorf("cert_file and key_file are required when tls.enabled is true")
	}
	if t.ClientAuth && t.CAFile == "" {
		return fmt.Errorf("ca_file is required when client_auth is true")
	}
	return nil
}

func validateClientTLS(t *TLSConfig) error {
	if t == nil || !t.Enabled {
		return nil
	}
	if t.CAFile == "" && !t.InsecureSkipVerify {
		return fmt.Errorf("ca_file is required unless insecure_skip_verify is true")
	}
	if (t.CertFile == "") != (t.KeyFile == "") {
		return fmt.Errorf("cert_file and key_file must both be set for client mTLS, or both left empty")
	}
	return nil
}
