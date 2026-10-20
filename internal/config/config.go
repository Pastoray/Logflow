// Package config defines the YAML-driven topology for a logflow agent:
// one or more sources feeding one or more channels, drained by one or more
// sinks - mirroring Apache Flume's source/channel/sink model.
package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"logflow/internal/interceptor"
)

// TLSConfig configures transport security for one gRPC endpoint - a
// server (source) or a client (sink / sink group). The same struct serves
// both roles; which fields matter depends on which side it's attached to.
type TLSConfig struct {
	Enabled bool `yaml:"enabled"`

	// Server: this endpoint's own identity, presented to connecting
	// clients. Client: this endpoint's identity, presented to the server
	// only when the server requires a client certificate (mTLS).
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`

	// Server: path to a CA bundle used to verify client certificates -
	// only consulted if ClientAuth is true (mTLS). Client: path to a CA
	// bundle used to verify the server's certificate.
	CAFile string `yaml:"ca_file"`

	// Server only: require and verify a client certificate (mutual TLS).
	ClientAuth bool `yaml:"client_auth"`

	// Client only: override the hostname used for server certificate
	// verification (SNI + hostname check), e.g. when TargetAddr is an IP
	// but the certificate was issued for a DNS name.
	ServerName string `yaml:"server_name"`

	// Client only, testing use: skip server certificate verification
	// entirely. Never use this against an untrusted network.
	InsecureSkipVerify bool `yaml:"insecure_skip_verify"`
}

type Config struct {
	AgentID    string            `yaml:"agent_id"`
	HTTPAddr   string            `yaml:"http_addr"` // metrics + health
	Sources    []SourceConfig    `yaml:"sources"`
	Channels   []ChannelConfig   `yaml:"channels"`
	Sinks      []SinkConfig      `yaml:"sinks"`
	SinkGroups []SinkGroupConfig `yaml:"sink_groups"`

	// DrainTimeout bounds how long shutdown will wait for sinks to finish
	// flushing whatever is already buffered in channels once sources have
	// stopped admitting new events, before giving up and closing anyway.
	DrainTimeout time.Duration `yaml:"drain_timeout"`
}

type SourceConfig struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"` // grpc | filetail

	// Channel wiring: either a single `channel`, or `channels` + `selector`
	// for fan-out to more than one. `channel` is shorthand for
	// `channels: [<name>]` with an implicit selector that always picks it.
	Channel  string          `yaml:"channel"`
	Channels []string        `yaml:"channels"`
	Selector *SelectorConfig `yaml:"selector"`

	// Interceptors run, in order, on every event this source admits,
	// before channel selection - see internal/interceptor.
	Interceptors []interceptor.Spec `yaml:"interceptors"`

	// grpc
	ListenAddr string `yaml:"listen_addr"`

	// Transport security for the gRPC server, and an optional shared-secret
	// bearer token every incoming call must present (checked independently
	// of, and in addition to, TLS/mTLS).
	TLS       *TLSConfig `yaml:"tls"`
	AuthToken string     `yaml:"auth_token"`

	// AuthTokensFile enables per-caller tokens (identity, expiry,
	// revocation, hot reload) instead of the single shared auth_token;
	// the two are mutually exclusive. See internal/security/tokens.go.
	AuthTokensFile string `yaml:"auth_tokens_file"`

	// filetail
	Path         string        `yaml:"path"`
	PollInterval time.Duration `yaml:"poll_interval"`
}

// SelectorConfig picks the channel-selector strategy for a source with more
// than one target channel.
type SelectorConfig struct {
	Type string `yaml:"type"` // replicating | multiplexing

	// multiplexing
	Header  string              `yaml:"header"`
	Mapping map[string][]string `yaml:"mapping"`
	Default []string            `yaml:"default"`
}

type ChannelConfig struct {
	Name     string `yaml:"name"`
	Type     string `yaml:"type"` // memory | file | spillable
	Capacity int    `yaml:"capacity"`
	Shards   int    `yaml:"shards"`

	// file channel; also used by spillable channel's overflow
	Dir            string `yaml:"dir"`
	MaxSegmentSize int64  `yaml:"max_segment_bytes"`
	// WALShards > 1 splits the durable log into that many parallel WALs
	// (dir/wal-0 ... wal-N-1), each with its own write segment and lock, so
	// durable Put throughput scales with the count. Default 1 = the single
	// WAL layout. Changing it on a non-empty dir is refused (see
	// channel.NewShardedFileChannel).
	WALShards int `yaml:"wal_shards"`

	// spillable channel only: overflow capacity is independent of the
	// primary's `capacity` above, since the overflow is a durable file
	// channel meant to absorb a much larger backlog than memory could.
	// poll_interval controls how long Take() waits on each of the primary
	// and overflow sources before checking the other (see
	// SpillableMemoryChannel for why Put() itself doesn't need a similar
	// knob: it's a single non-blocking attempt, not a retry loop).
	OverflowCapacity int           `yaml:"overflow_capacity"`
	PollInterval     time.Duration `yaml:"poll_interval"`

	// Retention (file and spillable channels only): an optional active
	// backstop, independent of normal consumption, bounding how long WAL
	// segments are kept at all. Both default to 0 (disabled - never
	// purge based on age/size, only ever via normal full-consumption
	// cleanup). If either fires, the oldest segment(s) are purged even if
	// they still have unconsumed entries - the same trade-off Kafka's
	// retention.ms/retention.bytes make.
	RetentionMaxAge    time.Duration `yaml:"retention_max_age"`
	RetentionMaxBytes  int64         `yaml:"retention_max_bytes"`
	CompactionInterval time.Duration `yaml:"compaction_interval"`

	// DrainTimeout, if set, overrides the agent-wide drain_timeout for
	// this channel only: how long shutdown waits for sinks to finish
	// flushing THIS channel's buffered events. Per channel rather than per
	// sink because "drained" is defined by a channel emptying - several
	// sinks on one channel compete for the same events, so a per-sink
	// budget would have no well-defined meaning. Zero inherits the
	// agent-wide value.
	DrainTimeout time.Duration `yaml:"drain_timeout"`

	// Cluster, if set, replicates this channel across multiple agent
	// processes via Raft (internal/cluster) instead of running as a
	// single standalone node: every event Put() is committed to a
	// majority of the group before Put() returns, every replica ends up
	// with a full durable copy (via its own underlying channel - file
	// channel strongly recommended so the replica survives a restart),
	// and only the current Raft leader actively drains the channel for
	// its sinks, so output isn't triplicated. See internal/cluster's doc
	// comment for the full design and its trade-offs. Mutually exclusive
	// with ShardedCluster.
	Cluster *ClusterConfig `yaml:"cluster"`

	// ShardedCluster, if set, partitions this channel across several
	// INDEPENDENT Raft groups (shards) instead of just one - see
	// internal/cluster/sharded.go's doc comment for the design (why this
	// is the way past a single group's one-leader throughput ceiling,
	// and what it deliberately doesn't do: no cross-shard ordering, no
	// auto-rebalancing). Mutually exclusive with Cluster.
	ShardedCluster *ShardedClusterConfig `yaml:"sharded_cluster"`
}

// ShardedClusterConfig describes every shard of a sharded channel. Each
// shard is a full, independent Raft group - its own peer list, its own
// ports, its own leader - configured exactly like a single Cluster block;
// nothing is shared between shards except the physical nodes they happen
// to run on.
type ShardedClusterConfig struct {
	Shards []ClusterConfig `yaml:"shards"`

	// ShardKeyHeader, if set, routes every event carrying this header to
	// the same shard (a hash of the header's value) - use this to keep
	// events that must stay in relative order (e.g. everything from one
	// source_agent) together on one shard's single ordered log. Events
	// without the header, or when this is left unset, are spread
	// round-robin across shards instead - maximum write-throughput
	// spread, with no ordering guarantee across Put() calls at all
	// (channels never promised cross-caller ordering anyway).
	ShardKeyHeader string `yaml:"shard_key_header"`
}

// ClusterPeer is one member of a Raft group, named both ways a group
// member needs to be addressed: Addr for the Raft protocol itself
// (RequestVote/AppendEntries), and ClientAddr for the "forward this write
// to whoever's actually leader" path a non-leader node takes when a client
// (a source) hands it a Put while it isn't leader.
type ClusterPeer struct {
	ID         string `yaml:"id"`
	Addr       string `yaml:"addr"`        // this peer's Raft RPC address
	ClientAddr string `yaml:"client_addr"` // this peer's client-forwarding RPC address
}

type ClusterConfig struct {
	// NodeID must match the ID of exactly one entry in Peers - this
	// node's own identity within the group. Ignored when Joining is true.
	NodeID string        `yaml:"node_id"`
	Peers  []ClusterPeer `yaml:"peers"` // bootstrap membership, including self; ignored (may be omitted) when Joining is true

	// Joining starts this node so it can be added to an ALREADY-RUNNING
	// cluster live (via that cluster's /cluster/add endpoint or
	// cluster.Node.AddServer - see internal/cluster's "Membership
	// changes" doc comment), rather than bootstrapping or rejoining one
	// from Peers. When true, Peers may be omitted entirely - Self is this
	// node's own address, and the real membership list is learned from
	// the existing leader once it starts replicating to this node as
	// part of adding it. A node started this way sits idle (answering
	// RPCs, but never calling an election itself) until it's actually
	// added.
	Joining bool         `yaml:"joining"`
	Self    *ClusterPeer `yaml:"self"` // required when Joining is true; ignored otherwise

	// DataDir persists this node's Raft term/vote/log across restarts
	// (separate from the channel's own data dir - see internal/cluster).
	DataDir string `yaml:"data_dir"`

	// ListenAddr / ClientListenAddr are what THIS node binds (default: its
	// own peers[].addr / client_addr). Use them in containers and pods,
	// where peer addresses are DNS names other nodes dial but which this
	// process can't necessarily bind yet: listen_addr ":8301" while
	// advertising "collector-0.collector.ns.svc:8301".
	ListenAddr       string `yaml:"listen_addr"`
	ClientListenAddr string `yaml:"client_listen_addr"`

	ElectionTimeoutMin time.Duration `yaml:"election_timeout_min"` // default 150ms
	ElectionTimeoutMax time.Duration `yaml:"election_timeout_max"` // default 300ms
	HeartbeatInterval  time.Duration `yaml:"heartbeat_interval"`   // default 50ms
	ApplyTimeout       time.Duration `yaml:"apply_timeout"`        // default 5s

	// SnapshotThreshold > 0 turns on Raft log compaction: after this many
	// applied entries beyond the last snapshot, the applied prefix of the
	// log is discarded (SnapshotTrailing recent entries are kept so a
	// follower that is only a little behind still catches up from the log;
	// default threshold/4). 0 (default) keeps the whole log forever. A
	// follower or new member too far behind is brought up to date with
	// InstallSnapshot, which carries the log position and membership but
	// NOT the events in the skipped range - see internal/cluster.
	SnapshotThreshold int `yaml:"snapshot_threshold"`
	SnapshotTrailing  int `yaml:"snapshot_trailing"`

	// BatchSize/BatchLinger batch many Put() calls into one Raft log
	// entry (one EventBatch), the same batch_size/batch_linger pattern
	// GRPCSink already uses: flush when BatchSize events have
	// accumulated, or BatchLinger has elapsed since the first one in the
	// batch, whichever comes first. Defaults: 200 / 50ms. This is what
	// makes clustered-channel throughput usable - see internal/cluster's
	// doc comment and the main README's Known limitations for the
	// measured difference.
	BatchSize   int           `yaml:"batch_size"`
	BatchLinger time.Duration `yaml:"batch_linger"`
}

type SinkConfig struct {
	Name    string `yaml:"name"`
	Type    string `yaml:"type"` // grpc | file | stdout | null
	Channel string `yaml:"channel"`

	// batching (all sink types)
	BatchSize   int           `yaml:"batch_size"`
	BatchLinger time.Duration `yaml:"batch_linger"`
	Parallelism int           `yaml:"parallelism"` // concurrent drain workers

	// grpc sink
	TargetAddr      string        `yaml:"target_addr"`
	Compression     bool          `yaml:"compression"`
	InFlightBatches int           `yaml:"in_flight_batches"`
	MaxRetries      int           `yaml:"max_retries"`
	RetryBackoff    time.Duration `yaml:"retry_backoff"`

	// Transport security for the gRPC client connection, and an optional
	// bearer token presented on every outgoing call.
	TLS       *TLSConfig `yaml:"tls"`
	AuthToken string     `yaml:"auth_token"`

	// file sink
	Dir          string        `yaml:"dir"`
	RollMaxBytes int64         `yaml:"roll_max_bytes"`
	RollInterval time.Duration `yaml:"roll_interval"`
}

// SinkGroupConfig describes a Flume-style "sink processor": a set of gRPC
// backends behind one logical sink, coordinated by either a load-balancing
// policy (spread batches across all healthy backends) or a failover policy
// (use one at a time, ordered by priority, falling back on failure).
type SinkGroupConfig struct {
	Name      string `yaml:"name"`
	Channel   string `yaml:"channel"`
	Processor string `yaml:"processor"` // load_balance | failover
	Selector  string `yaml:"selector"`  // load_balance only: round_robin | random
	// Targets are gRPC host:port addresses, or "file:<dir>" for a rolling
	// local-file backend (e.g. a failover group whose last target is a
	// local disk spool). failover: priority order, index 0 = primary.
	Targets []string `yaml:"targets"`

	BatchSize       int           `yaml:"batch_size"`
	BatchLinger     time.Duration `yaml:"batch_linger"`
	Parallelism     int           `yaml:"parallelism"` // load_balance only
	InFlightBatches int           `yaml:"in_flight_batches"`
	MaxRetries      int           `yaml:"max_retries"`
	RetryBackoff    time.Duration `yaml:"retry_backoff"`
	Compression     bool          `yaml:"compression"`

	// Transport security and bearer token, applied identically to every
	// backend connection this group makes (all targets share one policy).
	TLS       *TLSConfig `yaml:"tls"`
	AuthToken string     `yaml:"auth_token"`

	// Roll policy for any "file:<dir>" targets (defaults 128MiB / 1m, same
	// as a standalone file sink).
	FileRollMaxBytes int64         `yaml:"file_roll_max_bytes"`
	FileRollInterval time.Duration `yaml:"file_roll_interval"`

	// failover only: how often to probe a higher-priority target to see if
	// it has recovered and traffic should move back to it.
	RecoveryProbeInterval time.Duration `yaml:"recovery_probe_interval"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	b, err = expandEnv(b)
	if err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
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

// envRef matches ${NAME} and ${NAME:-default}. Only this exact form is
// expanded - a bare $ (regexes in interceptor configs, say) is left alone.
var envRef = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}`)

// expandEnv substitutes ${VAR} / ${VAR:-default} from the environment so
// one config file (e.g. a Kubernetes ConfigMap shared by every StatefulSet
// replica) can carry per-instance values like the pod name. An unset
// variable without a default is an error rather than a silent empty string.
func expandEnv(b []byte) ([]byte, error) {
	var missing []string
	out := envRef.ReplaceAllFunc(b, func(m []byte) []byte {
		sub := envRef.FindSubmatch(m)
		name, def := string(sub[1]), sub[2]
		if v, ok := os.LookupEnv(name); ok {
			return []byte(v)
		}
		if strings.Contains(string(m), ":-") {
			return def
		}
		missing = append(missing, name)
		return m
	})
	if len(missing) > 0 {
		return nil, fmt.Errorf("environment variable(s) not set: %s", strings.Join(missing, ", "))
	}
	return out, nil
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
			ch.MaxSegmentSize = 64 << 20 // 64MiB
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
		if g.FileRollMaxBytes == 0 {
			g.FileRollMaxBytes = 128 << 20
		}
		if g.FileRollInterval == 0 {
			g.FileRollInterval = 1 * time.Minute
		}
	}
}

func (c *Config) validate() error {
	for _, ch := range c.Channels {
		if ch.WALShards < 0 {
			return fmt.Errorf("channel %q: wal_shards must be >= 0", ch.Name)
		}
		if ch.WALShards > 1 && ch.Type != "file" && ch.Type != "spillable" {
			return fmt.Errorf("channel %q: wal_shards only applies to file and spillable channels", ch.Name)
		}
	}
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
		if s.AuthTokensFile != "" && s.AuthToken != "" {
			return fmt.Errorf("source %q: auth_token and auth_tokens_file are mutually exclusive", s.Name)
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
		for _, t := range g.Targets {
			if t == "" {
				return fmt.Errorf("sink group %q has an empty target", g.Name)
			}
			if strings.HasPrefix(t, "file:") && strings.TrimPrefix(t, "file:") == "" {
				return fmt.Errorf("sink group %q: file target %q has no directory", g.Name, t)
			}
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
