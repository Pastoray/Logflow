package agent

import (
	"github.com/prometheus/client_golang/prometheus"

	"logflow/internal/channel"
)

// channelStatsCollector adapts channel.Channel.Stats() - already computed
// for the /stats HTTP handler - into Prometheus gauges/counters, labeled by
// channel name so one agent with several channels (e.g. the multiplexing
// demo's "prod"/"default" split) still gets per-channel series rather than
// one blended number.
type channelStatsCollector struct {
	channels map[string]channel.Channel

	size     *prometheus.Desc
	capacity *prometheus.Desc
	put      *prometheus.Desc
	take     *prometheus.Desc
	dropped  *prometheus.Desc
	spilled  *prometheus.Desc
	purged   *prometheus.Desc

	clusterIsLeader     *prometheus.Desc
	clusterTerm         *prometheus.Desc
	clusterLeaderChange *prometheus.Desc

	logEntries    *prometheus.Desc
	snapshotIndex *prometheus.Desc

	applyStuck         *prometheus.Desc
	applyStuckSeconds  *prometheus.Desc
	applyStallWarnings *prometheus.Desc
}

func newChannelStatsCollector(channels map[string]channel.Channel) *channelStatsCollector {
	labels := []string{"channel"}
	return &channelStatsCollector{
		channels: channels,
		size:     prometheus.NewDesc("logflow_channel_size", "Current number of buffered events in the channel.", labels, nil),
		capacity: prometheus.NewDesc("logflow_channel_capacity", "Configured channel capacity.", labels, nil),
		put:      prometheus.NewDesc("logflow_channel_put_total", "Total events successfully put into the channel.", labels, nil),
		take:     prometheus.NewDesc("logflow_channel_take_total", "Total events successfully taken from the channel.", labels, nil),
		dropped:  prometheus.NewDesc("logflow_channel_dropped_total", "Total events dropped by the channel (e.g. full on Put).", labels, nil),
		spilled:  prometheus.NewDesc("logflow_channel_spilled_total", "Total events spilled from primary to overflow (spillable channels only).", labels, nil),
		purged:   prometheus.NewDesc("logflow_channel_purged_unconsumed_total", "Total still-unconsumed events discarded by WAL retention (file/spillable channels only).", labels, nil),

		clusterIsLeader:     prometheus.NewDesc("logflow_cluster_is_leader", "1 if this node is the current Raft leader for the channel, 0 otherwise (clustered channels only).", labels, nil),
		clusterTerm:         prometheus.NewDesc("logflow_cluster_term", "Current Raft term as seen by this node (clustered channels only).", labels, nil),
		clusterLeaderChange: prometheus.NewDesc("logflow_cluster_leader_changes_total", "Number of times this node has become Raft leader for the channel (clustered channels only).", labels, nil),

		logEntries:    prometheus.NewDesc("logflow_cluster_raft_log_entries", "Raft log entries currently retained (after compaction; clustered channels only).", labels, nil),
		snapshotIndex: prometheus.NewDesc("logflow_cluster_raft_snapshot_index", "Highest Raft log index discarded by compaction; 0 if never compacted (clustered channels only).", labels, nil),

		applyStuck:         prometheus.NewDesc("logflow_cluster_apply_stuck", "1 if this node's Raft applier is currently blocked writing a committed entry to the local channel, 0 otherwise (clustered channels only).", labels, nil),
		applyStuckSeconds:  prometheus.NewDesc("logflow_cluster_apply_stuck_seconds", "How long the current apply stall (if any) has been going on, in seconds; 0 when not stuck (clustered channels only).", labels, nil),
		applyStallWarnings: prometheus.NewDesc("logflow_cluster_apply_stall_warnings_total", "Cumulative count of apply-stall warnings logged for this channel (clustered channels only).", labels, nil),
	}
}

func (c *channelStatsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.size
	ch <- c.capacity
	ch <- c.put
	ch <- c.take
	ch <- c.dropped
	ch <- c.spilled
	ch <- c.purged
	ch <- c.clusterIsLeader
	ch <- c.clusterTerm
	ch <- c.clusterLeaderChange
	ch <- c.logEntries
	ch <- c.snapshotIndex
	ch <- c.applyStuck
	ch <- c.applyStuckSeconds
	ch <- c.applyStallWarnings
}

func (c *channelStatsCollector) Collect(ch chan<- prometheus.Metric) {
	for name, ci := range c.channels {
		st := ci.Stats()
		ch <- prometheus.MustNewConstMetric(c.size, prometheus.GaugeValue, float64(st.Size), name)
		ch <- prometheus.MustNewConstMetric(c.capacity, prometheus.GaugeValue, float64(st.Capacity), name)
		ch <- prometheus.MustNewConstMetric(c.put, prometheus.CounterValue, float64(st.PutCount), name)
		ch <- prometheus.MustNewConstMetric(c.take, prometheus.CounterValue, float64(st.TakeCount), name)
		ch <- prometheus.MustNewConstMetric(c.dropped, prometheus.CounterValue, float64(st.DroppedCount), name)
		if sp, ok := ci.(interface{ Spilled() int64 }); ok {
			ch <- prometheus.MustNewConstMetric(c.spilled, prometheus.CounterValue, float64(sp.Spilled()), name)
		}
		if pu, ok := ci.(interface{ PurgedUnconsumed() int64 }); ok {
			ch <- prometheus.MustNewConstMetric(c.purged, prometheus.CounterValue, float64(pu.PurgedUnconsumed()), name)
		}
		if cs, ok := ci.(interface {
			ClusterState() string
			ClusterTerm() uint64
			ClusterLeaderChanges() int64
		}); ok {
			isLeader := 0.0
			if cs.ClusterState() == "leader" {
				isLeader = 1.0
			}
			ch <- prometheus.MustNewConstMetric(c.clusterIsLeader, prometheus.GaugeValue, isLeader, name)
			ch <- prometheus.MustNewConstMetric(c.clusterTerm, prometheus.GaugeValue, float64(cs.ClusterTerm()), name)
			ch <- prometheus.MustNewConstMetric(c.clusterLeaderChange, prometheus.CounterValue, float64(cs.ClusterLeaderChanges()), name)
		}
		if lg, ok := ci.(interface {
			ClusterLogEntries() int
			ClusterSnapshotIndex() uint64
		}); ok {
			ch <- prometheus.MustNewConstMetric(c.logEntries, prometheus.GaugeValue, float64(lg.ClusterLogEntries()), name)
			ch <- prometheus.MustNewConstMetric(c.snapshotIndex, prometheus.GaugeValue, float64(lg.ClusterSnapshotIndex()), name)
		}
		if aw, ok := ci.(interface {
			ApplyStuck() bool
			ApplyStuckSeconds() float64
			ApplyStallWarnings() int64
		}); ok {
			stuck := 0.0
			if aw.ApplyStuck() {
				stuck = 1.0
			}
			ch <- prometheus.MustNewConstMetric(c.applyStuck, prometheus.GaugeValue, stuck, name)
			ch <- prometheus.MustNewConstMetric(c.applyStuckSeconds, prometheus.GaugeValue, aw.ApplyStuckSeconds(), name)
			ch <- prometheus.MustNewConstMetric(c.applyStallWarnings, prometheus.CounterValue, float64(aw.ApplyStallWarnings()), name)
		}
	}
}
