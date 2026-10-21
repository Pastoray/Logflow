// Package cluster implements a deliberately-scoped-down Raft consensus
// module (leader election + log replication, Raft's core: see "In Search
// of an Understandable Consensus Algorithm" by Ongaro & Ousterhout) used to
// replicate a collector's channel across multiple agent processes.
//
// This is a hand-rolled implementation, not a wrapped third-party library:
// the rest of logflow already hand-builds its own channels, retry/backoff,
// load-balancing and failover rather than pulling in a message-queue
// client, and the same reasoning applies here - the core Raft algorithm is
// well-specified and modest in size, and a dependency-free implementation
// stays consistent with how vendoring already works in this project (see
// the main README's Building section) without chasing a third-party
// library's own transitive dependency tree.
//
// Cluster membership can change while the group is live (see "Membership
// changes" below) via the standard two-phase joint-consensus protocol
// (Ongaro & Ousterhout §6), rather than requiring every node to be
// reconfigured and restarted together.
//
// Log compaction is opt-in (Config.SnapshotThreshold, see "Log
// compaction" in the project README): applied entries are discarded and
// replaced by a boundary (last index, term, membership); a node that needs
// entries already discarded is sent InstallSnapshot. The snapshot carries
// no event data - this state machine appends to a local channel whose
// contents can't be exported - so a node caught up that way lacks the
// events in the skipped range. With compaction off the log grows without
// bound and a new member replays all of it.
//
// Still out of scope: only one membership change may be in flight at a
// time (the standard Raft restriction - see ErrMembershipChangeInProgress).
//
// # Membership changes
//
// Config.Peers is only the cluster's *bootstrap* membership - what a
// brand-new group starts from, or what an existing group rejoins with
// after every node restarts. From then on, membership lives in the Raft
// log itself, the same place event data does: a configuration change is
// its own kind of log entry (LogEntry.Type == EntryConfig), replicated
// and committed exactly like any other entry, and a node always uses
// whatever configuration is most recent in its *own* log - even before
// that entry is committed - rather than anything coming from Config.Peers
// once the log is non-empty. AddServer and RemoveServer drive a change
// through Raft's two-phase protocol:
//
//  1. Cold,new: the leader proposes a joint configuration naming both the
//     old member set and the new one. From the moment this entry is
//     merely *appended* (not yet committed) to a node's log, that node
//     requires separate majorities of Cold AND Cnew to agree on anything -
//     elections, commits - which is what rules out the unsafe window two
//     disjoint single-majority views could otherwise open up mid-change.
//  2. Cnew: once Cold,new commits, the leader proposes the new
//     configuration alone. Once that commits, the change is final - Cold
//     is irrelevant from that point on. A leader that removed itself from
//     Cnew steps down once this entry commits (Raft §6.2), rather than
//     continuing to manage a cluster it's no longer a member of.
//
// Adding a server also runs a catch-up phase first: the leader replicates
// its full log directly to the new node (which does not yet count toward
// any quorum) until the new node is caught up, *then* proposes Cold,new -
// so a brand-new, empty-log member can't stall quorum for everyone else
// the moment it's added. A node meant to join an existing cluster this
// way (as opposed to bootstrapping a new one) is started with
// Config.Joining = true and Config.Self set instead of Config.Peers - it
// serves RPCs and can be caught up and voted for, but deliberately never
// originates an election itself (so it can't disrupt a live leader before
// it's actually a member) until it observes a real configuration entry in
// its own log.
package cluster

import (
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var ErrNotLeader = errors.New("cluster: not leader")
var ErrNoLeader = errors.New("cluster: no leader known")
var ErrApplyTimeout = errors.New("cluster: apply timed out waiting for commit")
var ErrShutdown = errors.New("cluster: node is shut down")
var ErrMembershipChangeInProgress = errors.New("cluster: a membership change is already in progress")

type raftState int32

const (
	Follower raftState = iota
	Candidate
	Leader
)

func (s raftState) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// EntryType distinguishes an ordinary application-data log entry from a
// cluster-membership configuration entry. Both travel through exactly the
// same replication/commit machinery - a config entry just isn't handed to
// ApplyFn, and updates Node's view of the cluster's membership as soon as
// it's appended (see recomputeMembershipLocked).
type EntryType uint8

const (
	EntryData EntryType = iota
	EntryConfig
	// EntryNoop is appended by every new leader (Raft §8) so that entries
	// from earlier terms, which may only be committed indirectly by
	// committing an entry of the leader's own term, become committed
	// promptly instead of waiting for the next client write. Never applied.
	EntryNoop
)

// LogEntry is one proposed, (once committed) replicated unit of work. Index
// is 1-based; index 0 is reserved as the "nothing committed yet" sentinel.
type LogEntry struct {
	Term   uint64
	Index  uint64
	Type   EntryType
	Data   []byte     // EntryData payload; unused when Type == EntryConfig
	Config Membership // EntryConfig payload; unused when Type == EntryData
}

type Peer struct {
	ID         string
	Addr       string // Raft protocol RPC address (RequestVote/AppendEntries)
	ClientAddr string // client-forwarding RPC address (ClientApply)
}

// MembershipPhase is Raft's joint-consensus state machine for a
// configuration change in progress: PhaseStable (the common case, one
// agreed-upon member set) or PhaseJoint (a change is in flight - every
// decision needs separate majorities of both the old and new member
// sets). See this package's doc comment, "Membership changes".
type MembershipPhase uint8

const (
	PhaseStable MembershipPhase = iota
	PhaseJoint
)

// Membership is the cluster configuration named by the most recent
// EntryConfig entry in a node's own log (or, before any such entry
// exists, Config.Peers verbatim). Old is only meaningful - and only
// populated - during PhaseJoint; New is always the current (or
// in-progress target) member set.
type Membership struct {
	Phase MembershipPhase
	Old   []Peer
	New   []Peer
}

type Config struct {
	NodeID string
	Peers  []Peer // bootstrap membership, including self; ignored (may be empty) when Joining is true

	// Joining starts this node so it can be added to an already-running
	// cluster live, rather than bootstrapping (or rejoining) one from
	// Peers - see this package's doc comment. Self must be set instead of
	// Peers; NodeID is taken from Self.ID.
	Joining bool
	Self    Peer

	DataDir string // persists term/vote/log across restarts; "" disables persistence

	// ListenAddr/ClientListenAddr override the addresses this node BINDS
	// (default: its own Peer.Addr / Peer.ClientAddr). Peer addresses are
	// what OTHER nodes dial, so in a container or pod they are DNS names
	// that need not resolve to a bindable local address yet - bind
	// ":8301" and advertise "pod-0.svc:8301".
	ListenAddr       string
	ClientListenAddr string

	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration

	// ApplyFn is invoked, in log order, once per committed EntryData
	// entry - the state machine this Raft group is replicating. Called
	// from the node's own applier goroutine; must not call back into the
	// Node. Not called for EntryConfig entries.
	ApplyFn func(data []byte)

	// SnapshotThreshold enables log compaction: once this many applied
	// entries have accumulated beyond the last snapshot, the applied
	// prefix of the Raft log is discarded (keeping SnapshotTrailing
	// entries so a follower that is only slightly behind still catches up
	// from the log). 0 disables compaction (the log grows without bound,
	// the original behaviour). See "Log compaction" in the package doc.
	SnapshotThreshold int
	// SnapshotTrailing is how many applied entries to keep un-compacted;
	// default SnapshotThreshold/4.
	SnapshotTrailing int

	// LinkFilter, if set, is consulted before every outbound Raft RPC with
	// (this node's ID, destination address); returning false drops the call
	// as if the network were partitioned. Test-only fault injection; nil
	// in production.
	LinkFilter func(fromID, toAddr string) bool

	Logger *log.Logger
}

func (c *Config) setDefaults() {
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 150 * time.Millisecond
	}
	if c.ElectionTimeoutMax == 0 {
		c.ElectionTimeoutMax = 300 * time.Millisecond
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	if c.Logger == nil {
		c.Logger = log.Default()
	}
}

// Node is one member of a Raft group.
type Node struct {
	cfg  Config
	self Peer

	mu          sync.Mutex
	currentTerm uint64
	votedFor    string
	raftLog     []LogEntry // raftLog[i] has Index == snapIndex+i+1

	// Compaction state: every entry with Index <= snapIndex has been
	// discarded; snapTerm is the term of entry snapIndex and snapMembership
	// the cluster configuration in effect at it. All zero until the first
	// compaction (or InstallSnapshot).
	snapIndex      uint64
	snapTerm       uint64
	snapMembership Membership
	commitIndex    uint64
	lastApplied    uint64
	state          raftState
	leaderID       string

	// membership is always the Config of the most recent EntryConfig entry
	// in raftLog (committed or not - see this package's doc comment), or
	// Config.Peers verbatim when raftLog has no config entry yet. Kept in
	// sync by recomputeMembershipLocked, called after every raftLog
	// mutation. joining mirrors Config.Joining but is cleared permanently
	// the first time this node observes any config entry in its own log -
	// see electionLoop.
	membership               Membership
	joining                  bool
	membershipChangeInFlight bool

	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	resetElection chan struct{}
	applySignal   chan struct{}
	closed        chan struct{}
	closeOnce     sync.Once

	listener       net.Listener
	clientListener net.Listener
	server         *rpc.Server

	connsMu sync.Mutex
	conns   []net.Conn // every accepted connection on either listener, tracked so Close() can kill them outright

	clientMu sync.Mutex
	clients  map[string]*rpc.Client // lazily-dialed, address -> client

	leaderChanges int64
}

// NewNode starts a Raft node: an RPC listener on its own Addr, the
// election-timeout loop, and the applier goroutine. It returns once the
// listener is up; cluster membership (who's leader) is settled
// asynchronously, same as any real Raft deployment's cold start.
func NewNode(cfg Config) (*Node, error) {
	cfg.setDefaults()
	var self Peer
	if cfg.Joining {
		if cfg.Self.ID == "" || cfg.Self.Addr == "" {
			return nil, errors.New("cluster: Self (ID and Addr) is required when Joining is true")
		}
		self = cfg.Self
		cfg.NodeID = cfg.Self.ID
		// Seed Peers to just self: recomputeMembershipLocked's "log is
		// empty" fallback becomes a harmless 1-node view that this node
		// will never act on, since a joining node never originates
		// elections (see electionLoop) until it observes a real
		// multi-member config entry replicated to it by the cluster it's
		// joining.
		cfg.Peers = []Peer{self}
	} else {
		found := false
		for _, p := range cfg.Peers {
			if p.ID == cfg.NodeID {
				self = p
				found = true
			}
		}
		if !found {
			return nil, fmt.Errorf("cluster: node_id %q not found in peers", cfg.NodeID)
		}
	}
	if cfg.ApplyFn == nil {
		return nil, errors.New("cluster: ApplyFn is required")
	}

	n := &Node{
		cfg:           cfg,
		self:          self,
		state:         Follower,
		joining:       cfg.Joining,
		nextIndex:     make(map[string]uint64),
		matchIndex:    make(map[string]uint64),
		resetElection: make(chan struct{}, 1),
		applySignal:   make(chan struct{}, 1),
		closed:        make(chan struct{}),
		clients:       make(map[string]*rpc.Client),
	}

	if err := n.loadState(); err != nil {
		return nil, fmt.Errorf("cluster: loading persisted state: %w", err)
	}
	n.mu.Lock()
	n.recomputeMembershipLocked()
	n.mu.Unlock()

	srv := rpc.NewServer()
	if err := srv.RegisterName("Raft", (*raftRPC)(n)); err != nil {
		return nil, err
	}
	bind := self.Addr
	if cfg.ListenAddr != "" {
		bind = cfg.ListenAddr
	}
	lis, err := net.Listen("tcp", bind)
	if err != nil {
		return nil, fmt.Errorf("cluster: listening on %s: %w", bind, err)
	}
	n.listener = lis
	n.server = srv
	go n.serveTracked(srv, lis)

	if self.ClientAddr != "" {
		clientSrv := rpc.NewServer()
		if err := clientSrv.RegisterName("Raft", (*clientRPC)(n)); err != nil {
			return nil, err
		}
		cbind := self.ClientAddr
		if cfg.ClientListenAddr != "" {
			cbind = cfg.ClientListenAddr
		}
		clis, err := net.Listen("tcp", cbind)
		if err != nil {
			return nil, fmt.Errorf("cluster: listening on client addr %s: %w", cbind, err)
		}
		n.clientListener = clis
		go n.serveTracked(clientSrv, clis)
	}

	go n.electionLoop()
	go n.applyLoop()

	n.mu.Lock()
	memberCount := len(n.votingPeersLocked())
	joining := n.joining
	n.mu.Unlock()
	if joining {
		n.cfg.Logger.Printf("[cluster:%s] node started (joining), raft=%s", n.cfg.NodeID, self.Addr)
	} else {
		n.cfg.Logger.Printf("[cluster:%s] node started, %d member(s), raft=%s", n.cfg.NodeID, memberCount, self.Addr)
	}
	return n, nil
}

// recomputeMembershipLocked refreshes n.membership from the most recent
// EntryConfig entry in raftLog (or Config.Peers, if none exists yet) and
// clears n.joining the first time any config entry is observed. Must be
// called with n.mu held, after every mutation of raftLog: initial load,
// Apply-style append of a new config entry, and AppendEntries' own
// truncate/append (a truncation can just as easily revert to an older
// configuration as an append can advance to a newer one).
func (n *Node) recomputeMembershipLocked() {
	for i := len(n.raftLog) - 1; i >= 0; i-- {
		if n.raftLog[i].Type == EntryConfig {
			n.membership = n.raftLog[i].Config
			n.joining = false
			return
		}
	}
	if n.snapIndex > 0 {
		n.membership = n.snapMembership
		n.joining = false
		return
	}
	n.membership = Membership{Phase: PhaseStable, New: n.cfg.Peers}
}

// --- log index helpers: raftLog only holds entries after snapIndex ---

func (n *Node) lastIndexLocked() uint64 { return n.snapIndex + uint64(len(n.raftLog)) }

// termAtLocked returns the term of the entry at idx, which may be the
// snapshot boundary itself; ok=false if idx is compacted away or beyond
// the log.
func (n *Node) termAtLocked(idx uint64) (uint64, bool) {
	switch {
	case idx == 0:
		return 0, true
	case idx == n.snapIndex:
		return n.snapTerm, true
	case idx < n.snapIndex || idx > n.lastIndexLocked():
		return 0, false
	}
	return n.raftLog[idx-n.snapIndex-1].Term, true
}

// entryAtLocked requires snapIndex < idx <= lastIndexLocked().
func (n *Node) entryAtLocked(idx uint64) LogEntry { return n.raftLog[idx-n.snapIndex-1] }

// truncateFromLocked drops entry idx and everything after it
// (idx > snapIndex).
func (n *Node) truncateFromLocked(idx uint64) { n.raftLog = n.raftLog[:idx-n.snapIndex-1] }

// maybeCompactLocked discards the applied prefix of the log once enough
// has accumulated (Config.SnapshotThreshold). Only ever compacts entries
// that are already applied, hence committed.
func (n *Node) maybeCompactLocked() {
	th := uint64(n.cfg.SnapshotThreshold)
	if th == 0 || n.lastApplied < n.snapIndex+th {
		return
	}
	trailing := uint64(n.cfg.SnapshotTrailing)
	if n.cfg.SnapshotTrailing <= 0 {
		trailing = th / 4
	}
	if n.lastApplied <= trailing {
		return
	}
	upTo := n.lastApplied - trailing
	if upTo <= n.snapIndex {
		return
	}
	term, ok := n.termAtLocked(upTo)
	if !ok {
		return
	}
	// Configuration in effect at upTo: the last config entry at or before
	// it, else whatever the previous snapshot (or bootstrap peers) said.
	mem := n.snapMembership
	if n.snapIndex == 0 {
		mem = Membership{Phase: PhaseStable, New: n.cfg.Peers}
	}
	for i := n.snapIndex + 1; i <= upTo; i++ {
		if e := n.entryAtLocked(i); e.Type == EntryConfig {
			mem = e.Config
		}
	}
	keep := n.raftLog[upTo-n.snapIndex:]
	n.raftLog = append([]LogEntry(nil), keep...) // copy so the old backing array can be freed
	n.snapIndex, n.snapTerm, n.snapMembership = upTo, term, mem
	n.recomputeMembershipLocked()
	n.persistLocked()
	n.cfg.Logger.Printf("[cluster:%s] compacted log through index %d (term %d); %d entries retained", n.cfg.NodeID, upTo, term, len(n.raftLog))
}

// SnapshotIndex is the highest log index discarded by compaction.
func (n *Node) SnapshotIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.snapIndex
}

// LogEntries is how many entries the Raft log currently holds in memory
// and on disk (i.e. after compaction).
func (n *Node) LogEntries() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.raftLog)
}

// votingPeersLocked returns every peer counted toward quorum right now:
// Cnew, plus Cold too during PhaseJoint (deduplicated by ID - a peer
// present in both counts once). This is the set replication, election,
// and heartbeats all fan out to; hasMajorityLocked is the set commit/vote
// decisions are judged against.
func (n *Node) votingPeersLocked() []Peer {
	seen := make(map[string]bool, len(n.membership.New)+len(n.membership.Old))
	out := make([]Peer, 0, len(n.membership.New)+len(n.membership.Old))
	add := func(ps []Peer) {
		for _, p := range ps {
			if !seen[p.ID] {
				seen[p.ID] = true
				out = append(out, p)
			}
		}
	}
	add(n.membership.New)
	if n.membership.Phase == PhaseJoint {
		add(n.membership.Old)
	}
	return out
}

// hasMajorityLocked reports whether yes(id) holds for a majority of every
// config group active in the current membership - just Cnew in the common
// (PhaseStable) case, or separate majorities of BOTH Cold and Cnew during
// PhaseJoint. This single predicate is reused for both vote-counting
// (startElection) and commit-advancement (maybeAdvanceCommit), which is
// exactly what keeps those two call sites from needing to know about
// joint consensus at all beyond calling this.
func (n *Node) hasMajorityLocked(yes func(id string) bool) bool {
	if !quorum(n.membership.New, yes) {
		return false
	}
	if n.membership.Phase == PhaseJoint && !quorum(n.membership.Old, yes) {
		return false
	}
	return true
}

func quorum(group []Peer, yes func(id string) bool) bool {
	if len(group) == 0 {
		return true
	}
	count := 0
	for _, p := range group {
		if yes(p.ID) {
			count++
		}
	}
	return count >= len(group)/2+1
}

func appendOrReplacePeer(peers []Peer, p Peer) []Peer {
	out := make([]Peer, 0, len(peers)+1)
	replaced := false
	for _, existing := range peers {
		if existing.ID == p.ID {
			out = append(out, p)
			replaced = true
		} else {
			out = append(out, existing)
		}
	}
	if !replaced {
		out = append(out, p)
	}
	return out
}

func removePeerByID(peers []Peer, id string) []Peer {
	out := make([]Peer, 0, len(peers))
	for _, p := range peers {
		if p.ID != id {
			out = append(out, p)
		}
	}
	return out
}

func containsID(peers []Peer, id string) bool {
	for _, p := range peers {
		if p.ID == id {
			return true
		}
	}
	return false
}

func (n *Node) randomElectionTimeout() time.Duration {
	lo, hi := n.cfg.ElectionTimeoutMin, n.cfg.ElectionTimeoutMax
	if hi <= lo {
		return lo
	}
	return lo + time.Duration(rand.Int63n(int64(hi-lo)))
}

func (n *Node) electionLoop() {
	timer := time.NewTimer(n.randomElectionTimeout())
	defer timer.Stop()
	for {
		select {
		case <-n.closed:
			return
		case <-n.resetElection:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(n.randomElectionTimeout())
		case <-timer.C:
			n.mu.Lock()
			isLeader := n.state == Leader
			joining := n.joining
			// departed mirrors joining at the other end of a node's
			// lifecycle: once a node's OWN log shows a Stable config that
			// no longer includes it (it was RemoveServer'd - see
			// changeMembership's courtesy push, which is what gets this
			// node that final entry even though it's no longer being
			// regularly replicated to), it stops calling elections too.
			// Without this, a removed-but-still-running node's timer
			// would fire forever (nothing replicates to it anymore, so
			// it never sees a heartbeat) - each one now a no-op (see
			// RequestVote's membership check), but an endless, noisy,
			// ever-climbing-term no-op rather than a clean stop.
			departed := n.membership.Phase == PhaseStable && !containsID(n.membership.New, n.cfg.NodeID)
			n.mu.Unlock()
			// A joining node (see Config.Joining and this package's doc
			// comment) never originates an election on its own, even once
			// its own election timeout fires from receiving no
			// heartbeats - it isn't a real member yet, so "electing
			// itself" would at best be meaningless and at worst (once it
			// starts actually being replicated to, before officially
			// joining) risk disrupting a live leader with a pointless
			// higher-term RequestVote. It still answers RequestVote/
			// AppendEntries normally; it simply never calls startElection.
			if !isLeader && !joining && !departed {
				n.startElection()
			}
			timer.Reset(n.randomElectionTimeout())
		}
	}
}

func (n *Node) notifyReset() {
	select {
	case n.resetElection <- struct{}{}:
	default:
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.currentTerm++
	term := n.currentTerm
	n.votedFor = n.cfg.NodeID
	n.state = Candidate
	lastIdx, lastTerm := n.lastLogIndexTermLocked()
	peers := n.votingPeersLocked()
	n.persistLocked()
	n.mu.Unlock()

	n.cfg.Logger.Printf("[cluster:%s] starting election for term %d", n.cfg.NodeID, term)

	granted := map[string]bool{n.cfg.NodeID: true} // vote for self
	var voteMu sync.Mutex
	var wg sync.WaitGroup

	for _, p := range peers {
		if p.ID == n.cfg.NodeID {
			continue
		}
		wg.Add(1)
		go func(p Peer) {
			defer wg.Done()
			args := &RequestVoteArgs{
				Term: term, CandidateID: n.cfg.NodeID,
				LastLogIndex: lastIdx, LastLogTerm: lastTerm,
			}
			var reply RequestVoteReply
			if err := n.call(p.Addr, "Raft.RequestVote", args, &reply); err != nil {
				return
			}
			n.mu.Lock()
			if reply.Term > n.currentTerm {
				n.stepDownLocked(reply.Term)
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()
			if reply.VoteGranted {
				voteMu.Lock()
				granted[p.ID] = true
				voteMu.Unlock()
			}
		}(p)
	}
	wg.Wait()

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Candidate || n.currentTerm != term {
		return // state changed (stepped down, or already elected/moved on) while votes were in flight
	}
	voteMu.Lock()
	won := n.hasMajorityLocked(func(id string) bool { return granted[id] })
	voteMu.Unlock()
	if won {
		n.becomeLeaderLocked()
	}
}

func (n *Node) becomeLeaderLocked() {
	n.state = Leader
	n.leaderID = n.cfg.NodeID
	n.leaderChanges++
	lastIdx := n.lastIndexLocked()
	for _, p := range n.votingPeersLocked() {
		n.nextIndex[p.ID] = lastIdx + 1
		n.matchIndex[p.ID] = 0
	}
	n.raftLog = append(n.raftLog, LogEntry{Term: n.currentTerm, Index: n.lastIndexLocked() + 1, Type: EntryNoop})
	n.persistLocked()
	n.cfg.Logger.Printf("[cluster:%s] elected leader for term %d", n.cfg.NodeID, n.currentTerm)
	go n.leaderLoop(n.currentTerm)
}

// stepDownLocked must be called with n.mu held.
func (n *Node) stepDownLocked(term uint64) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
		n.persistLocked()
	}
	if n.state != Follower {
		n.cfg.Logger.Printf("[cluster:%s] stepping down to follower (term %d)", n.cfg.NodeID, term)
	}
	n.state = Follower
}

// leaderLoop sends periodic AppendEntries (heartbeats, or real entries) to
// every peer for as long as this node remains leader of the given term.
func (n *Node) leaderLoop(term uint64) {
	ticker := time.NewTicker(n.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		n.mu.Lock()
		stillLeader := n.state == Leader && n.currentTerm == term
		peers := n.votingPeersLocked()
		n.mu.Unlock()
		if !stillLeader {
			return
		}
		var wg sync.WaitGroup
		for _, p := range peers {
			if p.ID == n.cfg.NodeID {
				continue
			}
			wg.Add(1)
			go func(p Peer) { defer wg.Done(); n.replicateTo(p, term) }(p)
		}
		wg.Wait()
		n.maybeAdvanceCommit(term)
		select {
		case <-n.closed:
			return
		case <-ticker.C:
		}
	}
}

// replicateTo sends one AppendEntries RPC to p, carrying whatever entries
// p is missing according to our view of nextIndex[p.ID], and backs
// nextIndex off by one on a log-inconsistency rejection (the textbook,
// unoptimized Raft backtracking - simple and correct, if not the fastest
// way to catch up a very-far-behind follower).
func (n *Node) replicateTo(p Peer, term uint64) {
	n.mu.Lock()
	if n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	next := n.nextIndex[p.ID]
	if next == 0 {
		next = 1
	}
	if next <= n.snapIndex {
		// The entries p needs were compacted away: ship the snapshot
		// boundary instead, then resume normal replication after it.
		snap := &InstallSnapshotArgs{
			Term: term, LeaderID: n.cfg.NodeID,
			LastIncludedIndex: n.snapIndex, LastIncludedTerm: n.snapTerm,
			Membership: n.snapMembership,
		}
		n.mu.Unlock()
		n.sendSnapshot(p, term, snap)
		return
	}
	prevIdx := next - 1
	prevTerm, _ := n.termAtLocked(prevIdx)
	var entries []LogEntry
	if next <= n.lastIndexLocked() {
		entries = append(entries, n.raftLog[next-n.snapIndex-1:]...)
	}
	args := &AppendEntriesArgs{
		Term: term, LeaderID: n.cfg.NodeID,
		PrevLogIndex: prevIdx, PrevLogTerm: prevTerm,
		Entries: entries, LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	var reply AppendEntriesReply
	if err := n.call(p.Addr, "Raft.AppendEntries", args, &reply); err != nil {
		return // unreachable this round; nextIndex untouched, will retry next heartbeat
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if reply.Term > n.currentTerm {
		n.stepDownLocked(reply.Term)
		return
	}
	if n.state != Leader || n.currentTerm != term {
		return
	}
	if reply.Success {
		n.matchIndex[p.ID] = prevIdx + uint64(len(entries))
		n.nextIndex[p.ID] = n.matchIndex[p.ID] + 1
	} else if n.nextIndex[p.ID] > 1 {
		n.nextIndex[p.ID]--
	}
}

// sendSnapshot delivers an InstallSnapshot RPC and, on success, resumes
// normal replication right after the snapshot boundary.
func (n *Node) sendSnapshot(p Peer, term uint64, args *InstallSnapshotArgs) {
	var reply InstallSnapshotReply
	if err := n.call(p.Addr, "Raft.InstallSnapshot", args, &reply); err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if reply.Term > n.currentTerm {
		n.stepDownLocked(reply.Term)
		return
	}
	if n.state != Leader || n.currentTerm != term {
		return
	}
	if reply.Success && args.LastIncludedIndex > n.matchIndex[p.ID] {
		n.matchIndex[p.ID] = args.LastIncludedIndex
		n.nextIndex[p.ID] = args.LastIncludedIndex + 1
	}
}

// maybeAdvanceCommit implements Raft's commitment rule: advance
// commitIndex to the highest N replicated on a majority of nodes, but only
// if entry N was proposed in the leader's *current* term (the safety
// condition that keeps an old leader's uncommitted entries from being
// silently committed by a later leader's replication of a since-overwritten
// position - see Raft §5.4.2).
func (n *Node) maybeAdvanceCommit(term uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Leader || n.currentTerm != term {
		return
	}
	for N := n.lastIndexLocked(); N > n.commitIndex; N-- {
		if t, _ := n.termAtLocked(N); t != n.currentTerm {
			continue
		}
		yes := func(id string) bool {
			return id == n.cfg.NodeID || n.matchIndex[id] >= N
		}
		if n.hasMajorityLocked(yes) {
			n.commitIndex = N
			n.signalApply()
			break
		}
	}
}

func (n *Node) signalApply() {
	select {
	case n.applySignal <- struct{}{}:
	default:
	}
}

func (n *Node) applyLoop() {
	for {
		select {
		case <-n.closed:
			return
		case <-n.applySignal:
		case <-time.After(50 * time.Millisecond): // also poll, in case a signal was coalesced away
		}
		for {
			n.mu.Lock()
			if n.lastApplied >= n.commitIndex || n.lastApplied >= n.lastIndexLocked() {
				n.mu.Unlock()
				break
			}
			idx := n.lastApplied + 1
			entry := n.entryAtLocked(idx)
			n.mu.Unlock()
			// EntryConfig entries already took effect on the cluster's
			// membership the moment they were appended
			// (recomputeMembershipLocked runs on append, not on commit -
			// see this package's doc comment) - lastApplied still
			// advances past them here so Apply/changeMembership's "wait
			// until my entry's index has been applied" logic works
			// uniformly for both entry types, but ApplyFn (the event-data
			// state machine) is never called for one.
			if entry.Type == EntryData {
				n.cfg.ApplyFn(entry.Data)
			}
			// lastApplied only advances AFTER ApplyFn actually returns,
			// not before calling it - a real race an earlier version of
			// this loop had: it bumped lastApplied, then unlocked, then
			// called ApplyFn, which left a window where any other
			// goroutine polling lastApplied (replicateAndWaitCommitted,
			// which is what a client's Put()/Apply() call blocks on)
			// could observe "applied" and return success before ApplyFn
			// had actually finished materializing the entry into the
			// local channel - meaningfully wrong if ApplyFn was slow or
			// (see ReplicatedChannel.putLocalWatched) stuck: a caller
			// could be told its write succeeded before it was actually
			// durable in the local store. ApplyFn's own blocking (not a
			// timeout here) is what previously made the race hard to hit
			// in practice, but it was still a race, and fixing the
			// ordering costs nothing - the inner loop is already
			// sequential and single-goroutine.
			n.mu.Lock()
			// An InstallSnapshot may have jumped lastApplied past idx while
			// ApplyFn ran; never move it backwards.
			if idx > n.lastApplied {
				n.lastApplied = idx
			}
			n.maybeCompactLocked()
			n.mu.Unlock()
		}
	}
}

func (n *Node) lastLogIndexTermLocked() (uint64, uint64) {
	if len(n.raftLog) == 0 {
		return n.snapIndex, n.snapTerm
	}
	last := n.raftLog[len(n.raftLog)-1]
	return last.Index, last.Term
}

// Apply proposes data as a new log entry. Only the leader can do this
// directly; call Propose (below) if the caller doesn't know/care whether
// this node is the leader. Blocks until the entry is committed and applied
// on this node, or timeout elapses.
func (n *Node) Apply(data []byte, timeout time.Duration) error {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	entry := LogEntry{Term: n.currentTerm, Index: n.lastIndexLocked() + 1, Type: EntryData, Data: data}
	n.raftLog = append(n.raftLog, entry)
	n.persistLocked()
	term := n.currentTerm
	targetIdx := entry.Index
	n.mu.Unlock()

	return n.replicateAndWaitCommitted(targetIdx, term, timeout)
}

// replicateAndWaitCommitted is the common tail of Apply and
// changeMembership's two config-entry proposals: kick replication to
// every current voter immediately (rather than waiting for the next
// heartbeat tick, so a caller isn't held hostage to HeartbeatInterval),
// nudge commitIndex forward, then wait for targetIdx to actually be
// applied on this node (which implies committed) or timeout/step-down/
// shutdown, whichever comes first.
func (n *Node) replicateAndWaitCommitted(targetIdx, term uint64, timeout time.Duration) error {
	n.mu.Lock()
	peers := n.votingPeersLocked()
	n.mu.Unlock()

	var wg sync.WaitGroup
	for _, p := range peers {
		if p.ID == n.cfg.NodeID {
			continue
		}
		wg.Add(1)
		go func(p Peer) { defer wg.Done(); n.replicateTo(p, term) }(p)
	}
	wg.Wait()
	n.maybeAdvanceCommit(term)

	deadline := time.Now().Add(timeout)
	for {
		n.mu.Lock()
		applied := n.lastApplied >= targetIdx
		stillLeader := n.state == Leader && n.currentTerm == term
		// lastApplied >= targetIdx only means SOME entry at that index was
		// applied; a partitioned leader's slot may have been overwritten.
		// Only our own entry (same term) counts; if compacted away we can
		// only vouch for it while still leader of the same term.
		overwritten := false
		if applied {
			if t, ok := n.termAtLocked(targetIdx); ok {
				overwritten = t != term
			} else {
				overwritten = !stillLeader
			}
		}
		n.mu.Unlock()
		if applied && !overwritten {
			return nil
		}
		if overwritten {
			return ErrNotLeader
		}
		if !stillLeader {
			return ErrNotLeader
		}
		if time.Now().After(deadline) {
			return ErrApplyTimeout
		}
		select {
		case <-n.closed:
			return ErrShutdown
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// catchUpNewServer directly replicates this leader's full log to p -
// which does not yet count toward any quorum, so this can't itself stall
// anything - repeatedly until p's matchIndex reaches the leader's last
// log index (within one round-trip) or timeout elapses. Run before
// proposing a Cold,new entry that would add p as a voter, so a brand-new,
// empty-log member doesn't become part of quorum while still wildly
// behind (which could stall or even block progress if it's immediately
// needed for a majority).
func (n *Node) catchUpNewServer(p Peer, term uint64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		n.replicateTo(p, term)
		n.mu.Lock()
		lastIdx, _ := n.lastLogIndexTermLocked()
		match := n.matchIndex[p.ID]
		stillLeader := n.state == Leader && n.currentTerm == term
		n.mu.Unlock()
		if !stillLeader {
			return ErrNotLeader
		}
		if match >= lastIdx {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("new server %s did not catch up within %s (matched %d/%d)", p.ID, timeout, match, lastIdx)
		}
		select {
		case <-n.closed:
			return ErrShutdown
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// changeMembership drives Raft's two-phase joint-consensus protocol (see
// this package's doc comment) to move the cluster from its current
// configuration to newPeers: propose and commit Cold,new, then propose
// and commit Cnew alone. Must be called on the leader; refuses to start a
// second change while one is already in flight (the standard Raft
// restriction - concurrent changes could let two disjoint single-config
// majorities form at once, exactly what joint consensus exists to rule
// out).
//
// If the leader removes itself from newPeers, it steps down once Cnew
// commits (Raft §6.2) rather than continuing to manage a cluster it's no
// longer part of.
//
// If this node loses leadership partway through (e.g. after Cold,new
// commits but before Cnew does), the change is left in PhaseJoint - safe
// (joint quorum rules are already protecting it), but incomplete. Calling
// AddServer/RemoveServer again (on whichever node is now leader) with the
// same target membership will finish it: votingPeersLocked already unions
// Old and New, so the retried call's own Cold,new proposal is a (harmless,
// if slightly redundant) superset of the one still in flight.
func (n *Node) changeMembership(newPeers []Peer, timeout time.Duration) error {
	n.mu.Lock()
	if n.membershipChangeInFlight {
		n.mu.Unlock()
		return ErrMembershipChangeInProgress
	}
	n.membershipChangeInFlight = true
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		n.membershipChangeInFlight = false
		n.mu.Unlock()
	}()

	deadline := time.Now().Add(timeout)

	// Phase 1: Cold,new.
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	oldPeers := n.votingPeersLocked()
	term := n.currentTerm
	jointEntry := LogEntry{
		Term: term, Index: n.lastIndexLocked() + 1, Type: EntryConfig,
		Config: Membership{Phase: PhaseJoint, Old: oldPeers, New: newPeers},
	}
	n.raftLog = append(n.raftLog, jointEntry)
	n.recomputeMembershipLocked()
	n.persistLocked()
	n.mu.Unlock()
	n.cfg.Logger.Printf("[cluster:%s] membership: proposing joint config (old=%d new=%d)", n.cfg.NodeID, len(oldPeers), len(newPeers))

	if err := n.replicateAndWaitCommitted(jointEntry.Index, term, time.Until(deadline)); err != nil {
		return fmt.Errorf("cluster: joint config entry did not commit: %w", err)
	}

	// Phase 2: Cnew.
	n.mu.Lock()
	if n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return ErrNotLeader
	}
	stableEntry := LogEntry{
		Term: term, Index: n.lastIndexLocked() + 1, Type: EntryConfig,
		Config: Membership{Phase: PhaseStable, New: newPeers},
	}
	n.raftLog = append(n.raftLog, stableEntry)
	n.recomputeMembershipLocked()
	n.persistLocked()
	selfRemoved := !containsID(newPeers, n.cfg.NodeID)
	n.mu.Unlock()
	n.cfg.Logger.Printf("[cluster:%s] membership: proposing new stable config (%d member(s))", n.cfg.NodeID, len(newPeers))

	if err := n.replicateAndWaitCommitted(stableEntry.Index, term, time.Until(deadline)); err != nil {
		return fmt.Errorf("cluster: new config entry did not commit: %w", err)
	}

	// Courtesy push: any peer that was in oldPeers but isn't in newPeers
	// just stopped being a replication target (votingPeersLocked now
	// excludes it, so leaderLoop/replicateAndWaitCommitted won't contact
	// it again on their own) - but if it's still running, it hasn't
	// actually received the Cnew entry that excludes it, so its own log
	// still shows the Joint phase, and nothing has told it to stop
	// calling elections (see electionLoop's "departed" check). One
	// best-effort direct AppendEntries, outside the normal replication
	// set, gets it that final entry so it learns of its own removal and
	// quiets down - not required for correctness (RequestVote's
	// membership check already makes a removed node's elections inert
	// either way), but turns "harmless but runs forever" into "stops
	// cleanly."
	for _, p := range oldPeers {
		if p.ID == n.cfg.NodeID || containsID(newPeers, p.ID) {
			continue
		}
		go n.replicateTo(p, term)
	}

	if selfRemoved {
		n.mu.Lock()
		if n.state == Leader && n.currentTerm == term {
			n.cfg.Logger.Printf("[cluster:%s] stepping down: removed self from cluster membership", n.cfg.NodeID)
			n.state = Follower
			n.leaderID = ""
		}
		n.mu.Unlock()
	}
	return nil
}

// addVoter is AddServer's leader-only implementation: catch p up, then
// fold it into the membership via changeMembership. See AddServer.
func (n *Node) addVoter(p Peer, catchUpTimeout, commitTimeout time.Duration) error {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	term := n.currentTerm
	n.mu.Unlock()

	if err := n.catchUpNewServer(p, term, catchUpTimeout); err != nil {
		return fmt.Errorf("cluster: catching up new server %s: %w", p.ID, err)
	}

	n.mu.Lock()
	newPeers := appendOrReplacePeer(n.votingPeersLocked(), p)
	n.mu.Unlock()
	return n.changeMembership(newPeers, commitTimeout)
}

// removeVoter is RemoveServer's leader-only implementation. See
// RemoveServer.
func (n *Node) removeVoter(id string, commitTimeout time.Duration) error {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	newPeers := removePeerByID(n.votingPeersLocked(), id)
	n.mu.Unlock()
	return n.changeMembership(newPeers, commitTimeout)
}

// AddServer adds p as a new voting member of the cluster: caught up via
// direct replication, then committed into the membership via Raft's
// two-phase joint-consensus protocol - live, with no restart of any
// existing node. Callable on any node in the cluster, not just the
// leader; a non-leader forwards the request (up to 2 redirect hops, the
// same bounded pattern ReplicatedChannel.Put uses to forward a client
// write) to whichever node it currently believes is leader.
func (n *Node) AddServer(p Peer, timeout time.Duration) error {
	if n.IsLeader() {
		return n.addVoter(p, timeout, timeout)
	}
	return n.forwardMembershipChange(&ChangeMembershipArgs{AddPeer: &p})
}

// RemoveServer removes the member with the given ID from the cluster, the
// same live, no-restart way AddServer adds one. Also callable on any
// node, with the same forwarding behavior.
func (n *Node) RemoveServer(id string, timeout time.Duration) error {
	if n.IsLeader() {
		return n.removeVoter(id, timeout)
	}
	return n.forwardMembershipChange(&ChangeMembershipArgs{RemovePeer: id})
}

// forwardMembershipChange relays an AddServer/RemoveServer request to
// whichever node this one currently believes is leader - up to 2 hops,
// the same bounded redirect pattern channel.go's forwardToLeader uses for
// client writes (an admin might call AddServer/RemoveServer against any
// node, not just whichever one happens to be leader at the moment).
func (n *Node) forwardMembershipChange(args *ChangeMembershipArgs) error {
	peer, ok := n.LeaderPeer()
	if !ok {
		return ErrNoLeader
	}
	addr := peer.ClientAddr
	for hop := 0; hop < 2; hop++ {
		var reply ChangeMembershipReply
		if err := n.call(addr, "Raft.ChangeMembership", args, &reply); err != nil {
			return fmt.Errorf("cluster: forwarding membership change to %s: %w", addr, err)
		}
		if !reply.NotLeader {
			if reply.Err != "" {
				return fmt.Errorf("cluster: leader rejected membership change: %s", reply.Err)
			}
			return nil
		}
		if reply.NewLeaderAddr == "" {
			return ErrNoLeader
		}
		addr = reply.NewLeaderAddr
	}
	return fmt.Errorf("cluster: too many leader redirects for membership change, last tried %s", addr)
}

// MemberIDs returns the IDs of every peer currently counted toward
// quorum (Cnew, plus Cold too mid-change) - the same set votingPeersLocked
// computes internally, exported for introspection (see /stats).
func (n *Node) MemberIDs() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	peers := n.votingPeersLocked()
	ids := make([]string, len(peers))
	for i, p := range peers {
		ids[i] = p.ID
	}
	return ids
}

// MembershipPhase returns "stable" or "joint" (a membership change is
// currently in flight) - see this package's doc comment.
func (n *Node) MembershipPhase() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.membership.Phase == PhaseJoint {
		return "joint"
	}
	return "stable"
}

func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == Leader
}

func (n *Node) State() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state.String()
}

func (n *Node) Term() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

// LeaderPeer returns the Peer this node currently believes is leader, and
// whether it knows of one at all.
func (n *Node) LeaderPeer() (Peer, bool) {
	n.mu.Lock()
	id := n.leaderID
	peers := n.votingPeersLocked()
	n.mu.Unlock()
	if id == "" {
		return Peer{}, false
	}
	for _, p := range peers {
		if p.ID == id {
			return p, true
		}
	}
	return Peer{}, false
}

func (n *Node) LeaderChanges() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderChanges
}

// serveTracked is like rpc.Server.Accept, except every accepted connection
// is recorded so Close() can kill them outright. Without this, a "stopped"
// node's already-open connections would keep right on being served by
// net/rpc's per-connection goroutines - Accept() stopping only refuses
// *new* connections - so a leader's existing connection to a node we just
// pulled out of the cluster would silently go on getting heartbeats and
// log entries applied against it, a zombie node indistinguishable from a
// live one until that connection happened to break on its own.
func (n *Node) serveTracked(srv *rpc.Server, lis net.Listener) {
	for {
		conn, err := lis.Accept()
		if err != nil {
			return
		}
		n.connsMu.Lock()
		n.conns = append(n.conns, conn)
		n.connsMu.Unlock()
		go srv.ServeConn(conn)
	}
}

func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		close(n.closed)
		if n.listener != nil {
			n.listener.Close()
		}
		if n.clientListener != nil {
			n.clientListener.Close()
		}
		n.connsMu.Lock()
		for _, c := range n.conns {
			c.Close()
		}
		n.connsMu.Unlock()
		n.clientMu.Lock()
		for _, c := range n.clients {
			c.Close()
		}
		n.clientMu.Unlock()
	})
	return nil
}

// call dials (or reuses a cached connection to) addr and invokes an RPC,
// dropping the cached client on any error so the next call redials - cheap
// insurance against a stale connection to a peer that restarted.
func (n *Node) call(addr, method string, args, reply interface{}) error {
	if f := n.cfg.LinkFilter; f != nil && !f(n.cfg.NodeID, addr) {
		return errors.New("cluster: link partitioned (fault injection)")
	}
	n.clientMu.Lock()
	c, ok := n.clients[addr]
	n.clientMu.Unlock()
	if !ok {
		var err error
		c, err = rpc.Dial("tcp", addr)
		if err != nil {
			return err
		}
		n.clientMu.Lock()
		n.clients[addr] = c
		n.clientMu.Unlock()
	}
	err := c.Call(method, args, reply)
	if err != nil {
		n.clientMu.Lock()
		if n.clients[addr] == c {
			delete(n.clients, addr)
		}
		n.clientMu.Unlock()
		c.Close()
	}
	return err
}

// --- persistence: term/vote/log survive a process restart; the richer
// event data each entry carries is separately durable in whatever channel
// ApplyFn writes it into. ---

type persistedState struct {
	CurrentTerm uint64
	VotedFor    string
	Log         []LogEntry

	// Compaction boundary (zero in state files written before compaction
	// existed - gob ignores missing fields, so those still load).
	SnapIndex      uint64
	SnapTerm       uint64
	SnapMembership Membership
}

func (n *Node) stateFile() string {
	if n.cfg.DataDir == "" {
		return ""
	}
	return filepath.Join(n.cfg.DataDir, "raft-state.gob")
}

func (n *Node) loadState() error {
	path := n.stateFile()
	if path == "" {
		return nil
	}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	defer f.Close()
	var ps persistedState
	if err := gob.NewDecoder(f).Decode(&ps); err != nil {
		return fmt.Errorf("decoding %s: %w", path, err)
	}
	n.currentTerm = ps.CurrentTerm
	n.votedFor = ps.VotedFor
	n.raftLog = ps.Log
	n.snapIndex, n.snapTerm, n.snapMembership = ps.SnapIndex, ps.SnapTerm, ps.SnapMembership
	// Everything at or below the snapshot boundary was applied before it
	// was discarded.
	n.commitIndex, n.lastApplied = n.snapIndex, n.snapIndex
	n.cfg.Logger.Printf("[cluster:%s] restored persisted state: term=%d log_len=%d", n.cfg.NodeID, n.currentTerm, len(n.raftLog))
	return nil
}

// persistLocked must be called with n.mu held. Writes via a temp file +
// rename so a crash mid-write can't leave a half-written state file behind
// (same atomic-replace pattern the rest of this project uses for on-disk
// state).
func (n *Node) persistLocked() {
	path := n.stateFile()
	if path == "" {
		return
	}
	if err := os.MkdirAll(n.cfg.DataDir, 0o755); err != nil {
		n.cfg.Logger.Printf("[cluster:%s] persist: mkdir: %v", n.cfg.NodeID, err)
		return
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		n.cfg.Logger.Printf("[cluster:%s] persist: %v", n.cfg.NodeID, err)
		return
	}
	ps := persistedState{CurrentTerm: n.currentTerm, VotedFor: n.votedFor, Log: n.raftLog,
		SnapIndex: n.snapIndex, SnapTerm: n.snapTerm, SnapMembership: n.snapMembership}
	if err := gob.NewEncoder(f).Encode(&ps); err != nil {
		f.Close()
		n.cfg.Logger.Printf("[cluster:%s] persist: encode: %v", n.cfg.NodeID, err)
		return
	}
	if err := f.Sync(); err != nil {
		f.Close()
		n.cfg.Logger.Printf("[cluster:%s] persist: sync: %v", n.cfg.NodeID, err)
		return
	}
	f.Close()
	if err := os.Rename(tmp, path); err != nil {
		n.cfg.Logger.Printf("[cluster:%s] persist: rename: %v", n.cfg.NodeID, err)
	}
}
