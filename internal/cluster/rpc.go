package cluster

import "time"

// RPC argument/reply types and handlers, registered over net/rpc (gob
// encoding over plain TCP - an internal control-plane protocol, separate
// from the gRPC data path the rest of logflow uses for events). Two
// services run on two different listeners per node:
//
//   - "Raft" on Peer.Addr: RequestVote + AppendEntries, the Raft protocol
//     itself, called only by other cluster members.
//   - "Raft" on Peer.ClientAddr: ClientApply, called by *this process's
//     own* ReplicatedChannel when it isn't the leader, to forward a
//     client's Put to whichever node currently is.

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
}

// raftRPC exposes Node's Raft-protocol methods over net/rpc. Defined as a
// distinct named type (rather than methods on *Node directly) so the
// RPC-visible surface is explicit and separate from Node's Go API.
type raftRPC Node

func (r *raftRPC) n() *Node { return (*Node)(r) }

func (r *raftRPC) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	n := r.n()
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		reply.VoteGranted = false
		return nil
	}

	// Reject a candidate that isn't part of the cluster as far as OUR OWN
	// log is concerned (Cold or Cnew - votingPeersLocked covers both) -
	// BEFORE adopting its term or stepping down as leader because of it,
	// not just before granting it a vote. A node removed via
	// RemoveServer (which steps itself down per Raft §6.2 - see
	// changeMembership) keeps its own election timer running, and once
	// no one's replicating to it anymore that timer WILL eventually fire
	// (it's a plain follower, not Joining). If every live node still
	// adopted its ever-climbing term and (if leader) stepped down before
	// checking membership, that removed node could disrupt the cluster
	// indefinitely even though it could never actually win - it would
	// just force a pointless re-election every time its own timeout
	// fired. Checking membership first denies it that leverage entirely:
	// its RequestVote changes nothing about any real node's state.
	// Trade-off: a voter whose log hasn't yet caught up to a just-added,
	// legitimate candidate could transiently reject it too - elections
	// already retry on timeout, so that just costs one extra round in
	// the rare case it happens.
	if !containsID(n.votingPeersLocked(), args.CandidateID) {
		reply.Term = n.currentTerm
		reply.VoteGranted = false
		return nil
	}

	if args.Term > n.currentTerm {
		n.stepDownLocked(args.Term)
	}
	reply.Term = n.currentTerm

	lastIdx, lastTerm := n.lastLogIndexTermLocked()
	logOK := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)

	if (n.votedFor == "" || n.votedFor == args.CandidateID) && logOK {
		n.votedFor = args.CandidateID
		n.persistLocked()
		reply.VoteGranted = true
		n.notifyReset()
	} else {
		reply.VoteGranted = false
	}
	return nil
}

func (r *raftRPC) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	n := r.n()
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		reply.Success = false
		return nil
	}
	// Valid leader contact (term is current or newer): reset the election
	// clock and recognize them as leader, even before the log-consistency
	// check below - a heartbeat from a legitimate leader must suppress
	// our own election timer regardless of whether our log happens to be
	// caught up yet.
	if args.Term > n.currentTerm {
		n.stepDownLocked(args.Term)
	}
	n.state = Follower
	n.leaderID = args.LeaderID
	n.notifyReset()
	reply.Term = n.currentTerm

	// Entries at or below our snapshot boundary are committed and already
	// applied, so they match the leader's by Raft's safety property: skip
	// past them rather than treating them as a log mismatch.
	if args.PrevLogIndex < n.snapIndex {
		skip := n.snapIndex - args.PrevLogIndex
		if skip >= uint64(len(args.Entries)) {
			args.Entries = nil
		} else {
			args.Entries = args.Entries[skip:]
		}
		args.PrevLogIndex = n.snapIndex
		args.PrevLogTerm = n.snapTerm
	}
	if args.PrevLogIndex > 0 {
		t, ok := n.termAtLocked(args.PrevLogIndex)
		if !ok {
			reply.Success = false
			return nil
		}
		if t != args.PrevLogTerm {
			// Conflicting entry: drop it and everything after it.
			n.truncateFromLocked(args.PrevLogIndex)
			n.recomputeMembershipLocked() // the truncated entries may have included our last-known config
			n.persistLocked()
			reply.Success = false
			return nil
		}
	}

	if len(args.Entries) > 0 {
		// Truncate on first conflict, then append whatever's new -
		// standard Raft log-matching reconciliation.
		insertAt := args.PrevLogIndex
		for i, e := range args.Entries {
			idx := insertAt + uint64(i) + 1
			if idx <= n.lastIndexLocked() {
				if t, _ := n.termAtLocked(idx); t != e.Term {
					n.truncateFromLocked(idx)
					n.raftLog = append(n.raftLog, args.Entries[i:]...)
					break
				}
				continue // already have this exact entry
			}
			n.raftLog = append(n.raftLog, args.Entries[i:]...)
			break
		}
		// A follower's own membership view must track whatever's most
		// recent in ITS log, same as the leader's - including a config
		// entry it just received for the first time (this is also what
		// clears n.joining for a node started with Config.Joining - see
		// recomputeMembershipLocked).
		n.recomputeMembershipLocked()
		n.persistLocked()
	}

	if args.LeaderCommit > n.commitIndex {
		lastNew, _ := n.lastLogIndexTermLocked()
		if args.LeaderCommit < lastNew {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = lastNew
		}
		n.signalApply()
	}
	reply.Success = true
	return nil
}

// InstallSnapshotArgs ships a compaction boundary to a follower whose
// needed entries the leader has already discarded. The state machine here
// is "append event to the local channel", whose contents are not part of
// the snapshot: a follower that installs one has the log position and
// membership but not the events inside the skipped range (see "Log
// compaction" in the package doc).
type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Membership        Membership
}

type InstallSnapshotReply struct {
	Term    uint64
	Success bool
}

func (r *raftRPC) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	n := r.n()
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		return nil
	}
	if args.Term > n.currentTerm {
		n.stepDownLocked(args.Term)
	}
	n.state = Follower
	n.leaderID = args.LeaderID
	n.notifyReset()
	reply.Term = n.currentTerm

	// Anything at or below our commit index is committed, hence identical
	// on every node by Raft's state-machine-safety property, and is
	// already applied here or covered by our own snapshot. Installing an
	// older (or equal) boundary would only throw away log we still have and
	// roll snapIndex backwards - a stale leader message or a leader that
	// compacted less aggressively than we did makes this normal.
	if args.LastIncludedIndex <= n.commitIndex {
		reply.Success = true
		return nil
	}
	// If we already hold that exact entry (or have it covered by our own
	// snapshot) there is nothing to install: normal AppendEntries from the
	// boundary onward works.
	if t, ok := n.termAtLocked(args.LastIncludedIndex); ok && t == args.LastIncludedTerm {
		reply.Success = true
		return nil
	}
	// Otherwise our log is behind or conflicts: replace it wholesale.
	skippedFrom := n.lastApplied
	n.raftLog = nil
	n.snapIndex, n.snapTerm, n.snapMembership = args.LastIncludedIndex, args.LastIncludedTerm, args.Membership
	if n.commitIndex < n.snapIndex {
		n.commitIndex = n.snapIndex
	}
	if n.lastApplied < n.snapIndex {
		n.lastApplied = n.snapIndex
	}
	n.recomputeMembershipLocked()
	n.persistLocked()
	n.cfg.Logger.Printf("[cluster:%s] installed snapshot through index %d (term %d) from %s; log positions %d..%d were not replayed on this node",
		n.cfg.NodeID, n.snapIndex, n.snapTerm, args.LeaderID, skippedFrom+1, n.snapIndex)
	reply.Success = true
	return nil
}

// ClientApply is what a non-leader ReplicatedChannel calls on whichever
// node it currently believes is leader. If that belief is stale, the
// reply carries the NewLeader hint this node knows about so the caller
// can redirect without another round of guessing.
type ClientApplyArgs struct {
	Data []byte
}

type ClientApplyReply struct {
	Err           string
	NotLeader     bool
	NewLeaderID   string
	NewLeaderAddr string // ClientAddr of the (believed) actual leader
}

type clientRPC Node

func (r *clientRPC) n() *Node { return (*Node)(r) }

func (r *clientRPC) ClientApply(args *ClientApplyArgs, reply *ClientApplyReply) error {
	n := r.n()
	if !n.IsLeader() {
		if p, ok := n.LeaderPeer(); ok {
			reply.NotLeader = true
			reply.NewLeaderID = p.ID
			reply.NewLeaderAddr = p.ClientAddr
		} else {
			reply.NotLeader = true
		}
		return nil
	}
	if err := n.Apply(args.Data, applyRPCTimeout); err != nil {
		reply.Err = err.Error()
		if err == ErrNotLeader {
			reply.NotLeader = true
			if p, ok := n.LeaderPeer(); ok {
				reply.NewLeaderID = p.ID
				reply.NewLeaderAddr = p.ClientAddr
			}
		}
	}
	return nil
}

// applyRPCTimeout bounds how long a leader will wait for its own Apply
// (replication + local apply) to complete when driven by another node's
// forwarded ClientApply - independent of, and typically shorter than, the
// caller's own end-to-end timeout so a slow leader fails fast enough for
// the caller to still retry elsewhere within its own budget.
const applyRPCTimeout = 5 * time.Second

// ChangeMembershipArgs/Reply: an admin operation (AddServer/RemoveServer)
// to add or remove a cluster member live, no restart required - see
// Node.changeMembership and this package's doc comment. Exactly one of
// AddPeer/RemovePeer is set. Registered on clientRPC (ClientAddr),
// alongside ClientApply, so it reuses that same "call any node, get
// redirected to the leader" pattern without needing its own listener.
type ChangeMembershipArgs struct {
	AddPeer    *Peer
	RemovePeer string
}

type ChangeMembershipReply struct {
	Err           string
	NotLeader     bool
	NewLeaderID   string
	NewLeaderAddr string
}

func (r *clientRPC) ChangeMembership(args *ChangeMembershipArgs, reply *ChangeMembershipReply) error {
	n := r.n()
	if !n.IsLeader() {
		if p, ok := n.LeaderPeer(); ok {
			reply.NotLeader = true
			reply.NewLeaderID = p.ID
			reply.NewLeaderAddr = p.ClientAddr
		} else {
			reply.NotLeader = true
		}
		return nil
	}
	var err error
	if args.AddPeer != nil {
		err = n.addVoter(*args.AddPeer, membershipCatchUpTimeout, membershipCommitTimeout)
	} else {
		err = n.removeVoter(args.RemovePeer, membershipCommitTimeout)
	}
	if err != nil {
		reply.Err = err.Error()
		if err == ErrNotLeader {
			reply.NotLeader = true
			if p, ok := n.LeaderPeer(); ok {
				reply.NewLeaderID = p.ID
				reply.NewLeaderAddr = p.ClientAddr
			}
		}
	}
	return nil
}

const (
	membershipCatchUpTimeout = 10 * time.Second
	membershipCommitTimeout  = 5 * time.Second
)
