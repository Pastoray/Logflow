package security

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
)

// --- Per-caller bearer tokens ------------------------------------------
//
// A token file replaces the single static shared secret with one entry per
// caller, each with its own identity, optional expiry and a revoked flag:
//
//	tokens:
//	  - id: edge-eu-1
//	    token_sha256: 9f86d0...      # sha256 hex of the secret (preferred)
//	    expires_at: 2027-01-01T00:00:00Z
//	  - id: batch-job
//	    token: plain-secret          # convenience; hashed on load
//	    revoked: true
//
// The file is re-read when its mtime/size changes (checked at most once
// per reloadCheckInterval), so adding, expiring or revoking a caller takes
// effect without restarting the agent. A reload that fails to parse keeps
// the last good set. Long-lived streams re-validate their token about
// once a second, so a revocation also cuts off a stream that was already
// open - not just new connections.

// TokenEntry is one caller's credential.
type TokenEntry struct {
	ID          string    `yaml:"id"`
	TokenSHA256 string    `yaml:"token_sha256"`
	Token       string    `yaml:"token"`
	ExpiresAt   time.Time `yaml:"expires_at"`
	Revoked     bool      `yaml:"revoked"`

	hash [sha256.Size]byte
}

type tokenFile struct {
	Tokens []TokenEntry `yaml:"tokens"`
}

// HashToken returns the sha256 hex digest stored in `token_sha256`.
func HashToken(secret string) string {
	h := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(h[:])
}

// GenerateToken returns a fresh random secret (256 bits, hex).
func GenerateToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// TokenStore authenticates presented bearer tokens against a token file.
type TokenStore struct {
	path string
	now  func() time.Time

	mu        sync.Mutex
	entries   []TokenEntry
	stamp     fileStamp
	lastCheck time.Time

	cmu      sync.Mutex
	accepted map[string]int64
	rejected map[string]int64

	accDesc, rejDesc, entDesc *prometheus.Desc
}

// NewTokenStore loads path; an unreadable or invalid file is an error at
// startup (fail closed), unlike later reloads which keep the last good set.
func NewTokenStore(path string) (*TokenStore, error) {
	s := &TokenStore{
		path: path, now: time.Now,
		accepted: map[string]int64{}, rejected: map[string]int64{},
		accDesc: prometheus.NewDesc("logflow_auth_accepted_total", "Authenticated calls/stream-checks per caller id.", []string{"caller"}, nil),
		rejDesc: prometheus.NewDesc("logflow_auth_rejected_total", "Rejected calls per caller id and reason (caller=unknown when the token matched nothing).", []string{"caller", "reason"}, nil),
		entDesc: prometheus.NewDesc("logflow_auth_tokens_active", "Token-file entries that are neither revoked nor expired.", nil, nil),
	}
	entries, st, err := loadTokenFile(path)
	if err != nil {
		return nil, err
	}
	s.entries, s.stamp, s.lastCheck = entries, st, time.Now()
	return s, nil
}

func loadTokenFile(path string) ([]TokenEntry, fileStamp, error) {
	st, err := stamp(path)
	if err != nil {
		return nil, fileStamp{}, fmt.Errorf("auth tokens file: %w", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fileStamp{}, fmt.Errorf("auth tokens file: %w", err)
	}
	var tf tokenFile
	if err := yaml.Unmarshal(raw, &tf); err != nil {
		return nil, fileStamp{}, fmt.Errorf("auth tokens file %s: %w", path, err)
	}
	seen := map[string]bool{}
	for i := range tf.Tokens {
		e := &tf.Tokens[i]
		if e.ID == "" {
			return nil, fileStamp{}, fmt.Errorf("auth tokens file %s: entry %d has no id", path, i)
		}
		if seen[e.ID] {
			return nil, fileStamp{}, fmt.Errorf("auth tokens file %s: duplicate id %q", path, e.ID)
		}
		seen[e.ID] = true
		switch {
		case e.TokenSHA256 != "":
			b, err := hex.DecodeString(e.TokenSHA256)
			if err != nil || len(b) != sha256.Size {
				return nil, fileStamp{}, fmt.Errorf("auth tokens file %s: entry %q: token_sha256 must be 64 hex chars", path, e.ID)
			}
			copy(e.hash[:], b)
		case e.Token != "":
			e.hash = sha256.Sum256([]byte(e.Token))
			e.Token = "" // never keep the plaintext around
		default:
			return nil, fileStamp{}, fmt.Errorf("auth tokens file %s: entry %q has neither token nor token_sha256", path, e.ID)
		}
	}
	return tf.Tokens, st, nil
}

func (s *TokenStore) maybeReload() {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if now.Sub(s.lastCheck) < reloadCheckInterval {
		return
	}
	s.lastCheck = now
	st, err := stamp(s.path)
	if err != nil {
		log.Printf("[auth] cannot stat %s (keeping last good tokens): %v", s.path, err)
		return
	}
	if st == s.stamp {
		return
	}
	entries, st, err := loadTokenFile(s.path)
	if err != nil {
		log.Printf("[auth] reload failed (keeping last good tokens): %v", err)
		return
	}
	s.entries, s.stamp = entries, st
	log.Printf("[auth] reloaded %d token(s) from %s", len(entries), s.path)
}

// Authenticate returns the caller id for a presented secret, or a gRPC
// Unauthenticated error. Every entry is compared (no early exit) so timing
// does not reveal which entry, if any, matched.
func (s *TokenStore) Authenticate(secret string) (string, error) {
	s.maybeReload()
	h := sha256.Sum256([]byte(secret))
	return s.authHash(h)
}

func (s *TokenStore) authHash(h [sha256.Size]byte) (string, error) {
	s.mu.Lock()
	entries := s.entries
	s.mu.Unlock()
	now := s.now()
	var match *TokenEntry
	for i := range entries {
		if subtle.ConstantTimeCompare(h[:], entries[i].hash[:]) == 1 {
			match = &entries[i]
		}
	}
	if match == nil {
		s.count(false, "unknown", "invalid")
		return "", status.Error(codes.Unauthenticated, "invalid authorization token")
	}
	if match.Revoked {
		s.count(false, match.ID, "revoked")
		return match.ID, status.Error(codes.Unauthenticated, "authorization token revoked")
	}
	if !match.ExpiresAt.IsZero() && !now.Before(match.ExpiresAt) {
		s.count(false, match.ID, "expired")
		return match.ID, status.Error(codes.Unauthenticated, "authorization token expired")
	}
	s.count(true, match.ID, "")
	return match.ID, nil
}

func (s *TokenStore) count(ok bool, id, reason string) {
	s.cmu.Lock()
	defer s.cmu.Unlock()
	if ok {
		s.accepted[id]++
	} else {
		s.rejected[id+"\x00"+reason]++
	}
}

// Describe/Collect make the store a prometheus.Collector.
func (s *TokenStore) Describe(ch chan<- *prometheus.Desc) {
	ch <- s.accDesc
	ch <- s.rejDesc
	ch <- s.entDesc
}

func (s *TokenStore) Collect(ch chan<- prometheus.Metric) {
	s.cmu.Lock()
	for id, n := range s.accepted {
		ch <- prometheus.MustNewConstMetric(s.accDesc, prometheus.CounterValue, float64(n), id)
	}
	for k, n := range s.rejected {
		for i := 0; i < len(k); i++ {
			if k[i] == 0 {
				ch <- prometheus.MustNewConstMetric(s.rejDesc, prometheus.CounterValue, float64(n), k[:i], k[i+1:])
				break
			}
		}
	}
	s.cmu.Unlock()
	s.mu.Lock()
	active := 0
	now := s.now()
	for _, e := range s.entries {
		if !e.Revoked && (e.ExpiresAt.IsZero() || now.Before(e.ExpiresAt)) {
			active++
		}
	}
	s.mu.Unlock()
	ch <- prometheus.MustNewConstMetric(s.entDesc, prometheus.GaugeValue, float64(active))
}

func presentedSecret(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing authorization metadata")
	}
	vals := md.Get(tokenMetadataKey)
	if len(vals) == 0 {
		return "", status.Error(codes.Unauthenticated, "missing authorization token")
	}
	v := vals[0]
	const p = "Bearer "
	if len(v) >= len(p) && v[:len(p)] == p {
		v = v[len(p):]
	}
	return v, nil
}

// UnaryServerAuthStore authenticates unary calls against a TokenStore.
func UnaryServerAuthStore(s *TokenStore) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		secret, err := presentedSecret(ctx)
		if err != nil {
			return nil, err
		}
		if _, err := s.Authenticate(secret); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// streamRecheckInterval is how often an open stream re-validates its
// token (var so tests can shrink it).
var streamRecheckInterval atomic.Int64

func init() { streamRecheckInterval.Store(int64(time.Second)) }

type recheckStream struct {
	grpc.ServerStream
	store *TokenStore
	hash  [sha256.Size]byte
	id    string
	last  time.Time
}

func (r *recheckStream) RecvMsg(m any) error {
	if now := time.Now(); now.Sub(r.last) >= time.Duration(streamRecheckInterval.Load()) {
		r.last = now
		r.store.maybeReload()
		if _, err := r.store.authHash(r.hash); err != nil {
			log.Printf("[auth] closing stream of caller %q: %v", r.id, err)
			return err
		}
	}
	return r.ServerStream.RecvMsg(m)
}

// StreamServerAuthStore authenticates a stream at setup and keeps
// re-validating it while it stays open.
func StreamServerAuthStore(s *TokenStore) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		secret, err := presentedSecret(ss.Context())
		if err != nil {
			return err
		}
		id, err := s.Authenticate(secret)
		if err != nil {
			return err
		}
		return handler(srv, &recheckStream{ServerStream: ss, store: s, hash: sha256.Sum256([]byte(secret)), id: id, last: time.Now()})
	}
}
