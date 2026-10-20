package security

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"logflow/gen/logflowpb"
)

func writeTokens(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	// Guarantee a distinct stamp even on coarse-mtime filesystems.
	future := time.Now().Add(time.Duration(len(body)) * time.Millisecond)
	os.Chtimes(path, future, future)
}

func code(err error) codes.Code { return status.Code(err) }

func TestTokenStoreIdentityExpiryRevocation(t *testing.T) {
	fastReload(t)
	path := filepath.Join(t.TempDir(), "tokens.yaml")
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	writeTokens(t, path, fmt.Sprintf(`tokens:
  - id: alice
    token_sha256: %s
  - id: bob
    token: bob-secret
    expires_at: 2030-01-02T00:00:00Z
  - id: carol
    token: carol-secret
    revoked: true
`, HashToken("alice-secret")))
	s, err := NewTokenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return now }

	if id, err := s.Authenticate("alice-secret"); err != nil || id != "alice" {
		t.Fatalf("alice: id=%q err=%v", id, err)
	}
	if id, err := s.Authenticate("bob-secret"); err != nil || id != "bob" {
		t.Fatalf("bob before expiry: id=%q err=%v", id, err)
	}
	if _, err := s.Authenticate("carol-secret"); code(err) != codes.Unauthenticated || !strings.Contains(err.Error(), "revoked") {
		t.Fatalf("carol should be revoked, got %v", err)
	}
	if _, err := s.Authenticate("nobody"); code(err) != codes.Unauthenticated {
		t.Fatalf("unknown token accepted: %v", err)
	}
	now = now.Add(48 * time.Hour)
	s.now = func() time.Time { return now }
	if _, err := s.Authenticate("bob-secret"); code(err) != codes.Unauthenticated || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("bob should be expired, got %v", err)
	}
	if _, err := s.Authenticate("alice-secret"); err != nil {
		t.Fatalf("alice (no expiry) must still work: %v", err)
	}
}

func TestTokenStoreHotReloadAndBadReload(t *testing.T) {
	fastReload(t)
	path := filepath.Join(t.TempDir(), "tokens.yaml")
	writeTokens(t, path, "tokens:\n  - {id: a, token: one}\n")
	s, err := NewTokenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Authenticate("two"); err == nil {
		t.Fatal("two should not exist yet")
	}
	writeTokens(t, path, "tokens:\n  - {id: a, token: one}\n  - {id: b, token: two}\n")
	if id, err := s.Authenticate("two"); err != nil || id != "b" {
		t.Fatalf("added caller not picked up: %q %v", id, err)
	}
	// Revoke a by editing the file.
	writeTokens(t, path, "tokens:\n  - {id: a, token: one, revoked: true}\n  - {id: b, token: two}\n")
	if _, err := s.Authenticate("one"); err == nil {
		t.Fatal("revocation not picked up")
	}
	// Garbage keeps the last good set.
	writeTokens(t, path, "tokens: [[[ not yaml")
	if id, err := s.Authenticate("two"); err != nil || id != "b" {
		t.Fatalf("bad reload dropped the last good set: %q %v", id, err)
	}
	if _, err := s.Authenticate("one"); err == nil {
		t.Fatal("bad reload resurrected a revoked caller")
	}
}

func TestTokenFileValidation(t *testing.T) {
	dir := t.TempDir()
	for name, body := range map[string]string{
		"noid":    "tokens:\n  - {token: x}\n",
		"dup":     "tokens:\n  - {id: a, token: x}\n  - {id: a, token: y}\n",
		"notoken": "tokens:\n  - {id: a}\n",
		"badhash": "tokens:\n  - {id: a, token_sha256: zz}\n",
	} {
		p := filepath.Join(dir, name)
		writeTokens(t, p, body)
		if _, err := NewTokenStore(p); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	if _, err := NewTokenStore(filepath.Join(dir, "missing")); err == nil {
		t.Error("missing file must fail closed at startup")
	}
}

// collector is a minimal server: Ping is unary, StreamEvents just receives.
type collector struct {
	logflowpb.UnimplementedCollectorServer
	mu       sync.Mutex
	streamEr error
}

func (c *collector) Ping(ctx context.Context, _ *logflowpb.PingRequest) (*logflowpb.PingResponse, error) {
	return &logflowpb.PingResponse{Ok: true}, nil
}
func (c *collector) StreamEvents(st logflowpb.Collector_StreamEventsServer) error {
	for {
		if _, err := st.Recv(); err != nil {
			c.mu.Lock()
			c.streamEr = err
			c.mu.Unlock()
			return err
		}
	}
}

func startAuthServer(t *testing.T, s *TokenStore) (string, *collector) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	c := &collector{}
	srv := grpc.NewServer(grpc.UnaryInterceptor(UnaryServerAuthStore(s)), grpc.StreamInterceptor(StreamServerAuthStore(s)))
	logflowpb.RegisterCollectorServer(srv, c)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis.Addr().String(), c
}

func dial(t *testing.T, addr, token string) logflowpb.CollectorClient {
	t.Helper()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if token != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(TokenPerRPCCredentials{Token: token}))
	}
	cc, err := grpc.Dial(addr, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	return logflowpb.NewCollectorClient(cc)
}

func TestInterceptorsAndRevokingAnOpenStream(t *testing.T) {
	fastReload(t)
	old := streamRecheckInterval.Swap(int64(50 * time.Millisecond))
	t.Cleanup(func() { streamRecheckInterval.Store(old) })

	path := filepath.Join(t.TempDir(), "tokens.yaml")
	writeTokens(t, path, "tokens:\n  - {id: edge1, token: s1}\n  - {id: edge2, token: s2}\n")
	store, err := NewTokenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	addr, _ := startAuthServer(t, store)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if _, err := dial(t, addr, "").Ping(ctx, &logflowpb.PingRequest{}); code(err) != codes.Unauthenticated {
		t.Fatalf("no token: %v", err)
	}
	if _, err := dial(t, addr, "wrong").Ping(ctx, &logflowpb.PingRequest{}); code(err) != codes.Unauthenticated {
		t.Fatalf("wrong token: %v", err)
	}
	if r, err := dial(t, addr, "s1").Ping(ctx, &logflowpb.PingRequest{}); err != nil || !r.Ok {
		t.Fatalf("valid token rejected: %v", err)
	}

	// Open a stream as edge1 and keep sending; then revoke edge1 only.
	st, err := dial(t, addr, "s1").StreamEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	st2, err := dial(t, addr, "s2").StreamEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Send(&logflowpb.EventBatch{BatchId: "1"}); err != nil {
		t.Fatal(err)
	}
	writeTokens(t, path, "tokens:\n  - {id: edge1, token: s1, revoked: true}\n  - {id: edge2, token: s2}\n")

	// The server ends the stream with its auth error; the client sees it
	// on Recv (Send only reports io.EOF once the stream is gone).
	recvErr := make(chan error, 1)
	go func() { _, err := st.Recv(); recvErr <- err }()
	var cut error
	deadline := time.Now().Add(5 * time.Second)
loop:
	for time.Now().Before(deadline) {
		st2.Send(&logflowpb.EventBatch{BatchId: "ok"})
		st.Send(&logflowpb.EventBatch{BatchId: "x"})
		select {
		case cut = <-recvErr:
			break loop
		case <-time.After(30 * time.Millisecond):
		}
	}
	if code(cut) != codes.Unauthenticated {
		t.Fatalf("revoked caller's open stream not cut off with Unauthenticated: %v", cut)
	}
	if err := st2.Send(&logflowpb.EventBatch{BatchId: "still-fine"}); err != nil {
		t.Fatalf("unrevoked caller's stream was affected: %v", err)
	}
	if _, err := dial(t, addr, "s1").Ping(ctx, &logflowpb.PingRequest{}); code(err) != codes.Unauthenticated {
		t.Fatalf("revoked caller can still make new calls: %v", err)
	}
}
