package channel

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func ev(s string) *Event { return &Event{Body: []byte(s), Timestamp: time.Now()} }

func TestShardedFileSpreadsAndDeliversAll(t *testing.T) {
	dir := t.TempDir()
	sc, err := NewShardedFileChannel("w", dir, 4, 4000, 1<<20, RetentionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Close()
	ctx := context.Background()
	for i := 0; i < 400; i++ {
		if err := sc.Put(ctx, ev(fmt.Sprintf("e%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 4; i++ {
		if got := sc.shards[i].Stats().PutCount; got != 100 {
			t.Errorf("wal-%d got %d puts, want an even 100", i, got)
		}
	}
	seen := map[string]bool{}
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for i := 0; i < 400; i++ {
		e, err := sc.Take(tctx)
		if err != nil {
			t.Fatalf("take %d: %v", i, err)
		}
		seen[string(e.Body)] = true
	}
	if len(seen) != 400 {
		t.Fatalf("got %d distinct events, want 400", len(seen))
	}
	if st := sc.Stats(); st.PutCount != 400 || st.TakeCount != 400 || st.Size != 0 {
		t.Errorf("stats = %+v", st)
	}
}

// Unconsumed events in every WAL must come back after a restart.
func TestShardedFileRecoversEveryWAL(t *testing.T) {
	dir := t.TempDir()
	sc, err := NewShardedFileChannel("w", dir, 3, 3000, 1<<20, RetentionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 90; i++ {
		if err := sc.Put(context.Background(), ev(fmt.Sprintf("r%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	sc.Close()

	sc2, err := NewShardedFileChannel("w", dir, 3, 3000, 1<<20, RetentionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer sc2.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	seen := map[string]bool{}
	for i := 0; i < 90; i++ {
		e, err := sc2.Take(ctx)
		if err != nil {
			t.Fatalf("take %d after restart: %v", i, err)
		}
		seen[string(e.Body)] = true
	}
	if len(seen) != 90 {
		t.Fatalf("recovered %d distinct events, want 90", len(seen))
	}
}

func TestShardedFileRefusesLayoutChange(t *testing.T) {
	// single-WAL data present -> refuse sharding
	d1 := t.TempDir()
	fc, err := NewFileChannel("s", d1, 100, 1<<20, RetentionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	fc.Put(context.Background(), ev("x"))
	fc.Close()
	if _, err := NewShardedFileChannel("s", d1, 2, 100, 1<<20, RetentionPolicy{}); err == nil || !strings.Contains(err.Error(), "single-WAL") {
		t.Fatalf("expected refusal over single-WAL data, got %v", err)
	}
	// shrinking the shard count -> refuse
	d2 := t.TempDir()
	sc, err := NewShardedFileChannel("s", d2, 4, 400, 1<<20, RetentionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	sc.Close()
	if _, err := NewShardedFileChannel("s", d2, 2, 400, 1<<20, RetentionPolicy{}); err == nil || !strings.Contains(err.Error(), "wal-") {
		t.Fatalf("expected refusal when shrinking shard count, got %v", err)
	}
	// growing is fine: new shards are empty
	sc, err = NewShardedFileChannel("s", d2, 6, 600, 1<<20, RetentionPolicy{})
	if err != nil {
		t.Fatalf("growing the shard count should work: %v", err)
	}
	sc.Close()
	if _, err := NewShardedFileChannel("s", d2, 1, 10, 1<<20, RetentionPolicy{}); err == nil {
		t.Fatal("n<2 must be rejected")
	}
	_ = os.Remove(filepath.Join(d2, "x"))
}

func TestShardedFileTakeUnblocksOnCloseAndCancel(t *testing.T) {
	sc, err := NewShardedFileChannel("w", t.TempDir(), 2, 100, 1<<20, RetentionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := sc.Take(ctx); err != context.DeadlineExceeded {
		t.Fatalf("want deadline, got %v", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := sc.Take(context.Background()); err != ErrClosed {
			t.Errorf("want ErrClosed, got %v", err)
		}
	}()
	time.Sleep(50 * time.Millisecond)
	sc.Close()
	wg.Wait()
	if err := sc.Put(context.Background(), ev("x")); err != ErrClosed {
		t.Fatalf("put after close: %v", err)
	}
}

// SpillableMemoryChannel must work with a sharded overflow.
func TestSpillableWithShardedOverflow(t *testing.T) {
	ov, err := NewShardedFileChannel("o", t.TempDir(), 3, 3000, 1<<20, RetentionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	sp := NewSpillableMemoryChannel("sp", NewMemoryChannel("p", 10, 1), ov, time.Millisecond)
	defer sp.Close()
	for i := 0; i < 100; i++ {
		if err := sp.Put(context.Background(), ev(fmt.Sprintf("s%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if sp.Spilled() < 80 {
		t.Fatalf("spilled = %d, expected ~90", sp.Spilled())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		e, err := sp.Take(ctx)
		if err != nil {
			t.Fatalf("take %d: %v", i, err)
		}
		seen[string(e.Body)] = true
	}
	if len(seen) != 100 {
		t.Fatalf("got %d distinct, want 100", len(seen))
	}
}

// BenchmarkParallelPut compares durable Put throughput under concurrent
// producers for 1 vs N WALs. Run: go test -bench ParallelPut -run xxx ./internal/channel
func BenchmarkParallelPut(b *testing.B) {
	for _, n := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("wal=%d", n), func(b *testing.B) {
			dir := b.TempDir()
			var ch Channel
			var err error
			if n == 1 {
				ch, err = NewFileChannel("b", dir, 4_000_000, 256<<20, RetentionPolicy{})
			} else {
				ch, err = NewShardedFileChannel("b", dir, n, 4_000_000, 256<<20, RetentionPolicy{})
			}
			if err != nil {
				b.Fatal(err)
			}
			defer ch.Close()
			e := ev("2026-08-21T12:00:00Z host=web-07 level=INFO msg=\"request handled\" duration_ms=12")
			ctx := context.Background()
			b.SetParallelism(8)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if err := ch.Put(ctx, e); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
