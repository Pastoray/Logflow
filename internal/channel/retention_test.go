package channel

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestFileChannelRetentionPurgesUnconsumed verifies that an active
// retention sweep will purge a closed segment that has exceeded MaxAge,
// even though it still has unconsumed entries sitting in it - the
// distinguishing behavior from the reactive full-consumption GC that
// happens automatically as part of Take().
func TestFileChannelRetentionPurgesUnconsumed(t *testing.T) {
	dir, err := os.MkdirTemp("", "logflow-retention-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// A tiny max segment size so a handful of events forces multiple
	// segments to exist (only closed, non-active segments are eligible
	// for purging).
	fc, err := NewFileChannel("t", dir, 10000, 200, RetentionPolicy{
		MaxAge:   50 * time.Millisecond,
		Interval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fc.Close()

	ctx := context.Background()
	// Write enough events, with big enough bodies, to roll past the tiny
	// 200-byte segment size several times over, leaving earlier segments
	// closed (and, since we never Take() any of them, fully unconsumed).
	for i := 0; i < 30; i++ {
		e := &Event{Body: []byte("padding-to-force-segment-rolls-0123456789"), Timestamp: time.Now()}
		if err := fc.Put(ctx, e); err != nil {
			t.Fatal(err)
		}
	}

	entries, _ := os.ReadDir(dir)
	if len(entries) < 2 {
		t.Fatalf("expected multiple segments before retention runs, got %d", len(entries))
	}
	before := len(entries)

	// Wait past MaxAge plus a couple of sweep intervals.
	time.Sleep(200 * time.Millisecond)

	entries, _ = os.ReadDir(dir)
	after := len(entries)
	if after >= before {
		t.Fatalf("expected retention to have purged some segments: before=%d after=%d", before, after)
	}
	if fc.PurgedUnconsumed() == 0 {
		t.Fatalf("expected PurgedUnconsumed() > 0 since none of the purged segments were consumed, got 0")
	}
	t.Logf("segments before=%d after=%d purgedUnconsumed=%d", before, after, fc.PurgedUnconsumed())
}

// TestFileChannelNoRetentionKeepsSegments confirms that with no
// RetentionPolicy configured (the default), unconsumed segments are never
// purged - only the existing full-consumption GC applies.
func TestFileChannelNoRetentionKeepsSegments(t *testing.T) {
	dir, err := os.MkdirTemp("", "logflow-no-retention-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	fc, err := NewFileChannel("t", dir, 10000, 200, RetentionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer fc.Close()

	ctx := context.Background()
	for i := 0; i < 30; i++ {
		e := &Event{Body: []byte("padding-to-force-segment-rolls-0123456789"), Timestamp: time.Now()}
		if err := fc.Put(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	before, _ := os.ReadDir(dir)

	time.Sleep(200 * time.Millisecond)

	after, _ := os.ReadDir(dir)
	if len(after) != len(before) {
		t.Fatalf("expected segment count to stay stable with no retention policy: before=%d after=%d", len(before), len(after))
	}
	if fc.PurgedUnconsumed() != 0 {
		t.Fatalf("expected PurgedUnconsumed()==0 with no retention policy, got %d", fc.PurgedUnconsumed())
	}
}
