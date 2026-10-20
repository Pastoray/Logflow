package channel

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestFileChannelDoesNotDropUnconsumedOnPartialTake is a regression test
// for a bug where a rolled (non-active) segment's `written` count was
// tracked on a different struct instance than the one Take()/maybeGC
// checked against, making it read as 0. That made maybeGC's "has every
// entry been consumed" check (consumed >= written) true after the very
// first Take() from any rolled segment - deleting it, and silently
// discarding every other entry still in it, even with no retention policy
// configured at all.
func TestFileChannelDoesNotDropUnconsumedOnPartialTake(t *testing.T) {
	dir, err := os.MkdirTemp("", "logflow-partial-consume-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Tiny segments so a modest number of events forces several rolls.
	fc, err := NewFileChannel("t", dir, 10000, 200, RetentionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer fc.Close()

	ctx := context.Background()
	const n = 40
	for i := 0; i < n; i++ {
		e := &Event{Body: []byte("padding-to-force-segment-rolls-0123456789"), Timestamp: time.Now()}
		if err := fc.Put(ctx, e); err != nil {
			t.Fatal(err)
		}
	}

	entries, _ := os.ReadDir(dir)
	if len(entries) < 2 {
		t.Fatalf("expected multiple segments (rolls) to set up this test, got %d", len(entries))
	}

	// Take exactly ONE event - enough to trigger maybeGC's check on
	// whatever segment held the very first entry, without draining
	// anything else from it.
	if _, err := fc.Take(ctx); err != nil {
		t.Fatal(err)
	}

	// The bug would have deleted that entire first segment (several
	// entries) after this single Take(). Confirm the rest of its entries
	// (and every other segment) are still fully readable.
	got := make(map[string]bool, n-1)
	for i := 0; i < n-1; i++ {
		tctx, cancel := context.WithTimeout(ctx, time.Second)
		e, err := fc.Take(tctx)
		cancel()
		if err != nil {
			t.Fatalf("Take() %d/%d failed (bug would manifest as data silently missing here): %v", i+1, n-1, err)
		}
		got[string(e.Body)] = true
	}

	if len(got) != 1 {
		// All bodies are identical in this test, so we can't check
		// distinct content - what matters is that we got n-1 successful,
		// non-error Takes with no premature "segment not open for read"
		// failures, which the loop above already asserts on each
		// iteration. This is just a final sanity count.
		t.Fatalf("expected to read a consistent body across all remaining events, got %d distinct", len(got))
	}

	st := fc.Stats()
	if st.DroppedCount != 0 {
		t.Fatalf("expected zero drops with no retention policy configured, got %d", st.DroppedCount)
	}
	if st.TakeCount != n {
		t.Fatalf("expected TakeCount == %d, got %d", n, st.TakeCount)
	}
}
