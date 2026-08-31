package channel

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestFileChannelDoesNotDropUnconsumedOnPartialTake(t *testing.T) {
	dir, err := os.MkdirTemp("", "logflow-partial-consume-test")
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

	if _, err := fc.Take(ctx); err != nil {
		t.Fatal(err)
	}

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
