package channel

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// BenchmarkFileChannelPutTake measures the allocation cost of a full
// Put+Take round trip through the durable file channel - the hot path
// this file's zero-copy optimizations (pooled encode buffer, aliased
// decode body) target. Run with `go test -bench=. -benchmem ./internal/channel`.
func BenchmarkFileChannelPutTake(b *testing.B) {
	dir, err := os.MkdirTemp("", "logflow-bench-wal")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	fc, err := NewFileChannel("bench", dir, 1_000_000, 256<<20, RetentionPolicy{})
	if err != nil {
		b.Fatal(err)
	}
	defer fc.Close()

	ctx := context.Background()
	body := []byte("2026-08-21T12:00:00Z host=web-07 level=INFO msg=\"request handled\" duration_ms=12")
	headers := map[string]string{"env": "prod", "service": "checkout"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := &Event{Body: body, Headers: headers, Timestamp: time.Unix(0, int64(i))}
		if err := fc.Put(ctx, e); err != nil {
			b.Fatal(err)
		}
		if _, err := fc.Take(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMemoryChannelPutTake is a baseline for comparison: the memory
// channel never serializes at all, so its allocation profile just reflects
// the shared Event struct itself, not any encode/decode work.
func BenchmarkMemoryChannelPutTake(b *testing.B) {
	mc := NewMemoryChannel("bench", 1_000_000, 4)
	defer mc.Close()

	ctx := context.Background()
	body := []byte("2026-08-21T12:00:00Z host=web-07 level=INFO msg=\"request handled\" duration_ms=12")
	headers := map[string]string{"env": "prod", "service": "checkout"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := &Event{Body: body, Headers: headers, Timestamp: time.Unix(0, int64(i))}
		if err := mc.Put(ctx, e); err != nil {
			b.Fatal(err)
		}
		if _, err := mc.Take(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func ExampleFileChannel_roundTrip() {
	dir, _ := os.MkdirTemp("", "logflow-example-wal")
	defer os.RemoveAll(dir)
	fc, _ := NewFileChannel("example", dir, 100, 1<<20, RetentionPolicy{})
	defer fc.Close()

	ctx := context.Background()
	fc.Put(ctx, &Event{Body: []byte("hello"), Timestamp: time.Now()})
	e, _ := fc.Take(ctx)
	fmt.Println(string(e.Body))
	// Output: hello
}
