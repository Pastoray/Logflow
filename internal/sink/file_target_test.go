package sink

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"logflow/internal/channel"
)

func fill(t *testing.T, ch channel.Channel, n int, prefix string) {
	t.Helper()
	for i := 0; i < n; i++ {
		e := &channel.Event{Body: []byte(prefix + string(rune('a'+i%26)) + strings.Repeat("x", i))}
		if err := ch.Put(context.Background(), e); err != nil {
			t.Fatal(err)
		}
	}
}

func readAll(t *testing.T, dir string) string {
	t.Helper()
	ents, _ := os.ReadDir(dir)
	var sb strings.Builder
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), ".") {
			continue
		}
		b, _ := os.ReadFile(filepath.Join(dir, e.Name()))
		sb.Write(b)
	}
	return sb.String()
}

func TestTargetHelpers(t *testing.T) {
	if !IsFileTarget("file:/tmp/x") || IsFileTarget("host:1234") {
		t.Fatal("IsFileTarget wrong")
	}
	if FileTargetDir("file:/tmp/x") != "/tmp/x" {
		t.Fatal("FileTargetDir wrong")
	}
	if ValidateTarget("file:") == nil || ValidateTarget("") == nil || ValidateTarget("file:/a") != nil || ValidateTarget("h:1") != nil {
		t.Fatal("ValidateTarget wrong")
	}
	d := t.TempDir()
	if !fileTargetHealthy(d) {
		t.Fatal("writable dir should be healthy")
	}
	if fileTargetHealthy("/dev/null/nope") {
		t.Fatal("unusable dir should be unhealthy")
	}
	if ents, _ := os.ReadDir(d); len(ents) != 0 {
		t.Fatalf("health probe left files behind: %v", ents)
	}
}

// A genuine ENOSPC (/dev/full) must hand the batch back to the channel
// instead of silently discarding it (the pre-fix behaviour).
func TestFileSinkWriteErrorRequeues(t *testing.T) {
	if _, err := os.Stat("/dev/full"); err != nil {
		t.Skip("no /dev/full")
	}
	ch := channel.NewMemoryChannel("c", 100, 1)
	fill(t, ch, 10, "e")
	fs := NewFileSink(FileSinkConfig{Name: "t", Dir: t.TempDir(), BatchSize: 10, BatchLinger: 50 * time.Millisecond,
		RollMaxBytes: 1 << 30, RollInterval: time.Hour}, ch, nil)
	fs.openFile = func(string) (*os.File, error) { return os.OpenFile("/dev/full", os.O_WRONLY, 0) }

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := fs.RunUntilError(ctx)
	if err == nil {
		t.Fatal("expected a write error from /dev/full")
	}
	var se *setupError
	if errors.As(err, &se) {
		t.Fatalf("write error misclassified as setup error: %v", err)
	}
	if got := ch.Stats().Size; got != 10 {
		t.Fatalf("channel holds %d events after failed write, want all 10 returned", got)
	}
}

// Primary (a file dir that cannot be written) fails -> failover spools to
// the file backup; nothing is lost.
func TestFailoverFallsBackToFileTarget(t *testing.T) {
	if _, err := os.Stat("/dev/null"); err != nil {
		t.Skip()
	}
	backup := t.TempDir()
	ch := channel.NewMemoryChannel("c", 1000, 1)
	fill(t, ch, 50, "f")
	g := NewFailoverGroup(FailoverGroupConfig{
		Name: "fo", Targets: []string{"file:/dev/null/primary", "file:" + backup},
		BatchSize: 10, BatchLinger: 20 * time.Millisecond,
		RetryBackoff: 20 * time.Millisecond, RecoveryProbeInterval: time.Hour,
		FileRollMaxBytes: 1 << 30, FileRollInterval: time.Hour,
	}, ch, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { g.Run(ctx); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && strings.Count(readAll(t, backup), "\n") < 50 {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
	if n := strings.Count(readAll(t, backup), "\n"); n != 50 {
		t.Fatalf("backup file has %d lines, want 50", n)
	}
}

// Recovery: while spooling to the backup, a recovered higher-priority file
// target is promoted and receives subsequent events.
func TestFailoverPromotesRecoveredFileTarget(t *testing.T) {
	root := t.TempDir()
	primary := filepath.Join(root, "primary")
	// Make primary unusable: a regular file where the directory should be.
	if err := os.WriteFile(primary, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(root, "backup")
	ch := channel.NewMemoryChannel("c", 1000, 1)
	fill(t, ch, 5, "a")
	g := NewFailoverGroup(FailoverGroupConfig{
		Name: "fo", Targets: []string{"file:" + primary, "file:" + backup},
		BatchSize: 5, BatchLinger: 20 * time.Millisecond,
		RetryBackoff: 20 * time.Millisecond, RecoveryProbeInterval: 150 * time.Millisecond,
		FileRollMaxBytes: 1 << 30, FileRollInterval: time.Hour,
	}, ch, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { g.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	wait := func(dir string, lines int) bool {
		end := time.Now().Add(5 * time.Second)
		for time.Now().Before(end) {
			if strings.Count(readAll(t, dir), "\n") >= lines {
				return true
			}
			time.Sleep(20 * time.Millisecond)
		}
		return false
	}
	if !wait(backup, 5) {
		t.Fatal("backup never received the first events")
	}
	// Repair the primary, then send more.
	os.Remove(primary)
	if err := os.MkdirAll(primary, 0o755); err != nil {
		t.Fatal(err)
	}
	time.Sleep(600 * time.Millisecond) // let the prober promote it
	fill(t, ch, 5, "b")
	if !wait(primary, 5) {
		t.Fatalf("recovered primary never received events (backup=%q)", readAll(t, backup))
	}
}
