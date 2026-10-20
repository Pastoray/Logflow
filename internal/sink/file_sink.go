package sink

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"logflow/internal/channel"
	"logflow/internal/metrics"
)

type FileSinkConfig struct {
	Name         string
	Dir          string
	BatchSize    int
	BatchLinger  time.Duration
	RollMaxBytes int64
	RollInterval time.Duration
}

// FileSink drains a channel and appends events as newline-delimited text to
// files under Dir, rolling to a new file once the current one exceeds
// RollMaxBytes or RollInterval has elapsed - the same policy Flume's
// file_roll sink uses.
type FileSink struct {
	cfg     FileSinkConfig
	channel channel.Channel
	metrics *metrics.Registry

	// openFile, if non-nil, replaces os.OpenFile in roll - a test seam so a
	// test can point the sink at /dev/full for a genuine ENOSPC.
	openFile func(path string) (*os.File, error)

	f        *os.File
	w        *bufio.Writer
	written  int64
	openedAt time.Time
}

func NewFileSink(cfg FileSinkConfig, ch channel.Channel, reg *metrics.Registry) *FileSink {
	return &FileSink{cfg: cfg, channel: ch, metrics: reg}
}

// setupError marks a failure to prepare the sink at all (mkdir, first
// file open) as opposed to a write error mid-run - see Run.
type setupError struct{ err error }

func (e *setupError) Error() string { return e.err.Error() }
func (e *setupError) Unwrap() error { return e.err }

// Run drains the channel to disk until ctx is done. A failure to set up
// (cannot create Dir / open the first file) is returned; a write error
// mid-run (disk full, I/O error) is NOT fatal for a standalone sink: the
// batch is returned to the channel, the error logged, and writing retried
// after a backoff with a fresh file - see RunUntilError.
func (s *FileSink) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		err := s.RunUntilError(ctx)
		if ctx.Err() != nil || err == nil {
			return nil
		}
		var se *setupError
		if errors.As(err, &se) {
			return se.err
		}
		log.Printf("[sink:%s] write failed, will retry in %s: %v", s.cfg.Name, fileSinkRetryBackoff, err)
		select {
		case <-ctx.Done():
		case <-time.After(fileSinkRetryBackoff):
		}
	}
	return nil
}

const fileSinkRetryBackoff = time.Second

// RunUntilError is Run's core loop, exposed so a failover group can treat
// a file backend like any other: it returns the first write/flush error
// (after handing the in-flight batch back to the channel so nothing is
// lost) instead of retrying internally, or nil when ctx is done. Before
// this existed FileSink ignored write and flush errors entirely
// (`n, _ := w.Write(...)`, unchecked Flush), so a full disk silently
// discarded every event it had already taken from the channel.
//
// A batch that failed partway may have had its first few events already
// written before the error; re-queuing the whole batch means those few can
// appear twice - at-least-once, the same delivery guarantee as everywhere
// else in this project.
func (s *FileSink) RunUntilError(ctx context.Context) error {
	if err := os.MkdirAll(s.cfg.Dir, 0o755); err != nil {
		return &setupError{fmt.Errorf("file sink mkdir: %w", err)}
	}
	if err := s.roll(); err != nil {
		return &setupError{err}
	}
	defer s.close()

	for {
		if ctx.Err() != nil {
			return nil
		}
		events := drainBatch(ctx, s.channel, s.cfg.BatchSize, s.cfg.BatchLinger)
		if len(events) == 0 {
			continue
		}
		if err := s.writeBatch(events); err != nil {
			s.requeue(events)
			return err
		}
		if s.metrics != nil {
			s.metrics.Counter("logflow_events_forwarded_total").Add(float64(len(events)))
		}
		if s.written >= s.cfg.RollMaxBytes || time.Since(s.openedAt) >= s.cfg.RollInterval {
			if err := s.roll(); err != nil {
				log.Printf("[sink:%s] roll failed: %v", s.cfg.Name, err)
			}
		}
	}
}

func (s *FileSink) writeBatch(events []*channel.Event) error {
	for _, e := range events {
		n, err := s.w.Write(e.Body)
		if err != nil {
			return err
		}
		if err := s.w.WriteByte('\n'); err != nil {
			return err
		}
		s.written += int64(n) + 1
	}
	return s.w.Flush()
}

// requeue hands a failed batch back to the channel. Uses its own bounded
// context (not the sink's) so a shutdown racing a write error still gets
// the events back in instead of dropping them.
func (s *FileSink) requeue(events []*channel.Event) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lost := 0
	for _, e := range events {
		if err := s.channel.Put(ctx, e); err != nil {
			lost++
		}
	}
	if lost > 0 {
		log.Printf("[sink:%s] could not return %d of %d event(s) to the channel after a write failure - lost", s.cfg.Name, lost, len(events))
	}
}

func (s *FileSink) roll() error {
	s.close()
	path := filepath.Join(s.cfg.Dir, fmt.Sprintf("%s-%d.log", s.cfg.Name, time.Now().UnixNano()))
	open := s.openFile
	if open == nil {
		open = func(p string) (*os.File, error) {
			return os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		}
	}
	f, err := open(path)
	if err != nil {
		return fmt.Errorf("open sink file: %w", err)
	}
	s.f = f
	s.w = bufio.NewWriterSize(f, 64<<10)
	s.written = 0
	s.openedAt = time.Now()
	return nil
}

func (s *FileSink) close() {
	if s.w != nil {
		s.w.Flush()
	}
	if s.f != nil {
		s.f.Close()
	}
}

// StdoutSink and NullSink are simple sinks useful for local testing:
// StdoutSink prints each event body, NullSink discards everything while
// still advancing channel stats.
type StdoutSink struct {
	Name        string
	Channel     channel.Channel
	BatchSize   int
	BatchLinger time.Duration
	Metrics     *metrics.Registry
}

func (s *StdoutSink) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		events := drainBatch(ctx, s.Channel, s.BatchSize, s.BatchLinger)
		for _, e := range events {
			fmt.Printf("[%s] %s\n", s.Name, string(e.Body))
		}
		if len(events) > 0 && s.Metrics != nil {
			s.Metrics.Counter("logflow_events_forwarded_total").Add(float64(len(events)))
		}
	}
	return nil
}

type NullSink struct {
	Name        string
	Channel     channel.Channel
	BatchSize   int
	BatchLinger time.Duration
	Metrics     *metrics.Registry
}

func (s *NullSink) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		events := drainBatch(ctx, s.Channel, s.BatchSize, s.BatchLinger)
		if len(events) > 0 && s.Metrics != nil {
			s.Metrics.Counter("logflow_events_forwarded_total").Add(float64(len(events)))
		}
	}
	return nil
}
