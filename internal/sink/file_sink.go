package sink

import (
	"bufio"
	"context"
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

type FileSink struct {
	cfg     FileSinkConfig
	channel channel.Channel
	metrics *metrics.Registry

	f        *os.File
	w        *bufio.Writer
	written  int64
	openedAt time.Time
}

func NewFileSink(cfg FileSinkConfig, ch channel.Channel, reg *metrics.Registry) *FileSink {
	return &FileSink{cfg: cfg, channel: ch, metrics: reg}
}

func (s *FileSink) Run(ctx context.Context) error {
	if err := os.MkdirAll(s.cfg.Dir, 0o755); err != nil {
		return fmt.Errorf("file sink mkdir: %w", err)
	}
	if err := s.roll(); err != nil {
		return err
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
		for _, e := range events {
			n, _ := s.w.Write(e.Body)
			s.w.WriteByte('\n')
			s.written += int64(n) + 1
		}
		s.w.Flush()
		if s.metrics != nil {
			s.metrics.Counter("logflow_events_forwarded_total").Add(int64(len(events)))
		}
		if s.written >= s.cfg.RollMaxBytes || time.Since(s.openedAt) >= s.cfg.RollInterval {
			if err := s.roll(); err != nil {
				log.Printf("[sink:%s] roll failed: %v", s.cfg.Name, err)
			}
		}
	}
}

func (s *FileSink) roll() error {
	s.close()
	path := filepath.Join(s.cfg.Dir, fmt.Sprintf("%s-%d.log", s.cfg.Name, time.Now().UnixNano()))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
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
			s.Metrics.Counter("logflow_events_forwarded_total").Add(int64(len(events)))
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
			s.Metrics.Counter("logflow_events_forwarded_total").Add(int64(len(events)))
		}
	}
	return nil
}
