package source

import (
	"bufio"
	"context"
	"io"
	"log"
	"os"
	"time"

	"logflow/internal/channel"
	"logflow/internal/interceptor"
	"logflow/internal/metrics"
	"logflow/internal/router"
)

// FileTailSource follows a file the way `tail -F` does: it reads new lines
// as they are appended and re-opens the file if it is rotated (truncated or
// replaced), turning each line into one Event.
type FileTailSource struct {
	Name         string
	Path         string
	Router       router.Putter
	Interceptors interceptor.Chain
	PollInterval time.Duration
	Metrics      *metrics.Registry
}

func (f *FileTailSource) Run(ctx context.Context) error {
	log.Printf("[source:%s] tailing %s", f.Name, f.Path)
	var (
		file   *os.File
		reader *bufio.Reader
		offset int64
	)
	open := func() error {
		var err error
		file, err = os.Open(f.Path)
		if err != nil {
			return err
		}
		reader = bufio.NewReader(file)
		offset = 0
		return nil
	}

	// Wait for the file to exist rather than failing hard, since it's common
	// for the target application to create it after the agent starts.
	for {
		if err := open(); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(f.PollInterval):
		}
	}
	defer file.Close()

	ticker := time.NewTicker(f.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			for {
				line, err := reader.ReadString('\n')
				if len(line) > 0 {
					n := len(line)
					if line[n-1] == '\n' {
						line = line[:n-1]
					}
					e := &channel.Event{
						Body:      []byte(line),
						Headers:   map[string]string{"source": f.Name, "path": f.Path},
						Timestamp: time.Now(),
					}
					if f.Interceptors != nil {
						e = f.Interceptors.Intercept(e)
					}
					if e == nil {
						// dropped by an interceptor
					} else if perr := f.Router.Put(ctx, e); perr != nil {
						log.Printf("[source:%s] drop line, channel put failed: %v", f.Name, perr)
					} else if f.Metrics != nil {
						f.Metrics.Counter("logflow_events_received_total").Inc()
					}
					offset += int64(len(line)) + 1
				}
				if err != nil {
					if err != io.EOF {
						log.Printf("[source:%s] read error: %v", f.Name, err)
					}
					break
				}
			}
			if fi, statErr := os.Stat(f.Path); statErr == nil {
				if curOff, _ := file.Seek(0, io.SeekCurrent); fi.Size() < curOff {
					// File was truncated/rotated; reopen from the start.
					file.Close()
					if err := open(); err != nil {
						log.Printf("[source:%s] reopen failed: %v", f.Name, err)
					}
				}
			}
		}
	}
}
