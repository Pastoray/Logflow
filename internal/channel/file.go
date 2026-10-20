package channel

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// FileChannel is a durable, write-ahead-log backed channel. Events are
// appended to an on-disk segment file before being made visible to
// consumers, so buffered-but-not-yet-forwarded events survive a process
// restart or crash. This trades some throughput (disk I/O, periodic fsync)
// for the durability that a pure in-memory channel cannot offer - the same
// trade Flume's file channel makes over its memory channel.
//
// Every entry is stored with a CRC32 checksum. On recovery, the first
// entry whose checksum doesn't verify (typically a torn write left by a
// crash mid-append) stops replay of that segment rather than risking a
// misparsed length prefix cascading into garbage for every entry after it.
//
// Delivery semantics are at-least-once: a segment file is only deleted once
// every event written to it has been taken by a consumer, so a crash between
// "taken" and "fully drained" can cause replay of already-delivered events on
// restart.
//
// Segment cleanup happens two ways. The common path is reactive: maybeGC
// deletes a segment the moment its last entry is taken, as part of normal
// consumption (see Take). The backstop is an active background sweep
// (retentionLoop) enforcing an optional RetentionPolicy - a time and/or
// total-size limit independent of whether consumers have caught up. That
// sweep can delete a segment that still has unconsumed entries once it's
// past the policy's limit, intentionally discarding that data the same way
// Kafka's retention.ms/retention.bytes do: retention limits describe how
// long you're willing to keep data at all, separate from whether it's been
// delivered yet. Purged-but-still-queued tokens are handled safely by
// Take/readAt's existing missing-segment error path (counted as a drop).
type FileChannel struct {
	name       string
	dir        string
	capacity   int
	maxSegment int64
	retention  RetentionPolicy

	mu        sync.Mutex
	writeSeg  *segment
	nextSegID int64
	readSegs  map[int64]*segment // open segments available for reading, keyed by id

	ready   chan token // durable-but-unconsumed event locations, replayed on startup
	closed  int32
	closeCh chan struct{}

	flushInterval time.Duration
	stopFlush     chan struct{}
	stopRetention chan struct{}

	purgedUnconsumed int64 // events discarded by retention while still unconsumed
	counters
}

// RetentionPolicy bounds how long WAL segments are kept, independent of
// consumption. Both fields default to 0 (disabled) - retention is opt-in,
// consistent with this project's "durable by default, extra limits only
// when asked for" stance. MaxAge and MaxBytes can be combined; either one
// firing is enough to purge a segment.
type RetentionPolicy struct {
	MaxAge   time.Duration // segments older than this are purged; 0 = no age limit
	MaxBytes int64         // total WAL size cap across closed segments; 0 = no size limit
	Interval time.Duration // how often to sweep; defaults to 30s if either limit is set
}

func (r RetentionPolicy) enabled() bool { return r.MaxAge > 0 || r.MaxBytes > 0 }

type token struct {
	segID  int64
	offset int64
}

type segment struct {
	id       int64
	path     string
	f        *os.File      // write handle (O_APPEND); only used while this is the active write segment
	w        *bufio.Writer // buffers writes to f
	rf       *os.File      // read-only handle, present for the segment's whole lifetime
	mu       sync.Mutex
	written  int64 // entries written
	consumed int64 // entries taken (for GC)
}

func NewFileChannel(name, dir string, capacity int, maxSegmentBytes int64, retention RetentionPolicy) (*FileChannel, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("file channel mkdir: %w", err)
	}
	fc := &FileChannel{
		name:          name,
		dir:           dir,
		capacity:      capacity,
		maxSegment:    maxSegmentBytes,
		retention:     retention,
		readSegs:      make(map[int64]*segment),
		ready:         make(chan token, capacity),
		closeCh:       make(chan struct{}),
		flushInterval: 200 * time.Millisecond,
		stopFlush:     make(chan struct{}),
		stopRetention: make(chan struct{}),
	}
	if err := fc.recover(); err != nil {
		return nil, err
	}
	if err := fc.openNewWriteSegment(); err != nil {
		return nil, err
	}
	go fc.flushLoop()
	go fc.retentionLoop()
	return fc, nil
}

// recover scans existing segment files (left behind by an unclean shutdown,
// since fully-drained segments are deleted during normal operation) and
// replays their entries into the ready queue. Each entry's checksum is
// verified; the first invalid or truncated entry in a segment is treated
// as the torn tail of an in-progress write interrupted by the crash, and
// replay of that segment stops there rather than risk reading garbage.
func (fc *FileChannel) recover() error {
	entries, err := os.ReadDir(fc.dir)
	if err != nil {
		return fmt.Errorf("file channel recover readdir: %w", err)
	}
	var ids []int64
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "seg-") {
			continue
		}
		idStr := strings.TrimSuffix(strings.TrimPrefix(e.Name(), "seg-"), ".log")
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	for _, id := range ids {
		path := filepath.Join(fc.dir, segName(id))
		f, err := os.OpenFile(path, os.O_RDONLY, 0o644)
		if err != nil {
			return fmt.Errorf("file channel reopen segment %d: %w", id, err)
		}
		seg := &segment{id: id, path: path}
		var offset int64
		for {
			payload, err := readAndVerifyEntry(f)
			if err != nil {
				if err != io.EOF {
					log.Printf("[channel:%s] segment %d: stopping replay at offset %d (%v) - likely a torn write from an unclean shutdown, or on-disk corruption",
						fc.name, id, offset, err)
				}
				break
			}
			fc.ready <- token{segID: id, offset: offset}
			atomic.AddInt64(&seg.written, 1)
			fc.onPut()
			offset += int64(4 + len(payload) + 4)
		}
		f.Close()
		if id+1 > fc.nextSegID {
			fc.nextSegID = id + 1
		}
		// Reopen for reads used by Take(); keep for GC bookkeeping.
		rf, err := os.Open(path)
		if err == nil {
			seg.rf = rf
			fc.readSegs[id] = seg
		}
	}
	return nil
}

// maxEntrySize bounds how large a single encoded event's length prefix is
// allowed to claim to be. Without this, a corrupted or torn length prefix
// (a few flipped bits turning "200 bytes" into "4 billion bytes") would be
// used directly to size an allocation, crashing the process with an
// out-of-memory error instead of being caught as the corruption it is.
const maxEntrySize = 64 << 20 // 64MiB - comfortably above any real event

// errCorruptEntry marks an entry whose stored checksum doesn't match its
// payload, or whose length prefix is implausibly large to be genuine -
// either genuine on-disk corruption (bit rot, bad sectors) or, far more
// commonly, a torn write: the process crashed after this entry's length
// prefix was flushed but before its payload and checksum were.
var errCorruptEntry = fmt.Errorf("file channel: corrupt entry (checksum mismatch or implausible length)")

// readAndVerifyEntry reads one [length][payload][crc32] record from r
// starting at its current position and returns the payload, having
// verified the checksum. It returns io.EOF for a clean end of stream and
// io.ErrUnexpectedEOF or errCorruptEntry for a torn/corrupt record.
func readAndVerifyEntry(r io.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err // io.EOF (clean) or io.ErrUnexpectedEOF (torn)
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if n > maxEntrySize {
		// This length prefix cannot be genuine - treat it as corruption
		// rather than trusting it to size an allocation.
		return nil, errCorruptEntry
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, io.ErrUnexpectedEOF
	}
	var crcBuf [4]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		return nil, io.ErrUnexpectedEOF
	}
	if binary.BigEndian.Uint32(crcBuf[:]) != crc32.ChecksumIEEE(payload) {
		return nil, errCorruptEntry
	}
	return payload, nil
}

func segName(id int64) string { return fmt.Sprintf("seg-%020d.log", id) }

func (fc *FileChannel) openNewWriteSegment() error {
	id := atomic.AddInt64(&fc.nextSegID, 1) - 1
	path := filepath.Join(fc.dir, segName(id))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open segment: %w", err)
	}
	rf, err := os.Open(path)
	if err != nil {
		f.Close()
		return fmt.Errorf("open segment for read: %w", err)
	}
	// One shared struct serves as both fc.writeSeg and fc.readSegs[id]
	// while this segment is active, so `written`/`consumed` bookkeeping
	// is visible and consistent from both the write path (Put) and the
	// read path (Take/maybeGC/retention) - using two separate structs
	// here previously meant the read side's `written` counter never
	// advanced, causing maybeGC to delete a still-mostly-unconsumed
	// segment after its very first Take().
	seg := &segment{id: id, path: path, f: f, w: bufio.NewWriterSize(f, 64<<10), rf: rf}
	fc.mu.Lock()
	fc.writeSeg = seg
	fc.readSegs[id] = seg
	fc.mu.Unlock()
	return nil
}

func (fc *FileChannel) flushLoop() {
	t := time.NewTicker(fc.flushInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			// Grab a reference to the current write segment just long
			// enough to read the pointer, then release fc.mu before doing
			// any I/O. fsync can be slow and unpredictable depending on
			// the underlying storage; holding the channel-wide lock for
			// its whole duration would stall every concurrent Put() (which
			// also needs fc.mu) for that same window, turning one slow
			// fsync into a system-wide stall. Only seg.mu - which Put()
			// only holds for the brief duration of appending to the
			// buffer, not for this fsync - needs to serialize with this.
			fc.mu.Lock()
			seg := fc.writeSeg
			fc.mu.Unlock()
			if seg != nil {
				seg.mu.Lock()
				seg.w.Flush()
				seg.f.Sync()
				seg.mu.Unlock()
			}
		case <-fc.stopFlush:
			return
		}
	}
}

func (fc *FileChannel) Name() string { return fc.name }

// encodeBufPool holds reusable scratch buffers for encodeEvent, avoiding a
// fresh heap allocation on every single Put() call. Buffers are returned to
// the pool once bufio.Writer has copied their contents into its own
// internal buffer - after that point nothing needs the scratch buffer's
// specific contents anymore, so recycling it is safe.
var encodeBufPool = sync.Pool{
	New: func() any { b := make([]byte, 0, 256); return &b },
}

// encodeEvent serializes e into a pooled scratch buffer:
// [bodyLen u32][body][headerCount u16][k/v pairs][timestamp i64]
// Callers must return the buffer to the pool via releaseEncodeBuf once
// they're done with it (immediately after copying it elsewhere - e.g.
// into a bufio.Writer - not after any async use).
func encodeEvent(e *Event) []byte {
	bp := encodeBufPool.Get().(*[]byte)
	buf := (*bp)[:0]
	var tmp [8]byte

	binary.BigEndian.PutUint32(tmp[:4], uint32(len(e.Body)))
	buf = append(buf, tmp[:4]...)
	buf = append(buf, e.Body...)

	binary.BigEndian.PutUint16(tmp[:2], uint16(len(e.Headers)))
	buf = append(buf, tmp[:2]...)
	for k, v := range e.Headers {
		binary.BigEndian.PutUint16(tmp[:2], uint16(len(k)))
		buf = append(buf, tmp[:2]...)
		buf = append(buf, k...)
		binary.BigEndian.PutUint16(tmp[:2], uint16(len(v)))
		buf = append(buf, tmp[:2]...)
		buf = append(buf, v...)
	}

	binary.BigEndian.PutUint64(tmp[:8], uint64(e.Timestamp.UnixNano()))
	buf = append(buf, tmp[:8]...)

	*bp = buf
	return buf
}

func releaseEncodeBuf(buf []byte) {
	encodeBufPool.Put(&buf)
}

// decodeEvent parses buf in place, aliasing its body bytes directly into
// the returned Event rather than copying them into a new allocation. This
// is safe because buf is always a freshly-read, single-use buffer (from
// readAndVerifyEntry) that nothing else holds a reference to - the Event
// becomes its sole owner.
func decodeEvent(buf []byte) *Event {
	off := 0
	bodyLen := binary.BigEndian.Uint32(buf[off:])
	off += 4
	body := buf[off : off+int(bodyLen)] // zero-copy: alias, don't duplicate
	off += int(bodyLen)
	hCount := binary.BigEndian.Uint16(buf[off:])
	off += 2
	headers := make(map[string]string, hCount)
	for i := 0; i < int(hCount); i++ {
		kLen := binary.BigEndian.Uint16(buf[off:])
		off += 2
		k := string(buf[off : off+int(kLen)])
		off += int(kLen)
		vLen := binary.BigEndian.Uint16(buf[off:])
		off += 2
		v := string(buf[off : off+int(vLen)])
		off += int(vLen)
		headers[k] = v
	}
	ts := int64(binary.BigEndian.Uint64(buf[off:]))
	return &Event{Body: body, Headers: headers, Timestamp: time.Unix(0, ts)}
}

func (fc *FileChannel) Put(ctx context.Context, e *Event) error {
	if atomic.LoadInt32(&fc.closed) == 1 {
		return ErrClosed
	}
	payload := encodeEvent(e)
	defer releaseEncodeBuf(payload)

	// Only fc.mu (the channel-wide lock) is needed to safely read the
	// current write segment pointer; the actual write and its flush use
	// only that segment's own lock, so a slow write to one segment can't
	// stall an unrelated openNewWriteSegment/flushLoop/Close call that
	// only needs fc.mu briefly, and vice versa.
	fc.mu.Lock()
	seg := fc.writeSeg
	fc.mu.Unlock()
	if seg == nil || seg.f == nil {
		return fmt.Errorf("file channel: no active write segment")
	}

	seg.mu.Lock()
	offset, err := seg.f.Seek(0, io.SeekCurrent)
	// account for buffered-but-unflushed bytes so offsets stay consistent
	offset += int64(seg.w.Buffered())
	if err == nil {
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
		if _, err = seg.w.Write(lenBuf[:]); err == nil {
			_, err = seg.w.Write(payload)
		}
	}
	if err == nil {
		var crcBuf [4]byte
		binary.BigEndian.PutUint32(crcBuf[:], crc32.ChecksumIEEE(payload))
		_, err = seg.w.Write(crcBuf[:])
	}
	if err == nil {
		// Flush (write(2) to the OS page cache, not fsync) immediately so
		// the separate read file descriptor used by Take() can see these
		// bytes right away. Durability-grade fsync still happens on the
		// periodic flushLoop timer / Close(), which is the more expensive
		// operation; this is just making the write visible to readers.
		err = seg.w.Flush()
	}
	var rollNeeded bool
	if err == nil {
		atomic.AddInt64(&seg.written, 1)
		curSize, _ := seg.f.Seek(0, io.SeekCurrent)
		rollNeeded = curSize >= fc.maxSegment
	}
	segID := seg.id
	seg.mu.Unlock()

	if err != nil {
		return fmt.Errorf("file channel write: %w", err)
	}
	if rollNeeded {
		seg.mu.Lock()
		seg.w.Flush()
		seg.f.Sync()
		seg.mu.Unlock()
		_ = fc.openNewWriteSegment()
	}

	tok := token{segID: segID, offset: offset}
	select {
	case fc.ready <- tok:
		fc.onPut()
		return nil
	case <-ctx.Done():
		fc.onDrop()
		return ctx.Err()
	case <-fc.closeCh:
		return ErrClosed
	}
}

func (fc *FileChannel) Take(ctx context.Context) (*Event, error) {
	for {
		select {
		case tok, ok := <-fc.ready:
			if !ok {
				return nil, ErrClosed
			}
			if e, ok := fc.consume(tok); ok {
				return e, nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-fc.closeCh:
			return nil, ErrClosed
		}
	}
}

// consume reads the event a ready token points at and does the take-side
// accounting. ok=false means the slot was unreadable and has been counted
// as a drop - the caller should just look for the next token.
func (fc *FileChannel) consume(tok token) (*Event, bool) {
	e, err := fc.readAt(tok)
	if err != nil {
		// Should not happen now that Put() flushes before making a
		// token visible, but if it ever does, don't silently lose
		// the slot from the size counter - account it as a drop and
		// keep looking for the next event rather than returning an
		// error that aborts the caller's whole batch.
		fc.onTake()
		fc.onDrop()
		return nil, false
	}
	fc.onTake()
	fc.maybeGC(tok.segID)
	return e, true
}

func (fc *FileChannel) readAt(tok token) (*Event, error) {
	fc.mu.Lock()
	seg, ok := fc.readSegs[tok.segID]
	fc.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("file channel: segment %d not open for read", tok.segID)
	}
	seg.mu.Lock()
	defer seg.mu.Unlock()
	if _, err := seg.rf.Seek(tok.offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("file channel seek: %w", err)
	}
	payload, err := readAndVerifyEntry(seg.rf)
	if err != nil {
		return nil, fmt.Errorf("file channel read entry: %w", err)
	}
	return decodeEvent(payload), nil
}

// maybeGC removes a segment file once every entry written to it has been
// consumed and it is no longer the active write segment.
func (fc *FileChannel) maybeGC(segID int64) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	seg, ok := fc.readSegs[segID]
	if !ok || fc.writeSeg == nil || seg.id == fc.writeSeg.id {
		return
	}
	consumed := atomic.AddInt64(&seg.consumed, 1)
	if consumed >= atomic.LoadInt64(&seg.written) {
		seg.rf.Close()
		os.Remove(seg.path)
		delete(fc.readSegs, segID)
	}
}

// retentionLoop is the active background half of retention/purging: where
// maybeGC reactively cleans up as a side effect of normal consumption,
// this periodically sweeps for segments that have overstayed
// RetentionPolicy regardless of whether anyone has consumed them, in case
// consumers are slow, stalled, or simply never going to catch up. It's a
// no-op loop that returns immediately if no policy is configured.
func (fc *FileChannel) retentionLoop() {
	if !fc.retention.enabled() {
		return
	}
	interval := fc.retention.Interval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			fc.enforceRetention()
		case <-fc.stopRetention:
			return
		}
	}
}

// enforceRetention purges closed (non-active) segments that have exceeded
// RetentionPolicy.MaxAge, or - if the total size of closed segments
// exceeds MaxBytes - the oldest closed segments first, until back under
// the cap. A purged segment that still has unconsumed entries has that
// count added to PurgedUnconsumed and logged clearly, since this is the
// one place in the channel that can lose data that was never delivered.
func (fc *FileChannel) enforceRetention() {
	type candidate struct {
		id    int64
		seg   *segment
		mtime time.Time
		size  int64
	}

	fc.mu.Lock()
	activeID := int64(-1)
	if fc.writeSeg != nil {
		activeID = fc.writeSeg.id
	}
	candidates := make([]candidate, 0, len(fc.readSegs))
	for id, seg := range fc.readSegs {
		if id == activeID {
			continue // never purge the segment still being written to
		}
		fi, err := os.Stat(seg.path)
		if err != nil {
			continue
		}
		candidates = append(candidates, candidate{id: id, seg: seg, mtime: fi.ModTime(), size: fi.Size()})
	}
	fc.mu.Unlock()

	sort.Slice(candidates, func(i, j int) bool { return candidates[i].id < candidates[j].id }) // oldest first

	var totalSize int64
	for _, c := range candidates {
		totalSize += c.size
	}

	now := time.Now()
	for _, c := range candidates {
		var reason string
		switch {
		case fc.retention.MaxAge > 0 && now.Sub(c.mtime) > fc.retention.MaxAge:
			reason = fmt.Sprintf("older than retention_max_age (%s)", fc.retention.MaxAge)
		case fc.retention.MaxBytes > 0 && totalSize > fc.retention.MaxBytes:
			reason = fmt.Sprintf("WAL size %d exceeds retention_max_bytes %d", totalSize, fc.retention.MaxBytes)
		default:
			continue
		}

		unconsumed := atomic.LoadInt64(&c.seg.written) - atomic.LoadInt64(&c.seg.consumed)

		fc.mu.Lock()
		if s, ok := fc.readSegs[c.id]; ok {
			s.rf.Close()
			delete(fc.readSegs, c.id)
		}
		fc.mu.Unlock()
		os.Remove(c.seg.path)
		totalSize -= c.size

		if unconsumed > 0 {
			atomic.AddInt64(&fc.purgedUnconsumed, unconsumed)
			log.Printf("[channel:%s] retention purge: removed segment %d (%s), discarding %d unconsumed event(s)",
				fc.name, c.id, reason, unconsumed)
		} else {
			log.Printf("[channel:%s] retention purge: removed fully-consumed segment %d (%s)", fc.name, c.id, reason)
		}
	}
}

// PurgedUnconsumed reports how many events have been discarded by a
// retention purge while still unconsumed - a signal that consumers aren't
// keeping up with the configured retention window/size, distinct from the
// generic dropped-event counter (every retention-purged event is also
// counted there, since it is a drop; this narrows down why).
func (fc *FileChannel) PurgedUnconsumed() int64 { return atomic.LoadInt64(&fc.purgedUnconsumed) }

func (fc *FileChannel) Stats() Stats {
	return fc.counters.snapshot(int64(fc.capacity))
}

func (fc *FileChannel) Close() error {
	if atomic.CompareAndSwapInt32(&fc.closed, 0, 1) {
		close(fc.stopFlush)
		close(fc.stopRetention)
		close(fc.closeCh)
		fc.mu.Lock()
		defer fc.mu.Unlock()
		if fc.writeSeg != nil {
			fc.writeSeg.w.Flush()
			fc.writeSeg.f.Sync()
			fc.writeSeg.f.Close()
		}
		for _, s := range fc.readSegs {
			s.rf.Close()
		}
	}
	return nil
}
