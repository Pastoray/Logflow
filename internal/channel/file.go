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

type FileChannel struct {
	name       string
	dir        string
	capacity   int
	maxSegment int64
	retention  RetentionPolicy

	mu        sync.Mutex
	writeSeg  *segment
	nextSegID int64
	readSegs  map[int64]*segment

	ready   chan token
	closed  int32
	closeCh chan struct{}

	flushInterval time.Duration
	stopFlush     chan struct{}
	stopRetention chan struct{}

	purgedUnconsumed int64
	counters
}

type RetentionPolicy struct {
	MaxAge   time.Duration
	MaxBytes int64
	Interval time.Duration
}

func (r RetentionPolicy) enabled() bool { return r.MaxAge > 0 || r.MaxBytes > 0 }

type token struct {
	segID  int64
	offset int64
}

type segment struct {
	id       int64
	path     string
	f        *os.File
	w        *bufio.Writer
	rf       *os.File
	mu       sync.Mutex
	written  int64
	consumed int64
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

		rf, err := os.Open(path)
		if err == nil {
			seg.rf = rf
			fc.readSegs[id] = seg
		}
	}
	return nil
}

const maxEntrySize = 64 << 20

var errCorruptEntry = fmt.Errorf("file channel: corrupt entry (checksum mismatch or implausible length)")

func readAndVerifyEntry(r io.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if n > maxEntrySize {
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

var encodeBufPool = sync.Pool{
	New: func() any { b := make([]byte, 0, 256); return &b },
}

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

func decodeEvent(buf []byte) *Event {
	off := 0
	bodyLen := binary.BigEndian.Uint32(buf[off:])
	off += 4
	body := buf[off : off+int(bodyLen)]
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

	fc.mu.Lock()
	seg := fc.writeSeg
	fc.mu.Unlock()
	if seg == nil || seg.f == nil {
		return fmt.Errorf("file channel: no active write segment")
	}

	seg.mu.Lock()
	offset, err := seg.f.Seek(0, io.SeekCurrent)

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
			e, err := fc.readAt(tok)
			if err != nil {
				fc.onTake()
				fc.onDrop()
				continue
			}
			fc.onTake()
			fc.maybeGC(tok.segID)
			return e, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-fc.closeCh:
			return nil, ErrClosed
		}
	}
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
			continue
		}
		fi, err := os.Stat(seg.path)
		if err != nil {
			continue
		}
		candidates = append(candidates, candidate{id: id, seg: seg, mtime: fi.ModTime(), size: fi.Size()})
	}
	fc.mu.Unlock()

	sort.Slice(candidates, func(i, j int) bool { return candidates[i].id < candidates[j].id })

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
