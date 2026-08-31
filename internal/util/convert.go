package util

import (
	"bytes"
	"compress/gzip"
	"io"
	"time"

	"logflow/gen/logflowpb"
	"logflow/internal/channel"
)

func ToPBEvent(e *channel.Event) *logflowpb.Event {
	return &logflowpb.Event{
		Body:              e.Body,
		Headers:           e.Headers,
		TimestampUnixNano: e.Timestamp.UnixNano(),
	}
}

func FromPBEvent(e *logflowpb.Event) *channel.Event {
	return &channel.Event{
		Body:      e.GetBody(),
		Headers:   e.GetHeaders(),
		Timestamp: time.Unix(0, e.GetTimestampUnixNano()),
	}
}

func GzipBytes(b []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func GunzipBytes(b []byte) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	return io.ReadAll(zr)
}
