// Command loadgen streams synthetic log events into a logflow agent's gRPC
// source, useful for exercising and benchmarking a running topology.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"logflow/gen/logflowpb"
	"logflow/internal/config"
	"logflow/internal/security"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:6000", "target agent gRPC address")
	rate := flag.Int("rate", 10000, "events per second")
	batchSize := flag.Int("batch", 500, "events per batch")
	duration := flag.Duration("duration", 30*time.Second, "how long to run")
	ackGrace := flag.Duration("ack-grace", 10*time.Second, "how long to wait for trailing acks after the last send, before giving up on them")

	tlsEnabled := flag.Bool("tls", false, "connect over TLS")
	caFile := flag.String("cacert", "", "CA bundle to verify the server certificate")
	certFile := flag.String("cert", "", "client certificate (for mutual TLS)")
	keyFile := flag.String("key", "", "client private key (for mutual TLS)")
	serverName := flag.String("server-name", "", "override server name for TLS verification")
	insecureSkipVerify := flag.Bool("insecure-skip-verify", false, "skip server certificate verification (testing only)")
	token := flag.String("token", "", "bearer token to present to the server")
	flag.Parse()

	tlsCfg := &config.TLSConfig{
		Enabled:            *tlsEnabled,
		CAFile:             *caFile,
		CertFile:           *certFile,
		KeyFile:            *keyFile,
		ServerName:         *serverName,
		InsecureSkipVerify: *insecureSkipVerify,
	}
	creds, err := security.ClientTransportCredentials(tlsCfg)
	if err != nil {
		log.Fatalf("tls: %v", err)
	}
	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}
	if *token != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(security.TokenPerRPCCredentials{
			Token: *token, RequireTLS: *tlsEnabled,
		}))
	}

	conn, err := grpc.NewClient(*addr, dialOpts...)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	client := logflowpb.NewCollectorClient(conn)

	// The stream's own context deliberately outlives *duration - it's
	// cancelled explicitly once every acked/nacked batch has been
	// accounted for (see the ack-reader goroutine below), not the moment
	// sending stops. Tying the stream to a *duration-only context (an
	// earlier version of this tool did) tears down the connection the
	// instant the send loop ends, which can cut off acks for batches sent
	// in the final tick - undercounting real delivery, not overcounting
	// it, which is actually the more dangerous direction for a tool
	// that's sometimes read as a correctness signal rather than just a
	// load driver. ackGrace bounds how long this tool will wait for those
	// trailing acks before giving up on them.
	streamCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.StreamEvents(streamCtx)
	if err != nil {
		log.Fatalf("stream: %v", err)
	}

	// sent is the number of events this tool has handed to stream.Send -
	// touched only by the main goroutine, so it needs no synchronization.
	// acked/nacked are the real, confirmed-delivery counts: only
	// incremented here, in the single reader goroutine, from each
	// AckResponse's own EventsReceived - never assumed from Send()
	// succeeding, which only means the event reached the local gRPC send
	// buffer, not that the server processed it.
	var sent int64
	var acked, nacked int64
	ackDone := make(chan struct{})
	go func() {
		defer close(ackDone)
		for {
			ack, err := stream.Recv()
			if err != nil {
				if err != io.EOF && !errors.Is(err, context.Canceled) {
					log.Printf("ack stream error: %v", err)
				}
				return
			}
			if ack.GetSuccess() {
				atomic.AddInt64(&acked, ack.GetEventsReceived())
			} else {
				atomic.AddInt64(&nacked, ack.GetEventsReceived())
				log.Printf("nack: %s", ack.GetError())
			}
		}
	}()

	// rate < batch would make this 0 (divide-by-zero panic): send one batch
	// per second at most in that case instead.
	batchesPerSec := *rate / *batchSize
	if batchesPerSec < 1 {
		batchesPerSec = 1
	}
	interval := time.Second / time.Duration(batchesPerSec)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	stopSending := time.After(*duration)
	start := time.Now()
	batchNum := 0
sendLoop:
	for {
		select {
		case <-stopSending:
			break sendLoop
		case <-ticker.C:
			batchNum++
			events := make([]*logflowpb.Event, *batchSize)
			for i := range events {
				events[i] = &logflowpb.Event{
					Body:              []byte(fmt.Sprintf("synthetic log line %d host=web-%02d level=INFO", rand.Intn(1_000_000), rand.Intn(20))),
					Headers:           map[string]string{"env": "loadtest"},
					TimestampUnixNano: time.Now().UnixNano(),
				}
			}
			batch := &logflowpb.EventBatch{
				BatchId: fmt.Sprintf("loadgen-%d", batchNum),
				Events:  events,
			}
			if err := stream.Send(batch); err != nil {
				log.Printf("send error: %v", err)
				break sendLoop
			}
			sent += int64(len(events))
		}
	}
	elapsed := time.Since(start).Seconds()
	stream.CloseSend()

	// Give the server up to ackGrace to finish acking everything already
	// sent before reporting final numbers - this is what actually makes
	// "acked" a trustworthy confirmed-delivery count instead of just a
	// less-wrong version of the same race.
	select {
	case <-ackDone:
	case <-time.After(*ackGrace):
		log.Printf("gave up waiting for trailing acks after %s (sent=%d acked=%d so far) - server or network may be stuck", *ackGrace, sent, atomic.LoadInt64(&acked))
	}
	cancel()

	finalAcked, finalNacked := atomic.LoadInt64(&acked), atomic.LoadInt64(&nacked)
	fmt.Printf("sent=%d acked=%d nacked=%d in %.2fs (%.0f acked/sec)\n", sent, finalAcked, finalNacked, elapsed, float64(finalAcked)/elapsed)
	if unaccounted := sent - finalAcked - finalNacked; unaccounted != 0 {
		fmt.Printf("warning: %d sent event(s) got neither an ack nor a nack within %s of the last send - connection or server issue, not necessarily data loss (SendBatch-based tools get a stronger guarantee per call)\n", unaccounted, *ackGrace)
	}
}
