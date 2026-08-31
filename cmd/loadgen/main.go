package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
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

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	stream, err := client.StreamEvents(ctx)
	if err != nil {
		log.Fatalf("stream: %v", err)
	}

	go func() {
		for {
			ack, err := stream.Recv()
			if err != nil {
				return
			}
			if !ack.GetSuccess() {
				log.Printf("nack: %s", ack.GetError())
			}
		}
	}()

	interval := time.Second / time.Duration(*rate/(*batchSize))
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	sent := 0
	start := time.Now()
	batchNum := 0
	for {
		select {
		case <-ctx.Done():
			stream.CloseSend()
			elapsed := time.Since(start).Seconds()
			fmt.Printf("sent %d events in %.2fs (%.0f events/sec)\n", sent, elapsed, float64(sent)/elapsed)
			return
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
				return
			}
			sent += len(events)
		}
	}
}
