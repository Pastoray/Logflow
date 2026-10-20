// Command agent runs a single logflow agent process from a YAML config
// file describing its sources, channels, and sinks.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"logflow/internal/agent"
	"logflow/internal/config"
)

func main() {
	cfgPath := flag.String("config", "configs/edge-agent.yaml", "path to agent YAML config")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// First SIGINT/SIGTERM starts a graceful, controlled drain (see
	// Agent.Run): ingestion stops immediately, but sinks keep flushing
	// whatever is already buffered until channels are empty or
	// drain_timeout elapses. A second signal during that drain forces an
	// immediate stop instead of waiting it out - the conventional
	// "I understand this may lose buffered data, stop now" escape hatch.
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	force := make(chan struct{})
	go func() {
		<-sigCh
		log.Printf("shutdown signal received: draining gracefully (send another signal to force an immediate quit)")
		cancel()
		<-sigCh
		log.Printf("second shutdown signal received: forcing an immediate quit")
		close(force)
	}()

	ag, err := agent.New(cfg)
	if err != nil {
		log.Fatalf("build agent: %v", err)
	}
	if err := ag.Run(ctx, force); err != nil {
		log.Fatalf("agent run: %v", err)
	}
}
