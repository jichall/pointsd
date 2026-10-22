// Command pointsd consumes payment events and serves member points balances.
//
// With PUBSUB_EMULATOR_HOST and PUBSUB_PROJECT_ID set it consumes PUBSUB_SUBSCRIPTION (default
// payments-events) and dead-letters invalid payloads to PUBSUB_DEAD_LETTER_TOPIC
// (default payments-events-dead-letter). PUBSUB_EMULATOR_HOST points the client
// at a local emulator.
//
// POINTSD_LOG_FILE additionally appends the logs to that file.
//
// Local mode replays newline-delimited events from a file instead of a
// Pub/Sub subscription:
//
//	go run ./cmd/pointsd -events testdata/events.jsonl
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/rentpoints/pointsd/internal/api"
	"github.com/rentpoints/pointsd/internal/ledger"
	"github.com/rentpoints/pointsd/internal/pubsub"
	"github.com/rentpoints/pointsd/internal/rewards"
)

type fileMessage struct {
	id   string
	data []byte
}

func (m fileMessage) ID() string           { return m.id }
func (m fileMessage) Data() []byte         { return m.data }
func (m fileMessage) DeliveryAttempt() int { return 1 }
func (m fileMessage) Ack()                 {}
func (m fileMessage) Nack()                {}

type logPublisher struct{ log *slog.Logger }

func (p logPublisher) Publish(_ context.Context, data []byte, attrs map[string]string) error {
	p.log.Warn("dead letter", "reason", attrs["reason"], "data", string(data))
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// maxOutstanding matches the production per-replica setting.
const maxOutstanding = 10

func main() {
	eventsPath := flag.String("events", "", "replay events from this JSONL file")
	addr := flag.String("addr", ":8080", "HTTP listen address")
	flag.Parse()

	var logOut io.Writer = os.Stderr
	if path := os.Getenv("POINTSD_LOG_FILE"); path != "" {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			fmt.Fprintln(os.Stderr, "open log file:", err)
			os.Exit(1)
		}
		defer f.Close()
		logOut = io.MultiWriter(os.Stderr, f)
	}

	log := slog.New(slog.NewTextHandler(logOut, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	defer stop()

	store := ledger.NewMemoryStore()

	var dlq pubsub.Publisher = logPublisher{log}
	var client *pubsub.Client
	if project, host := os.Getenv("PUBSUB_PROJECT_ID"), os.Getenv("PUBSUB_EMULATOR_HOST"); project != "" && host != "" {
		client = pubsub.NewEmulatorClient(host, project)
		dlq = client.NewPublisher(envOr("PUBSUB_DEAD_LETTER_TOPIC", "payments-events-dead-letter"))
	} else {
		log.Warn("not consuming from Pub/Sub: set PUBSUB_EMULATOR_HOST and PUBSUB_PROJECT_ID")
	}

	handler := rewards.NewHandler(store, rewards.DefaultPolicy, dlq, log)

	if *eventsPath != "" {
		f, err := os.Open(*eventsPath)
		if err != nil {
			log.Error("open events", "err", err)
			os.Exit(1)
		}
		sc := bufio.NewScanner(f)
		for n := 1; sc.Scan(); n++ {
			handler.Handle(ctx, fileMessage{id: strconv.Itoa(n), data: append([]byte(nil), sc.Bytes()...)})
		}
		f.Close()
	}

	srv := &http.Server{Addr: *addr, Handler: api.NewServer(store)}
	errc := make(chan error, 2)
	go func() {
		log.Info("listening", "addr", *addr)
		errc <- srv.ListenAndServe()
	}()
	if client != nil {
		sub := envOr("PUBSUB_SUBSCRIPTION", "payments-events")
		go func() {
			log.Info("consuming", "subscription", sub)
			errc <- client.Receive(ctx, sub, maxOutstanding, handler.Handle)
		}()
	}

	var err error
	select {
	case <-ctx.Done():
	case err = <-errc:
		if ctx.Err() != nil {
			err = nil
		}
	}

	stop()

	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_ = srv.Shutdown(shutdown)
	if err != nil && err != http.ErrServerClosed {
		log.Error("stopped", "err", err)
		os.Exit(1)
	}
}
