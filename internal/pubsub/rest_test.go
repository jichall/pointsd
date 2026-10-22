package pubsub_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rentpoints/pointsd/internal/pubsub"
)

// fakeBroker answers the subset of the Pub/Sub REST API that Client uses.
type fakeBroker struct {
	mu      sync.Mutex
	queue   []string // payloads not yet pulled
	pulled  int
	acks    chan string // ack IDs received
	nacks   chan string // ack IDs received with a zero ack deadline
	maxSeen int         // largest maxMessages requested
}

func newBroker(payloads ...string) (*fakeBroker, *pubsub.Client) {
	b := &fakeBroker{queue: payloads, acks: make(chan string, 100), nacks: make(chan string, 100)}
	srv := httptest.NewServer(b)
	return b, pubsub.NewEmulatorClient(strings.TrimPrefix(srv.URL, "http://"), "p")
}

func (b *fakeBroker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var in struct {
		MaxMessages int      `json:"maxMessages"`
		AckIDs      []string `json:"ackIds"`
		Deadline    *int     `json:"ackDeadlineSeconds"`
	}
	_ = json.NewDecoder(r.Body).Decode(&in)

	switch {
	case strings.HasSuffix(r.URL.Path, ":pull"):
		b.mu.Lock()
		defer b.mu.Unlock()
		b.maxSeen = max(b.maxSeen, in.MaxMessages)
		n := min(in.MaxMessages, len(b.queue))
		var msgs []map[string]any
		for _, p := range b.queue[:n] {
			b.pulled++
			msgs = append(msgs, map[string]any{
				"ackId":   "ack-" + p,
				"message": map[string]any{"data": []byte(p), "messageId": "id-" + p},
			})
		}
		b.queue = b.queue[n:]
		_ = json.NewEncoder(w).Encode(map[string]any{"receivedMessages": msgs})
	case strings.HasSuffix(r.URL.Path, ":acknowledge"):
		for _, id := range in.AckIDs {
			b.acks <- id
		}
		_, _ = w.Write([]byte("{}"))
	case strings.HasSuffix(r.URL.Path, ":modifyAckDeadline"):
		if in.Deadline != nil && *in.Deadline == 0 {
			for _, id := range in.AckIDs {
				b.nacks <- id
			}
		}
		_, _ = w.Write([]byte("{}"))
	}
}

// receive runs Receive in the background and returns a stop func that cancels
// it and fails the test if it does not return promptly.
func receive(t *testing.T, c *pubsub.Client, max int, h func(context.Context, pubsub.Message)) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Receive(ctx, "s", max, h) }()
	return func() {
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("Receive returned %v, want context.Canceled", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Receive did not return after cancel")
		}
	}
}

func expect(t *testing.T, ch <-chan string, want string) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %q", want)
	}
}

func TestReceive_AckAndNackReachTheBroker(t *testing.T) {
	b, c := newBroker("good", "bad")
	stop := receive(t, c, 10, func(_ context.Context, m pubsub.Message) {
		if string(m.Data()) == "bad" {
			m.Nack()
			m.Ack() // a second settle is ignored
			return
		}
		m.Ack()
	})
	defer stop()

	expect(t, b.acks, "ack-good")
	expect(t, b.nacks, "ack-bad")
	select {
	case id := <-b.acks:
		t.Fatalf("nacked message was also acked: %s", id)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestReceive_NeverHoldsMoreThanMaxOutstanding(t *testing.T) {
	const limit = 3
	b, c := newBroker("1", "2", "3", "4", "5", "6", "7", "8")
	var running, peak atomic.Int32
	release := make(chan struct{})
	stop := receive(t, c, limit, func(_ context.Context, m pubsub.Message) {
		n := running.Add(1)
		for { // record the high-water mark
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		<-release
		running.Add(-1)
		m.Ack()
	})
	defer stop()

	// Handlers are blocked: only `limit` messages may have been pulled.
	deadline := time.Now().Add(5 * time.Second)
	for running.Load() < limit && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond) // give an over-eager puller time to misbehave
	b.mu.Lock()
	pulled := b.pulled
	b.mu.Unlock()
	if pulled != limit {
		t.Fatalf("pulled %d messages while handlers were blocked, want %d", pulled, limit)
	}

	close(release)
	for i := 0; i < 8; i++ {
		select {
		case <-b.acks:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of 8 messages acked", i)
		}
	}
	if peak.Load() > limit {
		t.Fatalf("peak concurrent handlers = %d, want <= %d", peak.Load(), limit)
	}
}

func TestReceive_WaitsForRunningHandlersOnCancel(t *testing.T) {
	b, c := newBroker("slow")
	started, finish := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- c.Receive(ctx, "s", 10, func(_ context.Context, m pubsub.Message) {
			close(started)
			<-finish
			m.Ack()
		})
	}()

	<-started
	cancel()
	select {
	case <-done:
		t.Fatal("Receive returned while a handler was still running")
	case <-time.After(200 * time.Millisecond):
	}
	close(finish)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Receive did not return after the handler finished")
	}
	expect(t, b.acks, "ack-slow") // the ack still reaches the broker after cancel
}
