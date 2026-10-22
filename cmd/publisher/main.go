// Command publisher drives the integration test (scripts/integration.sh).
//
// With -setup it creates the topics and subscriptions on the broker.
// Otherwise it publishes a scenario of payment events to payments-events and
// checks the resulting balances on pointsd's API and the dead-letter topic,
// exiting non-zero on any mismatch.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/rentpoints/pointsd/internal/pubsub"
)

const (
	topic        = "payments-events"
	subscription = "payments-events"
	dlqTopic     = "payments-events-dead-letter"
	dlqSub       = "payments-events-dead-letter-check"
)

var october = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

func main() {
	setup := flag.Bool("setup", false, "create topics and subscriptions, then exit")
	pointsd := flag.String("pointsd", "http://localhost:8080", "pointsd base URL")
	timeout := flag.Duration("timeout", 30*time.Second, "how long to wait for each expectation")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx := context.Background()
	host, project := os.Getenv("PUBSUB_EMULATOR_HOST"), os.Getenv("PUBSUB_PROJECT_ID")
	if host == "" || project == "" {
		log.Error("PUBSUB_EMULATOR_HOST and PUBSUB_PROJECT_ID must be set, e.g. localhost:8085 and pointsd-local")
		os.Exit(1)
	}
	client := pubsub.NewEmulatorClient(host, project)

	var err error
	if *setup {
		err = provision(ctx, client)
	} else {
		err = run(ctx, log, client, *pointsd, *timeout)
	}
	if err != nil {
		log.Error("failed", "err", err)
		os.Exit(1)
	}
	log.Info("ok")
}

func provision(ctx context.Context, c *pubsub.Client) error {
	for _, t := range []string{topic, dlqTopic} {
		if err := c.CreateTopic(ctx, t); err != nil {
			return fmt.Errorf("create topic %s: %w", t, err)
		}
	}
	for s, t := range map[string]string{subscription: topic, dlqSub: dlqTopic} {
		if err := c.CreateSubscription(ctx, s, t); err != nil {
			return fmt.Errorf("create subscription %s: %w", s, err)
		}
	}
	return nil
}

type event struct {
	Type        string    `json:"type"`
	EventID     string    `json:"event_id"`
	PaymentID   string    `json:"payment_id"`
	MemberID    string    `json:"member_id"`
	AmountCents int64     `json:"amount_cents"`
	OccurredAt  time.Time `json:"occurred_at"`
}

func settled(eventID, payment, member string, cents int64) event {
	return event{"payment.settled", eventID, payment, member, cents, october}
}

func reversed(eventID, payment, member string, cents int64) event {
	return event{"payment.reversed", eventID, payment, member, cents, october.Add(72 * time.Hour)}
}

type step struct {
	name   string
	raw    [][]byte         // payloads to publish, concurrently
	expect map[string]int64 // balances to reach afterwards
}

func marshal(evs ...event) [][]byte {
	out := make([][]byte, len(evs))
	for i, e := range evs {
		out[i], _ = json.Marshal(e)
	}
	return out
}

func scenario() []step {
	ana := settled("evt_ana_1", "pay_ana_1", "mbr_ana", 245000)
	anaRepublished := ana
	anaRepublished.EventID = "evt_ana_1_republished"

	var burst []event
	for i := 0; i < 30; i++ {
		burst = append(burst, settled(fmt.Sprintf("evt_burst_%d", i), fmt.Sprintf("pay_burst_%d", i), "mbr_burst", 100000))
	}

	return []step{
		{
			name: "settlements, duplicates and a burst over the cap",
			raw: append(marshal(
				ana, ana, anaRepublished, // redelivery and republish: earns once
				settled("evt_kim_1", "pay_kim_1", "mbr_kim", 9999), // cents truncate
				settled("evt_raj_1", "pay_raj_1", "mbr_raj", 610000),
			), marshal(burst...)...),
			expect: map[string]int64{"mbr_ana": 2450, "mbr_kim": 99, "mbr_raj": 6100, "mbr_burst": 10000},
		},
		{
			name:   "payment cut by the monthly cap",
			raw:    marshal(settled("evt_raj_2", "pay_raj_2", "mbr_raj", 590000)),
			expect: map[string]int64{"mbr_raj": 10000},
		},
		{
			name: "reversals claw back only what was awarded; unknown payment is acked",
			raw: marshal(
				reversed("evt_raj_2_rev", "pay_raj_2", "mbr_raj", 590000),
				reversed("evt_raj_2_rev_again", "pay_raj_2", "mbr_raj", 590000),
				reversed("evt_ghost_rev", "pay_ghost", "mbr_ghost", 100000),
			),
			expect: map[string]int64{"mbr_raj": 6100, "mbr_ghost": 0},
		},
		{
			name:   "reversal did not give cap room back",
			raw:    marshal(settled("evt_raj_3", "pay_raj_3", "mbr_raj", 100000)),
			expect: map[string]int64{"mbr_raj": 6100},
		},
		{
			name:   "invalid payload is dead-lettered",
			raw:    [][]byte{[]byte(`{"type":"payment.settled","payment_id":"pay_bad"}`)},
			expect: map[string]int64{},
		},
	}
}

func run(ctx context.Context, log *slog.Logger, c *pubsub.Client, pointsd string, timeout time.Duration) error {
	for _, s := range scenario() {
		log.Info("step", "name", s.name, "messages", len(s.raw))
		if err := c.PublishBatch(ctx, topic, s.raw); err != nil {
			return fmt.Errorf("publish: %w", err)
		}
		for member, want := range s.expect {
			if err := awaitBalance(ctx, pointsd, member, want, timeout); err != nil {
				return fmt.Errorf("%s: %w", s.name, err)
			}
		}
	}
	return awaitDeadLetter(ctx, c, timeout)
}

// awaitBalance polls until the balance equals want, then keeps checking briefly
// so a late duplicate that overshoots is caught.
func awaitBalance(ctx context.Context, base, member string, want int64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var got int64 = -1
	for time.Now().Before(deadline) {
		var err error
		if got, err = balance(ctx, base, member); err == nil && got == want {
			time.Sleep(500 * time.Millisecond)
			if got, err = balance(ctx, base, member); err == nil && got == want {
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("%s balance = %d, want %d", member, got, want)
}

func balance(ctx context.Context, base, member string) (int64, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/v1/members/"+member+"/points", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var body struct {
		Points int64 `json:"points"`
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status %d", resp.StatusCode)
	}
	err = json.NewDecoder(resp.Body).Decode(&body)
	return body.Points, err
}

func awaitDeadLetter(ctx context.Context, c *pubsub.Client, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var n atomic.Int32
	err := c.Receive(ctx, dlqSub, 10, func(_ context.Context, m pubsub.Message) {
		m.Ack()
		if n.Add(1) == 1 {
			cancel()
		}
	})
	if n.Load() != 1 {
		return fmt.Errorf("dead-letter messages = %d, want 1 (receive: %v)", n.Load(), err)
	}
	return nil
}
