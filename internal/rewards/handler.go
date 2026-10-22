// Package rewards turns payment events into points ledger entries.
package rewards

import (
	"context"
	"errors"
	"log/slog"

	"github.com/rentpoints/pointsd/internal/events"
	"github.com/rentpoints/pointsd/internal/ledger"
	"github.com/rentpoints/pointsd/internal/pubsub"
)

// ErrUnknownPayment is returned for a reversal of a payment that was never awarded.
var ErrUnknownPayment = errors.New("rewards: reversal for unknown payment")

type Handler struct {
	store      ledger.Store
	policy     Policy
	deadLetter pubsub.Publisher
	log        *slog.Logger
}

func NewHandler(store ledger.Store, policy Policy, deadLetter pubsub.Publisher, log *slog.Logger) *Handler {
	return &Handler{store: store, policy: policy, deadLetter: deadLetter, log: log}
}

// Handle processes one delivery. The subscriber calls it concurrently
// (MaxOutstandingMessages per replica), and pointsd runs several replicas.
func (h *Handler) Handle(ctx context.Context, msg pubsub.Message) {
	ev, err := events.Decode(msg.Data())
	if err != nil {
		h.log.Warn("dead-lettering invalid event", "msg_id", msg.ID(), "err", err)
		if err := h.deadLetter.Publish(ctx, msg.Data(), map[string]string{"reason": err.Error()}); err != nil {
			h.log.Error("dead-letter publish failed", "msg_id", msg.ID(), "err", err)
			msg.Nack()
			return
		}
		msg.Ack()
		return
	}

	switch err := h.process(ctx, ev); {
	case err == nil:
		msg.Ack()
	case errors.Is(err, ErrUnknownPayment):
		h.log.Warn("reversal for payment that earned no points", "payment_id", ev.PaymentID, "member_id", ev.MemberID, "event_id", ev.EventID)
		msg.Ack()
	default:
		h.log.Error("processing failed, nacking", "payment_id", ev.PaymentID, "type", ev.Type, "attempt", msg.DeliveryAttempt(), "err", err)
		msg.Nack()
	}
}

func (h *Handler) process(ctx context.Context, ev events.Payment) error {
	switch ev.Type {
	case events.TypeSettled:
		return h.award(ctx, ev)
	case events.TypeReversed:
		return h.reverse(ctx, ev)
	}
	return nil
}

func (h *Handler) award(ctx context.Context, ev events.Payment) error {
	_, _, err := h.store.Award(ctx, ev.MemberID, ev.PaymentID, Month(ev.OccurredAt), h.policy.Points(ev.AmountCents), h.policy.MonthlyCap)
	return err
}

func (h *Handler) reverse(ctx context.Context, ev events.Payment) error {
	removed, dup, err := h.store.Reverse(ctx, ev.MemberID, ev.PaymentID)
	switch {
	case errors.Is(err, ledger.ErrNoAward):
		return ErrUnknownPayment
	case err == nil && !dup && removed == 0:
		return ErrUnknownPayment
	}
	return err
}
