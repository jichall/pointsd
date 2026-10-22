// Package ledger stores points entries.
//
// In production the Store is backed by Postgres (table ledger_entries).
// MemoryStore is the stand-in used for local runs and tests.
package ledger

import (
	"context"
	"errors"
	"sync"
	"time"
)

type Kind string

const (
	KindAward    Kind = "award"
	KindReversal Kind = "reversal"
)

// Entry is one line of a member's points ledger.
type Entry struct {
	MemberID  string
	PaymentID string
	Kind      Kind
	Points    int64  // positive for awards, negative for reversals
	Month     string // settlement month of the payment, "2006-01" in UTC
}

// ErrUnavailable is returned when the ledger cannot be reached. Retrying may succeed.
var ErrUnavailable = errors.New("ledger: unavailable")

// ErrNoAward is returned by Reverse when the payment has no award entry.
var ErrNoAward = errors.New("ledger: no award for payment")

type Store interface {
	HasEntry(ctx context.Context, paymentID string, kind Kind) (bool, error)
	AwardedInMonth(ctx context.Context, memberID, month string) (int64, error)
	Append(ctx context.Context, e Entry) error
	Balance(ctx context.Context, memberID string) (int64, error)

	// Award appends an award entry for paymentID, capped so the member's awards
	// in month never exceed cap. It returns the points actually written. If the
	// payment already has an award entry nothing is written and dup is true.
	// Concurrent calls for the same member and payment must not over-award.
	Award(ctx context.Context, memberID, paymentID, month string, points, cap int64) (awarded int64, dup bool, err error)
	// Reverse appends a reversal entry that removes exactly the points the
	// payment's award entry holds, in the award's month. It returns the points
	// removed. If the payment was already reversed nothing is written and dup is
	// true. ErrNoAward is returned when the payment has no award entry.
	Reverse(ctx context.Context, memberID, paymentID string) (removed int64, dup bool, err error)
}

type Option func(*MemoryStore)

// WithLatency delays every call, approximating a database round trip.
func WithLatency(d time.Duration) Option {
	return func(s *MemoryStore) { s.latency = d }
}

// WithFaults installs a hook run at the start of every call (after the
// round trip). A non-nil error is returned to the caller and the call has no effect.
func WithFaults(f func(op string) error) Option {
	return func(s *MemoryStore) { s.faults = f }
}

type MemoryStore struct {
	latency time.Duration
	faults  func(op string) error

	memberLocks sync.Map // member ID -> *sync.Mutex

	mu      sync.Mutex
	entries []Entry
}

func NewMemoryStore(opts ...Option) *MemoryStore {
	s := &MemoryStore{}
	for _, o := range opts {
		o(s)
	}

	return s
}

func (s *MemoryStore) roundTrip(ctx context.Context, op string) error {
	if s.latency > 0 {
		select {
		case <-time.After(s.latency):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if s.faults != nil {
		return s.faults(op)
	}

	return nil
}

func (s *MemoryStore) HasEntry(ctx context.Context, paymentID string, kind Kind) (bool, error) {
	if err := s.roundTrip(ctx, "has_entry"); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.entries {
		if e.PaymentID == paymentID && e.Kind == kind {
			return true, nil
		}
	}
	return false, nil
}

func (s *MemoryStore) AwardedInMonth(ctx context.Context, memberID, month string) (int64, error) {
	if err := s.roundTrip(ctx, "awarded_in_month"); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int64
	for _, e := range s.entries {
		if e.MemberID == memberID && e.Month == month && e.Kind == KindAward {
			total += e.Points
		}
	}
	return total, nil
}

// lockMember serializes writers for one member without blocking other members.
func (s *MemoryStore) lockMember(memberID string) func() {
	l, _ := s.memberLocks.LoadOrStore(memberID, &sync.Mutex{})
	mu := l.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

func (s *MemoryStore) Award(ctx context.Context, memberID, paymentID, month string, points, cap int64) (int64, bool, error) {
	unlock := s.lockMember(memberID)
	defer unlock()

	if err := s.roundTrip(ctx, "award"); err != nil {
		return 0, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var used int64

	for _, e := range s.entries {
		if e.Kind != KindAward {
			continue
		}
		if e.PaymentID == paymentID {
			return 0, true, nil
		}
		if e.MemberID == memberID && e.Month == month {
			used += e.Points
		}
	}
	points = max(min(points, cap-used), 0)
	s.entries = append(s.entries, Entry{MemberID: memberID, PaymentID: paymentID, Kind: KindAward, Points: points, Month: month})
	return points, false, nil
}

func (s *MemoryStore) Reverse(ctx context.Context, memberID, paymentID string) (int64, bool, error) {
	unlock := s.lockMember(memberID)
	defer unlock()

	if err := s.roundTrip(ctx, "reverse"); err != nil {
		return 0, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var award *Entry
	for i, e := range s.entries {
		if e.PaymentID != paymentID {
			continue
		}
		switch e.Kind {
		case KindReversal:
			return 0, true, nil
		case KindAward:
			award = &s.entries[i]
		}
	}
	if award == nil {
		return 0, false, ErrNoAward
	}
	if award.Points > 0 {
		s.entries = append(s.entries, Entry{MemberID: award.MemberID, PaymentID: paymentID, Kind: KindReversal, Points: -award.Points, Month: award.Month})
	}

	return award.Points, false, nil
}

func (s *MemoryStore) Append(ctx context.Context, e Entry) error {
	if err := s.roundTrip(ctx, "append"); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.entries = append(s.entries, e)

	return nil
}

func (s *MemoryStore) Balance(ctx context.Context, memberID string) (int64, error) {
	if err := s.roundTrip(ctx, "balance"); err != nil {
		return 0, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var total int64

	for _, e := range s.entries {
		if e.MemberID == memberID {
			total += e.Points
		}
	}

	return total, nil
}
