package ledger_test

import (
	"context"
	"errors"
	"testing"

	"github.com/rentpoints/pointsd/internal/ledger"
)

func TestAward_CapsAndDeduplicates(t *testing.T) {
	ctx := context.Background()
	s := ledger.NewMemoryStore()

	if got, dup, err := s.Award(ctx, "m", "p1", "2026-10", 9500, 10000); err != nil || dup || got != 9500 {
		t.Fatalf("first award = %d, %v, %v", got, dup, err)
	}
	if got, _, _ := s.Award(ctx, "m", "p2", "2026-10", 1000, 10000); got != 500 {
		t.Fatalf("capped award = %d, want 500", got)
	}
	if got, dup, _ := s.Award(ctx, "m", "p2", "2026-10", 1000, 10000); !dup || got != 0 {
		t.Fatalf("repeat award = %d, dup %v", got, dup)
	}
	if got, _, _ := s.Award(ctx, "m", "p3", "2026-11", 1000, 10000); got != 1000 {
		t.Fatalf("next month award = %d, want 1000", got)
	}
}

func TestReverse(t *testing.T) {
	ctx := context.Background()
	s := ledger.NewMemoryStore()

	if _, _, err := s.Reverse(ctx, "m", "p1"); !errors.Is(err, ledger.ErrNoAward) {
		t.Fatalf("err = %v, want ErrNoAward", err)
	}
	s.Award(ctx, "m", "p1", "2026-10", 9500, 10000)
	s.Award(ctx, "m", "p2", "2026-10", 1000, 10000)

	if got, dup, err := s.Reverse(ctx, "m", "p2"); err != nil || dup || got != 500 {
		t.Fatalf("reverse = %d, %v, %v", got, dup, err)
	}
	if _, dup, _ := s.Reverse(ctx, "m", "p2"); !dup {
		t.Fatal("second reverse was not a duplicate")
	}
	if b, _ := s.Balance(ctx, "m"); b != 9500 {
		t.Fatalf("balance = %d, want 9500", b)
	}
	// Reversals do not give cap room back.
	if got, _, _ := s.Award(ctx, "m", "p3", "2026-10", 1000, 10000); got != 0 {
		t.Fatalf("award after reversal = %d, want 0", got)
	}
}
