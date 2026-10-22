package rewards_test

import (
	"fmt"
	"testing"

	"github.com/rentpoints/pointsd/internal/ledger"
	"github.com/rentpoints/pointsd/internal/rewards"
)

func TestReplicas_RedeliveredPaymentsAndReversalsStayConsistent(t *testing.T) {
	store := ledger.NewMemoryStore(ledger.WithLatency(dbLatency))
	replicas := []*rewards.Handler{newHandler(store), newHandler(store), newHandler(store)}

	// 12 payments of 1,000 points per member: the cap lets 10 through.
	// Every payment is delivered three times at once, across replicas.
	const members, payments = 4, 12
	var fns []func()
	for m := 0; m < members; m++ {
		for p := 0; p < payments; p++ {
			ev := settled(fmt.Sprintf("pay_%d_%d", m, p), fmt.Sprintf("mbr_%d", m), 1000)
			for r := range replicas {
				h := replicas[(m+p+r)%len(replicas)]
				fns = append(fns, func() { deliver(t, h, ev) })
			}
		}
	}
	concurrently(fns...)

	for m := 0; m < members; m++ {
		if got := balance(t, store, fmt.Sprintf("mbr_%d", m)); got != rewards.DefaultPolicy.MonthlyCap {
			t.Fatalf("mbr_%d balance = %d, want %d", m, got, rewards.DefaultPolicy.MonthlyCap)
		}
	}

	// Reverse the first payment of every member, again three times at once.
	fns = nil
	for m := 0; m < members; m++ {
		ev := reversed(fmt.Sprintf("pay_%d_0", m), fmt.Sprintf("mbr_%d", m), 1000)
		for r := range replicas {
			h := replicas[(m+r)%len(replicas)]
			fns = append(fns, func() { deliver(t, h, ev) })
		}
	}
	concurrently(fns...)

	// Which payments were cut by the cap depends on arrival order, but a
	// reversal removes exactly what its payment earned, and never more than 1,000.
	for m := 0; m < members; m++ {
		got := balance(t, store, fmt.Sprintf("mbr_%d", m))
		if got < 9000 || got > 10000 {
			t.Fatalf("mbr_%d balance after reversal = %d, want between 9000 and 10000", m, got)
		}
	}
}
