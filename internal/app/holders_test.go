package app

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/d13co/algod-mesh/internal/adapters/clock"
	"github.com/d13co/algod-mesh/internal/domain"
	"github.com/d13co/algod-mesh/internal/fakealgod"
	"github.com/d13co/algod-mesh/internal/ports"
)

const holdersPath = "/v2/assets/7/holders"

var holdersCaps = domain.Capabilities{TimeTravel: true, HoldersIndex: true, MaxAcctLookback: 4}

// holdersFleet is a simFleet whose peer c has the asset holders index, and
// peer b too when both is set.
func holdersFleet(t *testing.T, both bool) *simFleet {
	t.Helper()
	f := newSimFleet(t, holdersCaps)
	if both {
		f.h.inject(addrA, signedHeartbeat(t, f.h.peerKey, "b", 2, 100, holdersCaps))
		waitFor(t, 2*time.Second, func() bool { return f.caps("b").HoldersIndex })
	}
	return f
}

// holdersMissingText is dtalgorand's message for a missing or stale index.
const holdersMissingText = "timetravel: the asset holders index is not complete: build it with ttimport -holders-index while the node is stopped"

// missIndex makes n answer holders pages as dtalgorand does while its index
// is missing or stale.
func missIndex(n *fakealgod.Node) {
	n.SetPathStatus(holdersPath, 503)
	n.SetPathMessage(holdersPath, holdersMissingText)
}

func (f *simFleet) caps(id string) domain.Capabilities {
	cands, _ := f.h.d.Snapshot()
	for _, u := range cands {
		if u.ID == id {
			return u.Caps
		}
	}
	return domain.Capabilities{}
}

func (f *simFleet) stats(id string) domain.Stats {
	cands, _ := f.h.d.Snapshot()
	for _, u := range cands {
		if u.ID == id {
			return u.Stats
		}
	}
	return domain.Stats{}
}

// A page of holders reaches only the node with the index, at the latest
// round and at a past one; a time-travel node without it is passed over.
func TestHoldersRouteToIndexedNode(t *testing.T) {
	f := holdersFleet(t, false)
	for _, q := range []string{"", "?round=10&limit=5"} {
		rec := f.do(http.MethodGet, holdersPath+q, nil)
		if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 200 || up != "c" {
			t.Fatalf("holders%s: %d upstream %q %s", q, rec.Code, up, rec.Body.String())
		}
	}
	if n := f.b.Hits(holdersPath); n != 0 {
		t.Fatalf("the node without the index saw %d holders pages", n)
	}
}

// A fleet with no node that enables the index says so, rather than letting a
// stock node answer 404.
func TestHoldersWithoutIndexedNode(t *testing.T) {
	f := newSimFleet(t, domain.Capabilities{TimeTravel: true})
	rec := f.do(http.MethodGet, holdersPath, nil)
	if rec.Code != 503 || !strings.Contains(rec.Body.String(), "TimeTravelHoldersIndex") {
		t.Fatalf("no holders node: %d %s", rec.Code, rec.Body.String())
	}
	if f.b.Hits(holdersPath)+f.c.Hits(holdersPath) != 0 {
		t.Fatal("a node without the index was asked")
	}
}

// A node whose index is missing or stale answers 503: the page goes to the
// next node, and that node gets no holders pages for a while, without
// counting against it or its other requests.
func TestHoldersMissingIndexRetriesElsewhere(t *testing.T) {
	f := holdersFleet(t, true)
	missIndex(f.b)
	missIndex(f.c)

	rec := f.do(http.MethodGet, holdersPath, nil)
	if rec.Code != 503 || !strings.Contains(rec.Body.String(), "ttimport -holders-index") {
		t.Fatalf("every index missing: %d %s", rec.Code, rec.Body.String())
	}
	if f.b.Hits(holdersPath) != 1 || f.c.Hits(holdersPath) != 1 {
		t.Fatalf("each node asked once: b %d c %d", f.b.Hits(holdersPath), f.c.Hits(holdersPath))
	}
	for _, id := range []string{"b", "c"} {
		if s := f.stats(id); !s.HoldersMissing || s.BreakerOpen || s.ErrorRate != 0 {
			t.Fatalf("%s after its 503: %+v", id, s)
		}
	}
	// Both are passed over now, with the same answer.
	rec = f.do(http.MethodGet, holdersPath, nil)
	if rec.Code != 503 || !strings.Contains(rec.Body.String(), "ttimport -holders-index") {
		t.Fatalf("both passed over: %d %s", rec.Code, rec.Body.String())
	}
	if f.b.Hits(holdersPath)+f.c.Hits(holdersPath) != 2 {
		t.Fatal("a node known to miss its index was asked again")
	}
	// Everything else still goes to them.
	if rec := f.do(http.MethodGet, "/v2/assets/7", nil); rec.Code != 200 {
		t.Fatalf("asset lookup: %d %s", rec.Code, rec.Body.String())
	}
}

// One node missing its index and the other only out for now is not "every
// node": the client gets the ordinary 503, to retry.
func TestHoldersMissingOnOneNodeOnly(t *testing.T) {
	f := holdersFleet(t, true)
	f.h.d.stats.HoldersMissing("b", holdersMissingFor)
	for i := 0; i < 5; i++ {
		f.h.d.stats.Record("c", "default", ports.Outcome{Status: 500})
	}
	if !f.stats("c").BreakerOpen {
		t.Fatal("c's breaker is not open")
	}
	rec := f.do(http.MethodGet, holdersPath, nil)
	if rec.Code != 503 || strings.Contains(rec.Body.String(), "every node") {
		t.Fatalf("one missing, one out: %d %s", rec.Code, rec.Body.String())
	}
}

// A holders page answered 503 with Retry-After (an overloaded node) goes to
// the next node; the client gets the 503 with Retry-After once every node
// has said so. The nodes are passed over for holders pages for a while, but
// not for simulations, and not marked missing or counted as failing.
func TestHoldersBusyIsNotMissing(t *testing.T) {
	f := holdersFleet(t, true)
	for _, n := range []*fakealgod.Node{f.b, f.c} {
		n.SetPathStatus(holdersPath, 503)
		n.SetPathRetryAfter(holdersPath, 2)
	}
	rec := f.do(http.MethodGet, holdersPath, nil)
	if rec.Code != 503 || rec.Header().Get("Retry-After") != "2" || !strings.Contains(rec.Body.String(), "busy") {
		t.Fatalf("every node busy: %d %q %s", rec.Code, rec.Header().Get("Retry-After"), rec.Body.String())
	}
	if f.b.Hits(holdersPath) != 1 || f.c.Hits(holdersPath) != 1 {
		t.Fatalf("each node asked once: b %d c %d", f.b.Hits(holdersPath), f.c.Hits(holdersPath))
	}
	sim := domain.Classify(http.MethodPost, "/v2/transactions/simulate", "")
	for _, id := range []string{"b", "c"} {
		s := f.stats(id)
		if !s.BusySlots.Has(domain.SlotHolders) || s.BusySlots.Has(domain.SlotHeavy) || s.HoldersMissing || s.BreakerOpen || s.ErrorRate != 0 {
			t.Fatalf("%s after its busy 503: %+v", id, s)
		}
		if s.BusySlots.Has(sim.Slot()) {
			t.Fatalf("%s is kept off simulations by a busy holders page", id)
		}
	}
	// The next page is not sent to them while they are busy.
	rec = f.do(http.MethodGet, holdersPath, nil)
	if rec.Code != 503 || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("both busy: %d %q %s", rec.Code, rec.Header().Get("Retry-After"), rec.Body.String())
	}
	if f.b.Hits(holdersPath)+f.c.Hits(holdersPath) != 2 {
		t.Fatal("a busy node was asked again")
	}
}

// A 503 to a holders page that is not dtalgorand's missing-index answer,
// such as a proxy's or a node starting up, is an ordinary failure: it counts
// against the node and is not reported as a missing index.
func TestHoldersOther503IsAFailure(t *testing.T) {
	f := holdersFleet(t, true)
	f.b.SetPathStatus(holdersPath, 503)
	f.c.SetPathStatus(holdersPath, 503)
	rec := f.do(http.MethodGet, holdersPath, nil)
	if rec.Code != 502 || strings.Contains(rec.Body.String(), "holders index") {
		t.Fatalf("plain 503s: %d %s", rec.Code, rec.Body.String())
	}
	for _, id := range []string{"b", "c"} {
		if s := f.stats(id); s.HoldersMissing || s.ErrorRate == 0 {
			t.Fatalf("%s after a plain 503: %+v", id, s)
		}
	}
}

// When one node is busy and the last one tried is missing its index, the
// client is told to retry: the busy node will serve the page.
func TestHoldersBusyBeatsMissing(t *testing.T) {
	f := holdersFleet(t, true)
	f.b.SetPathStatus(holdersPath, 503)
	f.b.SetPathRetryAfter(holdersPath, 2)
	f.b.SetPathMessage(holdersPath, "the node is busy: try again later or on another node")
	missIndex(f.c)
	rec := f.do(http.MethodGet, holdersPath, nil)
	if rec.Code != 503 || rec.Header().Get("Retry-After") != "2" || !strings.Contains(rec.Body.String(), "busy") {
		t.Fatalf("one busy, one missing: %d %q %s", rec.Code, rec.Header().Get("Retry-After"), rec.Body.String())
	}
	if !f.stats("c").HoldersMissing {
		t.Fatal("the node missing its index is not marked")
	}
}

// A page past the node's time limit gets its 504 as it is: the next node
// would only run it as long. It is not held against the node.
func TestHoldersTimeLimitPassesThrough(t *testing.T) {
	f := holdersFleet(t, true)
	f.b.SetPathStatus(holdersPath, 504)
	f.c.SetPathStatus(holdersPath, 504)
	rec := f.do(http.MethodGet, holdersPath, nil)
	if rec.Code != 504 {
		t.Fatalf("time limit: %d %s", rec.Code, rec.Body.String())
	}
	if n := f.b.Hits(holdersPath) + f.c.Hits(holdersPath); n != 1 {
		t.Fatalf("asked %d nodes, want one", n)
	}
	for _, id := range []string{"b", "c"} {
		if s := f.stats(id); s.ErrorRate != 0 || s.HoldersMissing {
			t.Fatalf("%s after a 504: %+v", id, s)
		}
	}
}

// With one node missing its index, a holders page is answered by the other,
// whichever of them is tried first.
func TestHoldersOneIndexMissing(t *testing.T) {
	f := holdersFleet(t, true)
	missIndex(f.c)
	for i := 0; i < 3; i++ {
		rec := f.do(http.MethodGet, holdersPath, nil)
		if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 200 || up != "b" {
			t.Fatalf("page %d: %d upstream %q %s", i, rec.Code, up, rec.Body.String())
		}
	}
	if n := f.c.Hits(holdersPath); n > 1 {
		t.Fatalf("the node missing its index was asked %d times", n)
	}
}

// A node missing its holders index is passed over for holdersMissingFor,
// then tried again, as a rebuilt one would be.
func TestStatsHoldersMissingExpires(t *testing.T) {
	fc := clock.NewFake(time.Unix(0, 0))
	b := NewStatsBook(DefaultBreaker, fc)
	b.HoldersMissing("n", holdersMissingFor)
	if s := b.Snapshot("n"); !s.HoldersMissing || s.BreakerOpen || s.Busy {
		t.Fatalf("marked: %+v", s)
	}
	fc.Advance(holdersMissingFor - time.Second)
	if !b.Snapshot("n").HoldersMissing {
		t.Fatal("expired early")
	}
	fc.Advance(2 * time.Second)
	if b.Snapshot("n").HoldersMissing {
		t.Fatal("still marked after holdersMissingFor")
	}
}
