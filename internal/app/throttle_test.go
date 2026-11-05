package app

import (
	"strings"
	"testing"
	"time"

	"github.com/d13co/algod-mesh/internal/adapters/clock"
	"github.com/d13co/algod-mesh/internal/domain"
	"github.com/d13co/algod-mesh/internal/fakealgod"
	"github.com/d13co/algod-mesh/internal/ports"
)

// A 429 throttles the node for its Retry-After, a second without one and
// ten at most, and is not held against it: the breaker stays closed however
// many it answers.
func TestStatsThrottle(t *testing.T) {
	fc := clock.NewFake(time.Unix(0, 0))
	b := NewStatsBook(BreakerOptions{Threshold: 2, OpenFor: 30 * time.Second, Window: 10, Alpha: 0.5}, fc)
	for range 5 {
		b.Record("u", "default", ports.Outcome{Status: 429, RetryAfter: 3 * time.Second})
	}
	if s := b.Snapshot("u"); !s.Throttled || s.BreakerOpen || s.ErrorRate != 0 {
		t.Fatalf("after 429s: %+v", s)
	}
	fc.Advance(2 * time.Second)
	if !b.Snapshot("u").Throttled {
		t.Fatal("throttled for less than its Retry-After")
	}
	fc.Advance(time.Second)
	if b.Snapshot("u").Throttled {
		t.Fatal("throttled past its Retry-After")
	}
	b.Record("u", "default", ports.Outcome{Status: 429})
	fc.Advance(999 * time.Millisecond)
	if !b.Snapshot("u").Throttled {
		t.Fatal("a 429 without Retry-After throttles for under a second")
	}
	fc.Advance(time.Millisecond)
	b.Record("u", "default", ports.Outcome{Status: 429, RetryAfter: time.Hour})
	fc.Advance(maxBusy - time.Millisecond)
	if !b.Snapshot("u").Throttled {
		t.Fatalf("throttled for under %v when Retry-After asks for more", maxBusy)
	}
	fc.Advance(time.Millisecond)
	if b.Snapshot("u").Throttled {
		t.Fatalf("throttled past %v", maxBusy)
	}
}

// A node that answers 429 is passed over for its Retry-After, without
// counting against it, and the request goes to the next node rather than
// carrying the node's 429 to the client.
func TestThrottledNodeIsPassedOver(t *testing.T) {
	h := newTwoPeerHarness(t, false)
	h.n.SetPathStatus("/v2/status", 429)
	h.n.SetPathRetryAfter("/v2/status", 3)
	for i := range 2 {
		if rec := h.get("/v2/status"); rec.Code != 200 || rec.Header().Get("X-Algod-Mesh-Upstream") != "c" {
			t.Fatalf("request %d: %d via %q: %s", i, rec.Code, rec.Header().Get("X-Algod-Mesh-Upstream"), rec.Body.String())
		}
	}
	if n := h.n.Hits("/v2/status"); n != 1 {
		t.Fatalf("throttled node saw %d requests, want 1", n)
	}
	if st := h.r.stats.Snapshot("b"); !st.Throttled || st.BreakerOpen || st.ErrorRate != 0 {
		t.Fatalf("throttled node stats: %+v", st)
	}
	if !strings.Contains(h.metricsText(), `mesh_upstream_throttled_total{upstream="b"} 1`) {
		t.Fatalf("throttle not counted:\n%s", h.metricsText())
	}
	// Past its Retry-After it gets requests again: with c failing, the
	// request reaches it.
	h.n.SetPathStatus("/v2/status", 0)
	h.c.SetFailing(true)
	h.fc.Advance(3 * time.Second)
	h.heartbeat(2, 10)
	wire, _ := domain.EncodeHeartbeat(h.cKey, domain.Heartbeat{NodeID: "c", Seq: 2, Online: true, LastRound: 10})
	h.inject(addrB, wire)
	if st := h.r.stats.Snapshot("b"); st.Throttled {
		t.Fatalf("throttled past its Retry-After: %+v", st)
	}
	if rec := h.get("/v2/status"); rec.Code != 200 || rec.Header().Get("X-Algod-Mesh-Upstream") != "b" {
		t.Fatalf("after the Retry-After: %d via %q", rec.Code, rec.Header().Get("X-Algod-Mesh-Upstream"))
	}
}

// When every node tried answers 429, the client gets a 503 with the soonest
// Retry-After, not a node's 429 it would take for its own limit. While they
// are all throttled, the next request gets one without reaching a node.
func TestEveryNodeThrottled(t *testing.T) {
	h := newTwoPeerHarness(t, false)
	for n, ra := range map[*fakealgod.Node]int{h.n: 3, h.c: 2, h.ext: 5} {
		n.SetPathStatus("/v2/status", 429)
		n.SetPathRetryAfter("/v2/status", ra)
	}
	rec := h.get("/v2/status")
	if rec.Code != 503 || rec.Header().Get("Retry-After") != "2" || !strings.Contains(rec.Body.String(), "rate limiting the mesh") {
		t.Fatalf("every node throttled: %d, Retry-After %q: %s", rec.Code, rec.Header().Get("Retry-After"), rec.Body.String())
	}
	if b, c, e := h.n.Hits("/v2/status"), h.c.Hits("/v2/status"), h.ext.Hits("/v2/status"); b != 1 || c != 1 || e != 1 {
		t.Fatalf("nodes tried b=%d c=%d ext=%d, want once each", b, c, e)
	}
	for _, id := range []string{"b", "c"} {
		if st := h.r.stats.Snapshot(id); st.BreakerOpen || st.ErrorRate != 0 {
			t.Fatalf("%s blamed for its 429: %+v", id, st)
		}
	}
	rec = h.get("/v2/status")
	if rec.Code != 503 || rec.Header().Get("Retry-After") != "1" || !strings.Contains(rec.Body.String(), "rate limiting the mesh") {
		t.Fatalf("every node still throttled: %d, Retry-After %q: %s", rec.Code, rec.Header().Get("Retry-After"), rec.Body.String())
	}
	if b, c := h.n.Hits("/v2/status"), h.c.Hits("/v2/status"); b != 1 || c != 1 {
		t.Fatalf("throttled nodes saw more requests: b=%d c=%d", b, c)
	}
}

// A broadcast every node answers 429 falls back to the external like one no
// node answered, and the client gets a 503 to retry, not the 429.
func TestMultiBroadcastThrottled(t *testing.T) {
	h := newTwoPeerHarness(t, true)
	for _, n := range []*fakealgod.Node{h.n, h.c, h.ext} {
		n.SetPathStatus("/v2/transactions", 429)
	}
	rec := h.post("/v2/transactions", "txnbytes")
	if rec.Code != 503 || rec.Header().Get("Retry-After") != "1" || !strings.Contains(rec.Body.String(), "rate limiting the mesh") {
		t.Fatalf("every node throttled: %d, Retry-After %q: %s", rec.Code, rec.Header().Get("Retry-After"), rec.Body.String())
	}
	if e := h.ext.Hits("/v2/transactions"); e != 1 {
		t.Fatalf("external tried %d times, want 1", e)
	}
	if st := h.r.stats.Snapshot("b"); !st.Throttled || st.BreakerOpen || st.ErrorRate != 0 {
		t.Fatalf("throttled node stats: %+v", st)
	}
	if !strings.Contains(h.metricsText(), `mesh_upstream_throttled_total{upstream="b"} 1`) {
		t.Fatalf("throttle not counted:\n%s", h.metricsText())
	}
}
