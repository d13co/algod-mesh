package app

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/d13co/algod-mesh/internal/domain"
	"github.com/d13co/algod-mesh/internal/fakealgod"
)

var ttCaps = domain.Capabilities{TimeTravel: true, MaxAcctLookback: 4}

// ttFleet is a simFleet whose peers b and c both have the time-travel
// history.
func ttFleet(t *testing.T) *simFleet {
	t.Helper()
	f := newSimFleet(t, ttCaps)
	f.h.inject(addrA, signedHeartbeat(t, f.h.peerKey, "b", 2, 100, ttCaps))
	waitFor(t, 2*time.Second, func() bool { return f.caps("b").TimeTravel })
	return f
}

const (
	pastAccount = "/v2/accounts/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAY5HFKQ"
	boxVersions = "/v2/applications/5/boxes/versions"
)

// untrust makes n answer path as dtalgorand does once its history is found
// to differ from the chain.
func untrust(n *fakealgod.Node, path string) {
	n.SetPathStatus(path, 503)
	n.SetPathMessage(path, "the time travel history is not served: the node's own round 90 differs from its evaluation")
}

// A node whose history is untrusted answers 503 to a read at a round: the
// read goes to the next node, and the node gets no more of them for a while,
// without counting against it or its other requests.
func TestHistoryUntrustedRetriesElsewhere(t *testing.T) {
	f := ttFleet(t)
	untrust(f.b, pastAccount)
	rec := f.do(http.MethodGet, pastAccount+"?round=50", nil)
	if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 200 || up != "c" {
		t.Fatalf("read at a round: %d upstream %q %s", rec.Code, up, rec.Body.String())
	}
	if s := f.stats("b"); !s.HistoryUntrusted || s.BreakerOpen || s.ErrorRate != 0 {
		t.Fatalf("b after its 503: %+v", s)
	}
	rec = f.do(http.MethodGet, pastAccount+"?round=50", nil)
	if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 200 || up != "c" {
		t.Fatalf("second read: %d upstream %q", rec.Code, up)
	}
	if n := f.b.Hits(pastAccount); n != 1 {
		t.Fatalf("b asked %d times, want once", n)
	}
	// b still serves the latest state.
	rec = f.do(http.MethodGet, "/v2/assets/7", nil)
	if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 200 || up != "b" {
		t.Fatalf("latest read: %d upstream %q", rec.Code, up)
	}
}

// When every time-travel node's history is untrusted, the client is told so.
func TestHistoryUntrustedEverywhere(t *testing.T) {
	f := ttFleet(t)
	untrust(f.b, pastAccount)
	untrust(f.c, pastAccount)
	for i := 0; i < 2; i++ {
		rec := f.do(http.MethodGet, pastAccount+"?round=50", nil)
		if rec.Code != 503 || !strings.Contains(rec.Body.String(), "not served on any node") {
			t.Fatalf("attempt %d: %d %s", i, rec.Code, rec.Body.String())
		}
	}
	if f.b.Hits(pastAccount) != 1 || f.c.Hits(pastAccount) != 1 {
		t.Fatalf("each node asked once: b %d c %d", f.b.Hits(pastAccount), f.c.Hits(pastAccount))
	}
}

// A versions page whose range starts before a node's box index is retried on
// the next node, and the node is not passed over: a later range may be in
// its index. When no node has it, the client gets the node's message.
func TestBoxIndexMissingRetriesWithoutPassOver(t *testing.T) {
	f := ttFleet(t)
	f.b.SetPathStatus(boxVersions, 503)
	f.b.SetPathMessage(boxVersions, "timetravel: the box index is incomplete before round 80: build it with ttimport -index-boxes while the node is stopped")
	for i := 0; i < 2; i++ {
		rec := f.do(http.MethodGet, boxVersions, nil)
		if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 200 || up != "c" {
			t.Fatalf("attempt %d: %d upstream %q %s", i, rec.Code, up, rec.Body.String())
		}
	}
	if s := f.stats("b"); s.BreakerOpen || s.ErrorRate != 0 || s.HistoryUntrusted {
		t.Fatalf("b after its 503s: %+v", s)
	}
	f.c.SetPathStatus(boxVersions, 503)
	f.c.SetPathMessage(boxVersions, "timetravel: the box index is incomplete: it is being rebuilt: build it with ttimport -index-boxes while the node is stopped")
	before := f.b.Hits(boxVersions)
	rec := f.do(http.MethodGet, boxVersions, nil)
	if rec.Code != 503 || !strings.Contains(rec.Body.String(), "ttimport -index-boxes") {
		t.Fatalf("no index anywhere: %d %s", rec.Code, rec.Body.String())
	}
	if f.b.Hits(boxVersions) != before+1 {
		t.Fatal("b was passed over after its box index 503s")
	}
}

// A peer whose probe found its history untrusted is not sent reads at a
// round at all, while it keeps serving the latest state.
func TestHistoryUntrustedFromHeartbeat(t *testing.T) {
	f := ttFleet(t)
	untrusted := ttCaps
	untrusted.HistoryUntrusted = true
	f.h.inject(addrA, signedHeartbeat(t, f.h.peerKey, "b", 3, 100, untrusted))
	waitFor(t, 2*time.Second, func() bool { return f.caps("b").HistoryUntrusted })
	rec := f.do(http.MethodGet, pastAccount+"?round=50", nil)
	if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 200 || up != "c" {
		t.Fatalf("read at a round: %d upstream %q", rec.Code, up)
	}
	if rec.Header().Get(domain.TTRoundHeader) != "50" {
		t.Fatalf("%s %q, want 50", domain.TTRoundHeader, rec.Header().Get(domain.TTRoundHeader))
	}
	if n := f.b.Hits(pastAccount); n != 0 {
		t.Fatalf("b asked %d times", n)
	}
	rec = f.do(http.MethodGet, "/v2/assets/7", nil)
	if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 200 || up != "b" {
		t.Fatalf("latest read: %d upstream %q", rec.Code, up)
	}
}

const boxList = "/v2/applications/5/boxes"

// refuseRound makes n answer path as algod does for a round past its
// lookback, which is how dtalgorand answers a box listing at a round outside
// its memory while its history is untrusted.
func refuseRound(n *fakealgod.Node, path string) {
	n.SetPathStatus(path, 400)
	n.SetPathMessage(path, "given round is no longer available")
}

// A time-travel node answers a box listing at an old round with a 400 while
// its history is untrusted: the listing goes to the next node, and the node
// is held to a stock node's windows, as for the 503.
func TestBoxesRoundGoneOnTimeTravelNode(t *testing.T) {
	f := ttFleet(t)
	refuseRound(f.b, boxList)
	for i := 0; i < 2; i++ {
		rec := f.do(http.MethodGet, boxList+"?round=50", nil)
		if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 200 || up != "c" {
			t.Fatalf("attempt %d: %d upstream %q %s", i, rec.Code, up, rec.Body.String())
		}
	}
	if n := f.b.Hits(boxList); n != 1 {
		t.Fatalf("b asked %d times, want once", n)
	}
	if s := f.stats("b"); !s.HistoryUntrusted || s.BreakerOpen || s.ErrorRate != 0 {
		t.Fatalf("b after its 400: %+v", s)
	}
}

// When every time-travel node refuses the round that way, the client is told
// their history is not served, as the SDK expects, then and while they are
// passed over.
func TestBoxesRoundGoneEverywhere(t *testing.T) {
	f := ttFleet(t)
	refuseRound(f.b, boxList)
	refuseRound(f.c, boxList)
	for i := 0; i < 2; i++ {
		rec := f.do(http.MethodGet, boxList+"?round=50", nil)
		if rec.Code != 503 || !strings.Contains(rec.Body.String(), "history is not served") {
			t.Fatalf("attempt %d: %d %s", i, rec.Code, rec.Body.String())
		}
	}
	if f.b.Hits(boxList) != 1 || f.c.Hits(boxList) != 1 {
		t.Fatalf("each node asked once: b %d c %d", f.b.Hits(boxList), f.c.Hits(boxList))
	}
}

// A stock node's 400 for a box listing past its lookback is the client's
// answer, as is any other 400 from a time-travel node.
func TestBoxesOther400PassedThrough(t *testing.T) {
	stock := newSimFleet(t, ttCaps) // b is a stock node, c a time-travel one
	refuseRound(stock.b, boxList)
	rec := stock.do(http.MethodGet, boxList+"?round=98", nil)
	if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 400 || up != "b" || !strings.Contains(rec.Body.String(), "no longer available") {
		t.Fatalf("stock node's 400: %d upstream %q %s", rec.Code, up, rec.Body.String())
	}
	if s := stock.stats("b"); s.HistoryUntrusted {
		t.Fatal("a stock node was marked untrusted")
	}

	f := ttFleet(t)
	f.b.SetPathStatus(boxList, 400)
	f.b.SetPathMessage(boxList, "limit parameter must be a positive integer")
	rec = f.do(http.MethodGet, boxList+"?round=50", nil)
	if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 400 || up != "b" || !strings.Contains(rec.Body.String(), "positive integer") {
		t.Fatalf("other 400: %d upstream %q %s", rec.Code, up, rec.Body.String())
	}
	if f.c.Hits(boxList) != 0 || f.stats("b").HistoryUntrusted {
		t.Fatalf("other 400 retried (c %d) or marked b", f.c.Hits(boxList))
	}
}
