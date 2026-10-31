package app

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/d13co/algod-mesh/internal/domain"
	"github.com/d13co/algod-mesh/internal/fakealgod"
)

var ttHeaderCaps = domain.Capabilities{TimeTravel: true, MaxAcctLookback: 4, TTRoundHeader: true}

// ttHeaderFleet is a simFleet whose peers b and c both have the time-travel
// history and mark their answers at a round with X-Algod-TT-Round, as their
// agents' probes found. b is tried first.
func ttHeaderFleet(t *testing.T) *simFleet {
	t.Helper()
	f := newSimFleetDir(t, ttHeaderCaps, RouterOptions{Mode: domain.ModeFallback, RetryBudget: 1}, DirectoryOptions{},
		func(o *fakealgod.Options) { o.TimeTravel = true })
	f.h.inject(addrA, signedHeartbeat(t, f.h.peerKey, "b", 2, 100, ttHeaderCaps))
	waitFor(t, 2*time.Second, func() bool { return f.caps("b").TTRoundHeader })
	return f
}

const missingBox = "/v2/applications/5/box"

// A node that answers a read at a round without the mark ignored the round:
// the read goes to the next node, and the node gets no more reads at a round
// for a while, without counting against it or its other requests.
func TestRoundUnhonoredRetriesAndPassesOver(t *testing.T) {
	f := ttHeaderFleet(t)
	f.b.SetOmitTTRound(true)
	for i := 0; i < 2; i++ {
		rec := f.do(http.MethodGet, pastAccount+"?round=50", nil)
		if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 200 || up != "c" || rec.Header().Get(domain.TTRoundHeader) != "50" {
			t.Fatalf("attempt %d: %d upstream %q mark %q %s", i, rec.Code, up, rec.Header().Get(domain.TTRoundHeader), rec.Body.String())
		}
	}
	if n := f.b.Hits(pastAccount); n != 1 {
		t.Fatalf("b asked %d times, want once", n)
	}
	if s := f.stats("b"); !s.RoundUnhonored || s.BreakerOpen || s.ErrorRate != 0 || s.HistoryUntrusted {
		t.Fatalf("b after its unmarked answer: %+v", s)
	}
	rec := f.do(http.MethodGet, "/v2/assets/7", nil)
	if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 200 || up != "b" {
		t.Fatalf("latest read: %d upstream %q", rec.Code, up)
	}
}

// An answer marked with another round is not the one asked for either.
func TestRoundUnhonoredWrongRound(t *testing.T) {
	f := ttHeaderFleet(t)
	f.b.SetTTRoundValue("100")
	rec := f.do(http.MethodGet, pastAccount+"?round=50", nil)
	if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 200 || up != "c" || rec.Header().Get(domain.TTRoundHeader) != "50" {
		t.Fatalf("read at a round: %d upstream %q mark %q", rec.Code, up, rec.Header().Get(domain.TTRoundHeader))
	}
	if !f.stats("b").RoundUnhonored {
		t.Fatal("b not passed over")
	}
}

// A 404 marked with the round is "not found at R": the client's answer. One
// without the mark may be the latest state's, and is retried.
func TestRoundUnhonored404(t *testing.T) {
	f := ttHeaderFleet(t)
	rec := f.do(http.MethodGet, missingBox+"?name=str:k&round=50", nil)
	if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 404 || up != "b" || rec.Header().Get(domain.TTRoundHeader) != "50" {
		t.Fatalf("marked 404: %d upstream %q mark %q", rec.Code, up, rec.Header().Get(domain.TTRoundHeader))
	}
	if f.c.Hits(missingBox) != 0 || f.stats("b").RoundUnhonored {
		t.Fatalf("marked 404 retried (c %d) or b passed over", f.c.Hits(missingBox))
	}

	f.b.SetOmitTTRound(true)
	rec = f.do(http.MethodGet, missingBox+"?name=str:k&round=50", nil)
	if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 404 || up != "c" || rec.Header().Get(domain.TTRoundHeader) != "50" {
		t.Fatalf("unmarked 404: %d upstream %q mark %q", rec.Code, up, rec.Header().Get(domain.TTRoundHeader))
	}
	if !f.stats("b").RoundUnhonored {
		t.Fatal("b not passed over after an unmarked 404")
	}
}

// A box listing is not checked: stock algod honours its round at recent
// rounds without the mark.
func TestRoundCheckSkipsBoxes(t *testing.T) {
	f := ttHeaderFleet(t)
	f.b.SetOmitTTRound(true)
	rec := f.do(http.MethodGet, boxList+"?round=50", nil)
	if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 200 || up != "b" {
		t.Fatalf("box listing: %d upstream %q", rec.Code, up)
	}
	if f.c.Hits(boxList) != 0 || f.stats("b").RoundUnhonored {
		t.Fatalf("box listing retried (c %d) or b passed over", f.c.Hits(boxList))
	}
}

// A node whose probe did not find the mark (a dtalgorand from before it, or
// an external) is not checked.
func TestRoundCheckNeedsCapability(t *testing.T) {
	f := ttFleet(t) // b and c have the history, without the mark capability
	f.b.SetOmitTTRound(true)
	rec := f.do(http.MethodGet, pastAccount+"?round=50", nil)
	if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 200 || up != "b" {
		t.Fatalf("read at a round: %d upstream %q", rec.Code, up)
	}
	if f.c.Hits(pastAccount) != 0 || f.stats("b").RoundUnhonored {
		t.Fatalf("read retried (c %d) or b passed over", f.c.Hits(pastAccount))
	}
}

// Reads without a round, and refusals of the round, carry no mark and are
// the client's.
func TestRoundCheckLeavesUnmarkedAnswers(t *testing.T) {
	f := ttHeaderFleet(t)
	f.b.SetOmitTTRound(true)
	rec := f.do(http.MethodGet, pastAccount, nil)
	if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 200 || up != "b" {
		t.Fatalf("latest read: %d upstream %q", rec.Code, up)
	}
	f.b.SetPathStatus(pastAccount, 400)
	f.b.SetPathMessage(pastAccount, "given round is greater than the latest round")
	rec = f.do(http.MethodGet, pastAccount+"?round=50", nil)
	if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 400 || up != "b" {
		t.Fatalf("refusal: %d upstream %q", rec.Code, up)
	}
	if f.c.Hits(pastAccount) != 0 || f.stats("b").RoundUnhonored {
		t.Fatalf("retried (c %d) or b passed over", f.c.Hits(pastAccount))
	}
	// A path no node routes gets echo's 404 at every round.
	f.b.SetPathStatus(pastAccount, 404)
	f.b.SetPathMessage(pastAccount, "Not Found")
	rec = f.do(http.MethodGet, pastAccount+"?round=50", nil)
	if rec.Code != 404 || f.c.Hits(pastAccount) != 0 || f.stats("b").RoundUnhonored {
		t.Fatalf("route 404: %d, c %d", rec.Code, f.c.Hits(pastAccount))
	}
}

// When no node answers as of the round, the client is told so, then and
// while they are passed over.
func TestRoundUnhonoredEverywhere(t *testing.T) {
	f := ttHeaderFleet(t)
	f.b.SetOmitTTRound(true)
	f.c.SetOmitTTRound(true)
	for i := 0; i < 2; i++ {
		rec := f.do(http.MethodGet, pastAccount+"?round=50", nil)
		if rec.Code != 503 || !strings.Contains(rec.Body.String(), "algod-mesh: ") || !strings.Contains(rec.Body.String(), "round") {
			t.Fatalf("attempt %d: %d %s", i, rec.Code, rec.Body.String())
		}
	}
	// No node honoured it, so none is passed over: the request is as
	// likely to blame, and one client mustn't take the history out of use.
	if f.b.Hits(pastAccount) != 2 || f.c.Hits(pastAccount) != 2 {
		t.Fatalf("each node asked on each request: b %d c %d", f.b.Hits(pastAccount), f.c.Hits(pastAccount))
	}
	if f.stats("b").RoundUnhonored || f.stats("c").RoundUnhonored {
		t.Fatal("a node was passed over though none honoured the round")
	}
}

// A path that takes no round, such as pending transactions by address, is
// answered without the mark and is not checked: no node is passed over.
func TestRoundCheckSkipsUnmarkedRoutes(t *testing.T) {
	f := ttHeaderFleet(t)
	path := pastAccount + "/transactions/pending"
	f.b.SetOmitTTRound(true)
	f.c.SetOmitTTRound(true)
	rec := f.do(http.MethodGet, path+"?round=50", nil)
	if rec.Code != 200 || f.stats("b").RoundUnhonored || f.stats("c").RoundUnhonored {
		t.Fatalf("pending by address: %d %s", rec.Code, rec.Body.String())
	}
	if f.b.Hits(path)+f.c.Hits(path) != 1 {
		t.Fatalf("retried: b %d c %d", f.b.Hits(path), f.c.Hits(path))
	}
}
