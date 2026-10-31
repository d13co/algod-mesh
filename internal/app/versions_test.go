package app

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/d13co/algod-mesh/internal/domain"
)

const versionsPath = "/v2/accounts/ADDR/versions"

// versionsFleet is a simFleet whose peers both have the history: c at 100,
// b at 90, within the sync tolerance of c. b is the better tier, so it gets
// every page it can serve.
func versionsFleet(t *testing.T) *simFleet {
	t.Helper()
	caps := domain.Capabilities{TimeTravel: true, MaxAcctLookback: 4}
	tier := 2
	f := newSimFleetDir(t, caps, RouterOptions{Mode: domain.ModeFallback, RetryBudget: 1, SyncTolerance: 20},
		DirectoryOptions{SyncTolerance: 20, PeerOverrides: map[string]PeerOverride{"c": {Tier: &tier}}})
	f.h.inject(addrA, signedHeartbeat(t, f.h.peerKey, "b", 2, 90, caps))
	waitFor(t, 2*time.Second, func() bool {
		cands, _ := f.h.d.Snapshot()
		for _, u := range cands {
			if u.ID == "b" {
				return u.Caps.TimeTravel && u.LastRound == 90 && u.Health.Reachable()
			}
		}
		return false
	})
	return f
}

// A versions page that ends past a node's round goes to a node that has
// reached it, which a client following a page from that node needs; a
// lagging node would answer 400.
func TestVersionsRouteByLastRound(t *testing.T) {
	f := versionsFleet(t)
	for q, want := range map[string]string{
		"": "b", "?max-round=90": "b", "?rounds=5,90": "b",
		"?max-round=95": "c", "?min-round=10&max-round=100": "c", "?rounds=5,95,20": "c",
	} {
		rec := f.do(http.MethodGet, versionsPath+q, nil)
		if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 200 || up != want {
			t.Fatalf("versions%s: %d upstream %q, want %q: %s", q, rec.Code, up, want, rec.Body.String())
		}
	}
}

// A versions page past every node gets algod's own answer, without reaching
// a node.
func TestVersionsPastEveryNodeAnswersAsAlgod(t *testing.T) {
	f := versionsFleet(t)
	for _, q := range []string{"?max-round=101", "?rounds=5,101"} {
		rec := f.do(http.MethodGet, versionsPath+q, nil)
		if rec.Code != 400 || !strings.Contains(rec.Body.String(), "given round is greater than the latest round") {
			t.Fatalf("versions%s: %d %s", q, rec.Code, rec.Body.String())
		}
	}
	if n := f.b.Hits(versionsPath) + f.c.Hits(versionsPath); n != 0 {
		t.Fatalf("the nodes saw %d pages past their round", n)
	}
}
