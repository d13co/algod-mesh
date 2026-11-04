package app

import (
	"crypto/ed25519"
	crand "crypto/rand"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
	"time"

	"github.com/d13co/algod-mesh/internal/adapters/algodhttp"
	"github.com/d13co/algod-mesh/internal/adapters/logging"
	"github.com/d13co/algod-mesh/internal/adapters/proxy"
	"github.com/d13co/algod-mesh/internal/domain"
	"github.com/d13co/algod-mesh/internal/fakealgod"
)

// go-dtalgorand-sdk tells errors apart by status and message
// (client/v2/algod/dt_errors.go, dt_holders.go, dt_versions.go): these are
// its patterns. The answers the mesh writes itself must match the ones the
// node's own answer would, or a client behind the mesh misreads them.
var (
	sdkHistory     = regexp.MustCompile(`box index is incomplete|asset holders index is not complete|history is not served`)
	sdkPastRound   = regexp.MustCompile(`requires? EnableTimeTravel|given round is no longer available`)
	sdkHoldersOff  = regexp.MustCompile(`asset holders require EnableTimeTravel and TimeTravelHoldersIndex`)
	sdkAssetAbsent = regexp.MustCompile(`asset does not exist`)
)

func TestMeshAnswersMatchSDK(t *testing.T) {
	for _, tc := range []struct {
		name, msg string
		re        *regexp.Regexp
	}{
		{"holders index missing everywhere (IsHistoryUnavailable)", holdersMissingMessage, sdkHistory},
		{"holders index missing on the nodes tried (IsHistoryUnavailable)", holdersMissingTriedMessage, sdkHistory},
		{"no time-travel node (DTUnsupportedError)", noTimeTravelMessage, sdkPastRound},
		{"no holders index node (DTUnsupportedError)", noHoldersIndexMessage, sdkHoldersOff},
	} {
		if !tc.re.MatchString(tc.msg) {
			t.Errorf("%s: %q does not match %s", tc.name, tc.msg, tc.re)
		}
	}
	// A versions 404 is unsupported on any message; a holders 404 only when
	// it doesn't say the asset is missing.
	if sdkAssetAbsent.MatchString(noVersionsMessage) {
		t.Errorf("versions 404 %q reads as a missing asset", noVersionsMessage)
	}
}

// A versions page in a fleet without time travel gets the 404 of a node
// without the routes.
func TestVersionsWithoutHistoryNode(t *testing.T) {
	f := newSimFleet(t, domain.Capabilities{})
	rec := f.do(http.MethodGet, "/v2/accounts/ADDR/versions", nil)
	if rec.Code != 404 || !sdkPastRound.MatchString(rec.Body.String()) {
		t.Fatalf("versions without history node: %d %s", rec.Code, rec.Body.String())
	}
}

// A holders page in a fleet with no time travel at all gets the holders
// refusal, which the SDK reads as unsupported, rather than the time-travel
// one, which it reads as a missing past round.
func TestHoldersWithoutTimeTravel(t *testing.T) {
	f := newSimFleet(t, domain.Capabilities{})
	rec := f.do(http.MethodGet, holdersPath, nil)
	if rec.Code != 400 || !sdkHoldersOff.MatchString(rec.Body.String()) {
		t.Fatalf("holders without time travel: %d %s", rec.Code, rec.Body.String())
	}
}

// stopC stops peer c: its node goes away, and its heartbeat expires while
// b's is renewed.
func (f *simFleet) stopC(t *testing.T) {
	t.Helper()
	f.c.Close()
	f.h.fc.Advance(4 * time.Second)
	f.h.inject(addrA, signedHeartbeat(t, f.h.peerKey, "b", 2, 100, domain.Capabilities{MaxAcctLookback: 4}))
	waitFor(t, 2*time.Second, func() bool {
		cands, _ := f.h.d.Snapshot()
		var bUp, cDown bool
		for _, u := range cands {
			bUp = bUp || (u.ID == "b" && u.Health.Reachable())
			cDown = cDown || (u.ID == "c" && u.Source == "none")
		}
		return bUp && cDown
	})
}

// A time-travel node that is only down leaves the fleet with time travel:
// the client gets a 503 to retry, not the refusal it would keep.
func TestTimeTravelNodeDownIsNotRefusal(t *testing.T) {
	f := newSimFleet(t, domain.Capabilities{TimeTravel: true})
	f.stopC(t)
	for _, path := range []string{"/v2/accounts/ADDR?round=5", "/v2/accounts/ADDR/versions"} {
		if rec := f.do(http.MethodGet, path, nil); rec.Code != 503 {
			t.Fatalf("%s with the time-travel node down: %d %s", path, rec.Code, rec.Body.String())
		}
	}
	if n := f.b.Hits("/v2/accounts"); n != 0 {
		t.Fatalf("plain node saw %d time-travel requests", n)
	}
}

// Likewise for the only node with the holders index.
func TestHoldersNodeDownIsNotRefusal(t *testing.T) {
	f := holdersFleet(t, false)
	f.stopC(t)
	if rec := f.do(http.MethodGet, holdersPath, nil); rec.Code != 503 {
		t.Fatalf("holders with the holders node down: %d %s", rec.Code, rec.Body.String())
	}
}

// A peer never heard from, with no declared capabilities, may have time
// travel: until it is heard, a read at a round gets a 503, though its node
// answers probes. With its capabilities declared it is known, and the
// fleet's refusal is given.
func TestTimeTravelUnknownPeerIsNotRefusal(t *testing.T) {
	for _, tc := range []struct {
		name     string
		declared *domain.CapabilityOverrides
		want     int
	}{
		{"never heard", nil, 503},
		{"declared without time travel", &domain.CapabilityOverrides{TimeTravel: new(bool)}, 400},
	} {
		h := newHarnessWith(t, DirectoryOptions{}, algodhttp.Factory{})
		b := fakealgod.New(fakealgod.Options{ID: "b", StartRound: 20})
		t.Cleanup(b.Close)
		c := fakealgod.New(fakealgod.Options{ID: "c", StartRound: 20})
		t.Cleanup(c.Close)
		cPub, _, _ := ed25519.GenerateKey(crand.Reader)
		recB := h.peerRecord(addrA)
		recB.Endpoints, recB.Token = []string{b.URL()}, b.Token()
		recC := domain.NodeRecord{ID: "c", Network: "n", Agent: domain.AgentInfo{Addrs: []string{addrB}, PubKey: []byte(cPub)},
			Endpoints: []string{c.URL()}, Token: c.Token(), Declared: tc.declared}
		h.d.SetRecords([]domain.NodeRecord{recB, recC})
		r := NewRouter(RouterOptions{Mode: domain.ModeFallback, RetryBudget: 1},
			h.d, h.d.monitor, proxy.New(nil), nil, h.d.stats, h.fc, logging.Nop{}, h.m, nil, nil)
		h.inject(addrA, signedHeartbeat(t, h.peerKey, "b", 1, 20, domain.Capabilities{}))
		waitFor(t, 2*time.Second, func() bool {
			cands, _ := h.d.Snapshot()
			for _, u := range cands {
				if u.ID == "b" && u.Health.Reachable() {
					return true
				}
			}
			return false
		})
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v2/accounts/ADDR?round=5", nil))
		if rec.Code != tc.want {
			t.Fatalf("%s: %d %s, want %d", tc.name, rec.Code, rec.Body.String(), tc.want)
		}
	}
}

// A fleet with no node known yet (a balancer just started) may have the
// feature: no refusal for it.
func TestFleetLacksNeedsAKnownNode(t *testing.T) {
	tt := func(c domain.Capabilities) bool { return c.TimeTravel }
	if fleetLacks(nil, tt) {
		t.Fatal("an empty fleet lacks time travel")
	}
	if fleetLacks([]domain.Upstream{{ID: "x", Excluded: true}}, tt) {
		t.Fatal("a fleet of excluded nodes lacks time travel")
	}
	if !fleetLacks([]domain.Upstream{{ID: "s", Health: domain.HealthSynced}}, tt) {
		t.Fatal("a known stock node has time travel")
	}
}
