package app

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/d13co/algod-mesh/internal/adapters/algodhttp"
	"github.com/d13co/algod-mesh/internal/adapters/logging"
	"github.com/d13co/algod-mesh/internal/adapters/proxy"
	"github.com/d13co/algod-mesh/internal/domain"
	"github.com/d13co/algod-mesh/internal/fakealgod"
)

const stateChangesPath = "/v2/deltas/50/txn/state-changes"

const groupIncludes = domain.IncludeAccounts | domain.IncludeAssets | domain.IncludeApps

// scCaps is a time-travel dtalgorand that serves state changes, taking the
// include values given.
func scCaps(include domain.StateChangesInclude) domain.Capabilities {
	return domain.Capabilities{TimeTravel: true, MaxAcctLookback: 4, Simulate: &domain.SimulateLimits{MaxTxnGroups: 1,
		StateChanges: domain.StateChangesLimits{Served: true, MaxResponseBytes: 1 << 24, Include: include}}}
}

func scFleet(t *testing.T, bCaps, cCaps domain.Capabilities) *simFleet {
	t.Helper()
	return newSimPair(t, bCaps, cCaps, RouterOptions{Mode: domain.ModeFallback, RetryBudget: 1}, DirectoryOptions{})
}

// State changes reach only a node that serves them, never a time-travel
// node from before them (which has no route).
func TestStateChangesRouteToServingNode(t *testing.T) {
	f := scFleet(t, ttCaps, scCaps(groupIncludes))
	rec := f.do(http.MethodGet, stateChangesPath, nil)
	if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 200 || up != "c" {
		t.Fatalf("state changes: %d upstream %q %s", rec.Code, up, rec.Body.String())
	}
	if n := f.b.Hits(stateChangesPath); n != 0 {
		t.Fatalf("b asked %d times", n)
	}
}

// A request goes to a node that takes every include value it asks for. Each
// case has a fleet of its own: a latency sample from one would sway the next.
func TestStateChangesIncludeRouting(t *testing.T) {
	trace := scCaps(groupIncludes | domain.IncludeBudget | domain.IncludeReads | domain.IncludeTrace)
	trace.DeveloperAPI = true
	for _, tc := range []struct{ q, want string }{
		{"", "b"},
		{"?include=accounts,budget", "b"},
		{"?include=trace&txid=X", "c"},
		{"?include=reads,trace&txid=X", "c"},
	} {
		q, want := tc.q, tc.want
		f := scFleet(t, scCaps(groupIncludes|domain.IncludeBudget|domain.IncludeReads), trace)
		rec := f.do(http.MethodGet, stateChangesPath+q, nil)
		if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 200 || up != want {
			t.Errorf("%q: %d upstream %q, want %q: %s", q, rec.Code, up, want, rec.Body.String())
		}
	}
}

// With no node that takes an include value, a node that serves state
// changes answers with its own 400, as it would alone.
func TestStateChangesIncludeNobodyTakes(t *testing.T) {
	f := scFleet(t, ttCaps, scCaps(groupIncludes))
	rec := f.do(http.MethodGet, stateChangesPath+"?include=trace&txid=X", nil)
	if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 400 || up != "c" || !strings.Contains(rec.Body.String(), "include=trace needs node config EnableDeveloperAPI") {
		t.Fatalf("include nobody takes: %d upstream %q %s", rec.Code, up, rec.Body.String())
	}
}

// The only node that takes an include value is a round behind: the client
// gets a 503 to retry, not another node's lasting refusal of the value, and
// the request reaches that node once it has the round.
func TestStateChangesIncludeNodeBehind(t *testing.T) {
	trace := scCaps(groupIncludes | domain.IncludeTrace)
	trace.DeveloperAPI = true
	opts := RouterOptions{Mode: domain.ModeFallback, RetryBudget: 1, SyncTolerance: 2}
	f := newSimPair(t, scCaps(groupIncludes), trace, opts, DirectoryOptions{SyncTolerance: 2})
	f.b.SetRound(101)
	f.h.inject(addrA, signedHeartbeat(t, f.h.peerKey, "b", 2, 101, scCaps(groupIncludes)))
	waitFor(t, 2*time.Second, func() bool { return upstreamRound(f, "b") == 101 })
	const path = "/v2/deltas/101/txn/state-changes?include=trace&txid=X"
	rec := f.do(http.MethodGet, path, nil)
	if rec.Code != 503 || f.b.Hits("/v2/deltas/101/txn/state-changes") != 0 {
		t.Fatalf("trace node behind: %d upstream %q %s", rec.Code, rec.Header().Get("X-Algod-Mesh-Upstream"), rec.Body.String())
	}
	f.c.SetRound(101)
	f.h.inject(addrB, signedHeartbeat(t, f.cKey, "c", 2, 101, trace))
	waitFor(t, 2*time.Second, func() bool { return upstreamRound(f, "c") == 101 })
	rec = f.do(http.MethodGet, path, nil)
	if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 200 || up != "c" {
		t.Fatalf("trace node caught up: %d upstream %q %s", rec.Code, up, rec.Body.String())
	}
}

func upstreamRound(f *simFleet, id string) uint64 {
	cands, _ := f.h.d.Snapshot()
	for _, u := range cands {
		if u.ID == id {
			return u.LastRound
		}
	}
	return 0
}

// State changes share the heavy slots: a busy node is passed over for them
// for its Retry-After, and a 504 past its time limit reaches the client
// without counting against it or being run again.
func TestStateChangesBusyAndTimeLimit(t *testing.T) {
	f := scFleet(t, scCaps(groupIncludes), scCaps(groupIncludes))
	f.b.SetPathStatus(stateChangesPath, 503)
	f.b.SetPathRetryAfter(stateChangesPath, 5)
	rec := f.do(http.MethodGet, stateChangesPath, nil)
	if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 200 || up != "c" {
		t.Fatalf("b busy: %d upstream %q %s", rec.Code, up, rec.Body.String())
	}
	if s := f.stats("b"); !s.BusySlots.Has(domain.SlotHeavy) || s.BreakerOpen || s.ErrorRate != 0 {
		t.Fatalf("b after its busy answer: %+v", s)
	}
	f2 := scFleet(t, scCaps(groupIncludes), scCaps(groupIncludes))
	f2.b.SetPathStatus(stateChangesPath, 504)
	rec = f2.do(http.MethodGet, stateChangesPath, nil)
	if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 504 || up != "b" || f2.c.Hits(stateChangesPath) != 0 {
		t.Fatalf("b past its time limit: %d upstream %q, c asked %d times", rec.Code, up, f2.c.Hits(stateChangesPath))
	}
	if s := f2.stats("b"); s.ErrorRate != 0 || s.BreakerOpen {
		t.Fatalf("b after its 504: %+v", s)
	}
}

// An external that refuses a round it should have reached may answer from a
// backend behind the one probed: its answer is withheld, but its history is
// not taken for untrusted, nor its rounds for ignored.
func TestStateChangesExternalRoundMissing(t *testing.T) {
	ttOn := true
	ext := fakealgod.New(fakealgod.Options{ID: "ext", StartRound: 100, TimeTravel: true,
		Simulate: &domain.SimulateLimits{MaxTxnGroups: 1, StateChanges: domain.StateChangesLimits{Served: true}}})
	t.Cleanup(ext.Close)
	ext.SetPathStatus(stateChangesPath, 404)
	ext.SetPathMessage(stateChangesPath, "failed retrieving State Delta: round 50 is not in the history")
	h := newHarnessWith(t, DirectoryOptions{Externals: []ExternalUpstream{{Name: "ext", URL: ext.URL(), Token: ext.Token(), HealthCheck: 5 * time.Second,
		Capabilities: &domain.CapabilityOverrides{TimeTravel: &ttOn, Simulate: &domain.SimulateLimits{MaxTxnGroups: 1,
			StateChanges: domain.StateChangesLimits{Served: true}}}}}}, algodhttp.Factory{})
	r := NewRouter(RouterOptions{Mode: domain.ModeFallback, RetryBudget: 1},
		h.d, h.d.monitor, proxy.New(nil), nil, h.d.stats, h.fc, logging.Nop{}, h.m, nil, nil)
	f := &simFleet{r: r, h: h}
	rec := f.do(http.MethodGet, stateChangesPath, nil)
	if rec.Code == 404 || ext.Hits(stateChangesPath) != 1 {
		t.Fatalf("external refusal: %d, asked %d times: %s", rec.Code, ext.Hits(stateChangesPath), rec.Body.String())
	}
	if s := f.stats("ext"); s.HistoryUntrusted || s.RoundUnhonored || s.ErrorRate != 0 {
		t.Fatalf("external after its refusal: %+v", s)
	}
	if !strings.Contains(h.metricsText(), "mesh_upstream_round_missing") {
		t.Fatal("refusal not counted")
	}
}

// A fleet without the route answers 404, as a node without it does, so that
// a client sees the fleet lacks it rather than a 503 to retry.
func TestStateChangesFleetWithout(t *testing.T) {
	for name, caps := range map[string]domain.Capabilities{"stock": {}, "time travel": ttCaps} {
		f := scFleet(t, caps, caps)
		rec := f.do(http.MethodGet, stateChangesPath, nil)
		if rec.Code != 404 || !strings.Contains(rec.Body.String(), noStateChangesMessage) {
			t.Errorf("%s fleet: %d %s", name, rec.Code, rec.Body.String())
		}
	}
}

// A round past every node gets dtalgorand's 404 for a round its history
// does not hold.
func TestStateChangesRoundAhead(t *testing.T) {
	f := scFleet(t, scCaps(groupIncludes), scCaps(groupIncludes))
	rec := f.do(http.MethodGet, "/v2/deltas/500/txn/state-changes", nil)
	if rec.Code != 404 || !strings.Contains(rec.Body.String(), "failed retrieving State Delta: round 500 is not in the history") {
		t.Fatalf("round ahead: %d %s", rec.Code, rec.Body.String())
	}
}

// A node that says a round it has reached is not in its history has an
// untrusted one: its 404 is withheld, the next node answers, and the node
// is passed over for the history without counting against it.
func TestStateChangesUntrustedRetriesElsewhere(t *testing.T) {
	f := scFleet(t, scCaps(groupIncludes), scCaps(groupIncludes))
	f.b.SetPathStatus(stateChangesPath, 404)
	f.b.SetPathMessage(stateChangesPath, "failed retrieving State Delta: round 50 is not in the history")
	rec := f.do(http.MethodGet, stateChangesPath, nil)
	if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 200 || up != "c" {
		t.Fatalf("state changes: %d upstream %q %s", rec.Code, up, rec.Body.String())
	}
	if s := f.stats("b"); !s.HistoryUntrusted || s.BreakerOpen || s.ErrorRate != 0 {
		t.Fatalf("b after its 404: %+v", s)
	}
	// Another 404, an unknown txid, is the client's.
	f2 := scFleet(t, scCaps(groupIncludes), scCaps(groupIncludes))
	f2.b.SetPathStatus(stateChangesPath, 404)
	f2.b.SetPathMessage(stateChangesPath, "failed retrieving State Delta: transaction X is not in round 50")
	rec = f2.do(http.MethodGet, stateChangesPath+"?txid=X", nil)
	if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 404 || up != "b" {
		t.Fatalf("unknown txid: %d upstream %q %s", rec.Code, up, rec.Body.String())
	}
	if s := f2.stats("b"); s.HistoryUntrusted {
		t.Fatalf("b after an unknown txid: %+v", s)
	}
}

// The fleet's capabilities report state-changes and simulate's
// exec-trace-config, read from the nodes' own answers.
func TestStateChangesCapabilities(t *testing.T) {
	c := scCaps(groupIncludes | domain.IncludeBudget)
	c.Simulate.StateChangeAppID = true
	f := scFleet(t, ttCaps, c)
	rec := f.do(http.MethodGet, "/v2/capabilities", nil)
	var got struct {
		Simulate struct {
			ExecTraceConfig []string `json:"exec-trace-config"`
		} `json:"simulate"`
		StateChanges *struct {
			MaxResponseBytes uint64   `json:"max-response-bytes"`
			Include          []string `json:"include"`
		} `json:"state-changes"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); rec.Code != 200 || err != nil {
		t.Fatalf("capabilities: %d %v %s", rec.Code, err, rec.Body.String())
	}
	if got.StateChanges == nil || got.StateChanges.MaxResponseBytes != 1<<24 || strings.Join(got.StateChanges.Include, ",") != "accounts,assets,apps,budget" ||
		strings.Join(got.Simulate.ExecTraceConfig, ",") != "state-change-app-id" {
		t.Fatalf("capabilities %s", rec.Body.String())
	}
}

// A simulation that asks for state-change-app-id goes to a node that takes
// it; one that does not goes where it would anyway.
func TestSimulateStateChangeAppIDRouting(t *testing.T) {
	b := domain.Capabilities{DeveloperAPI: true, Simulate: &domain.SimulateLimits{MaxTxnGroups: 4, ReadsRemoved: true}}
	c := domain.Capabilities{DeveloperAPI: true, Simulate: &domain.SimulateLimits{MaxTxnGroups: 4, ReadsRemoved: true, StateChangeAppID: true}}
	f := scFleet(t, b, c)
	trace := `{"txn-groups":[{"txns":[]}],"exec-trace-config":{"enable":true,"state-change":true%s}}`
	if up := f.simulate(t, strings.Replace(trace, "%s", "", 1)); up != "b" {
		t.Fatalf("trace without app id went to %q, want b", up)
	}
	if up := f.simulate(t, strings.Replace(trace, "%s", `,"state-change-app-id":true`, 1)); up != "c" {
		t.Fatalf("app id went to %q, want c", up)
	}
}
