package app

import (
	"bytes"
	"crypto/ed25519"
	crand "crypto/rand"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/algorand/go-codec/codec"

	"github.com/d13co/algod-mesh/internal/adapters/algodhttp"
	"github.com/d13co/algod-mesh/internal/adapters/logging"
	"github.com/d13co/algod-mesh/internal/adapters/proxy"
	"github.com/d13co/algod-mesh/internal/domain"
	"github.com/d13co/algod-mesh/internal/fakealgod"
)

// simFleet is a router in front of two peers: b, a stock algod, and c, a
// dtalgorand with the given simulate limits (nil: stock as well).
type simFleet struct {
	r    *Router
	b, c *fakealgod.Node
	h    *harness
	cKey ed25519.PrivateKey
}

func newSimFleet(t *testing.T, cCaps domain.Capabilities) *simFleet {
	t.Helper()
	return newSimFleetWith(t, cCaps, RouterOptions{Mode: domain.ModeFallback, RetryBudget: 1})
}

func newSimFleetWith(t *testing.T, cCaps domain.Capabilities, o RouterOptions) *simFleet {
	t.Helper()
	return newSimFleetDir(t, cCaps, o, DirectoryOptions{})
}

// newSimFleetDir builds the fleet; bOpts, if any, change b's fake node.
func newSimFleetDir(t *testing.T, cCaps domain.Capabilities, o RouterOptions, do DirectoryOptions, bOpts ...func(*fakealgod.Options)) *simFleet {
	t.Helper()
	return newSimPair(t, domain.Capabilities{}, cCaps, o, do, bOpts...)
}

// newSimPair is newSimFleetDir with b's capabilities given too.
func newSimPair(t *testing.T, bCaps, cCaps domain.Capabilities, o RouterOptions, do DirectoryOptions, bOpts ...func(*fakealgod.Options)) *simFleet {
	t.Helper()
	h := newHarnessWith(t, do, algodhttp.Factory{})
	bo := fakealgod.Options{ID: "b", StartRound: 100, Simulate: bCaps.Simulate, TimeTravel: bCaps.TimeTravel}
	for _, f := range bOpts {
		f(&bo)
	}
	b := fakealgod.New(bo)
	t.Cleanup(b.Close)
	c := fakealgod.New(fakealgod.Options{ID: "c", StartRound: 100, Simulate: cCaps.Simulate, TimeTravel: cCaps.TimeTravel})
	t.Cleanup(c.Close)
	cPub, cKey, _ := ed25519.GenerateKey(crand.Reader)
	recB := h.peerRecord(addrA)
	recB.Endpoints, recB.Token = []string{b.URL()}, b.Token()
	recC := domain.NodeRecord{ID: "c", Network: "n", Agent: domain.AgentInfo{Addrs: []string{addrB}, PubKey: []byte(cPub)},
		Endpoints: []string{c.URL()}, Token: c.Token()}
	h.d.SetRecords([]domain.NodeRecord{recB, recC})
	r := NewRouter(o,
		h.d, h.d.monitor, proxy.New(nil), nil, h.d.stats, h.fc, logging.Nop{}, h.m, nil, nil)
	bCaps.MaxAcctLookback, cCaps.MaxAcctLookback = 4, 4
	h.inject(addrA, signedHeartbeat(t, h.peerKey, "b", 1, 100, bCaps))
	h.inject(addrB, signedHeartbeat(t, cKey, "c", 1, 100, cCaps))
	waitFor(t, 2*time.Second, func() bool {
		cands, _ := h.d.Snapshot()
		up := 0
		for _, u := range cands {
			if u.Health.Reachable() {
				up++
			}
		}
		return up == 2
	})
	return &simFleet{r: r, b: b, c: c, h: h, cKey: cKey}
}

func (f *simFleet) do(method, path string, body io.Reader) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	f.r.ServeHTTP(rec, httptest.NewRequest(method, path, body))
	return rec
}

func (f *simFleet) simulate(t *testing.T, body string) string {
	t.Helper()
	rec := f.do(http.MethodPost, "/v2/transactions/simulate", strings.NewReader(body))
	if rec.Code != 200 {
		t.Fatalf("simulate %s: %d %s", body, rec.Code, rec.Body.String())
	}
	return rec.Header().Get("X-Algod-Mesh-Upstream")
}

var testLimits = domain.SimulateLimits{MaxTxnGroups: 16, MaxRounds: 8, MaxExtraOpcodeBudget: 1_000_000, ResourceLimitMultiplier: 2, TimeLimitSeconds: 30}

// A simulate request that only dtalgorand can run reaches the dtalgorand
// node; a stock node would reject it. One that fits stock algod still goes
// where routing would send it anyway.
func TestSimulateRoutesForkRequestsToDtalgorand(t *testing.T) {
	f := newSimFleet(t, domain.Capabilities{Simulate: &testLimits})
	if up := f.simulate(t, `{"txn-groups":[{"txns":[]}]}`); up != "b" {
		t.Fatalf("stock request went to %q, want the first peer", up)
	}
	for _, body := range []string{
		`{"txn-groups":[{"txns":[]},{"txns":[]}]}`,
		`{"txn-groups":[{"txns":[]}],"rounds":[0,0]}`,
		`{"txn-groups":[{"txns":[]}],"extra-opcode-budget":500000}`,
		`{"txn-groups":[{"txns":[]}],"parallel-groups":true}`,
		`{"txn-groups":[{"txns":[]}],"protocol":"latest"}`,
	} {
		if up := f.simulate(t, body); up != "c" {
			t.Errorf("%s went to %q, want c", body, up)
		}
	}

	// The SDKs post msgpack; the groups are counted all the same.
	type group struct {
		Txns []any `codec:"txns"`
	}
	type simReq struct {
		TxnGroups []group `codec:"txn-groups"`
	}
	var buf bytes.Buffer
	if err := codec.NewEncoder(&buf, &codec.MsgpackHandle{}).Encode(simReq{TxnGroups: []group{{}, {}, {}}}); err != nil {
		t.Fatal(err)
	}
	rec := f.do(http.MethodPost, "/v2/transactions/simulate", &buf)
	if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 200 || up != "c" {
		t.Fatalf("msgpack three groups: %d upstream %q", rec.Code, up)
	}
	if n := f.b.Hits("/v2/transactions/simulate"); n != 1 {
		t.Fatalf("stock node saw %d simulations, want only the stock one", n)
	}
}

// When no node fits, the request is relaxed and sent on, so that a node
// answers it (with its own 400, on a real algod) rather than the router with
// a 503.
func TestSimulateRelaxedWhenNothingFits(t *testing.T) {
	f := newSimFleet(t, domain.Capabilities{Simulate: &testLimits})
	groups := strings.Repeat(`{"txns":[]},`, 20)
	if up := f.simulate(t, `{"txn-groups":[`+strings.TrimSuffix(groups, ",")+`]}`); up == "" {
		t.Fatal("no upstream for a request over every limit")
	}
	// Rounds beyond every node are dropped before the rest: the dtalgorand
	// node, which accepts rounds, reports the missing ones per round.
	if up := f.simulate(t, `{"txn-groups":[{"txns":[]}],"rounds":[50,5000]}`); up != "c" {
		t.Fatalf("rounds past every node went to %q, want c", up)
	}
	// A single round no node holds stays refused: a node without it would
	// answer the latest state or an error.
	rec := f.do(http.MethodPost, "/v2/transactions/simulate", strings.NewReader(`{"txn-groups":[],"round":5000}`))
	if rec.Code != 503 {
		t.Fatalf("future round: %d %s", rec.Code, rec.Body.String())
	}
}

// The rounds of a rounds list must all be on the node that runs it.
func TestSimulateRoundsNeedEveryRound(t *testing.T) {
	f := newSimFleet(t, domain.Capabilities{Simulate: &testLimits, TimeTravel: true})
	// b, a plain node at 100 with lookback 4, holds 96..100; only c the rest.
	if up := f.simulate(t, `{"txn-groups":[],"round":97}`); up != "b" {
		t.Fatalf("round in the state window went to %q, want b", up)
	}
	if up := f.simulate(t, `{"txn-groups":[],"rounds":[97,12]}`); up != "c" {
		t.Fatalf("rounds reaching past the window went to %q, want c", up)
	}
}

// The router answers the capabilities for the fleet: what a request sent to
// it can ask for, since it routes each request to a node that allows it.
func TestCapabilitiesForFleet(t *testing.T) {
	f := newSimFleet(t, domain.Capabilities{Simulate: &testLimits, TimeTravel: true})
	rec := f.do(http.MethodGet, "/v2/capabilities", nil)
	if rec.Code != 200 {
		t.Fatalf("capabilities: %d %s", rec.Code, rec.Body.String())
	}
	var got domain.FleetCapabilitiesReply
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	rounds, multiplier := uint64(8), uint64(1)
	want := domain.FleetCapabilitiesReply{TimeTravel: true, TimeLimitSeconds: 30,
		Simulate: domain.FleetSimulateLimits{MaxTxnGroups: 16, MaxRounds: &rounds, MaxExtraOpcodeBudget: 1_000_000, ResourceLimitMultiplier: &multiplier}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("capabilities %+v, want %+v", got, want)
	}
	// dtalgorand's own form: the simulate limits nested, no query object
	// without a node that has queries.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := raw["max-txn-groups"]; ok || raw["simulate"] == nil || raw["query"] != nil {
		t.Fatalf("capabilities shape: %s", rec.Body.String())
	}
	if n := f.b.Hits("/v2/capabilities") + f.c.Hits("/v2/capabilities"); n != 0 {
		t.Fatalf("capabilities forwarded %d times", n)
	}
}

// fleetSimulate is the simulate object of the router's capabilities answer.
func (f *simFleet) fleetSimulate(t *testing.T) map[string]any {
	t.Helper()
	rec := f.do(http.MethodGet, "/v2/capabilities", nil)
	var got struct {
		Simulate map[string]any `json:"simulate"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || rec.Code != 200 || got.Simulate == nil {
		t.Fatalf("capabilities: %d %s", rec.Code, rec.Body.String())
	}
	return got.Simulate
}

// newTestLimits is a dtalgorand without simulate's read extensions, with
// testLimits' groups and budget.
var newTestLimits = domain.SimulateLimits{MaxTxnGroups: 16, MaxExtraOpcodeBudget: 1_000_000, TimeLimitSeconds: 30, ReadsRemoved: true}

// readBodies are simulate requests only a dtalgorand from before POST
// /v2/query runs, with the field its successor names in its refusal.
var readBodies = []struct{ body, field string }{
	{`{"txn-groups":[{"txns":[]}],"parallel-groups":true}`, "parallel-groups"},
	{`{"txn-groups":[{"txns":[]}],"skip-validity-window":true}`, "skip-validity-window"},
	{`{"txn-groups":[{"txns":[]}],"rewrite-validity-window":true}`, "rewrite-validity-window"},
	{`{"txn-groups":[{"txns":[]}],"protocol":"latest"}`, "protocol"},
	{`{"txn-groups":[{"txns":[]}],"rounds":[0,0]}`, "rounds"},
	{`{"txn-groups":[{"txns":[]}],"rounds":[97,99]}`, "rounds"},
}

// During a rollout, requests that name the read extensions go to the older
// node that still takes them, the rest to either, and the fleet answer is
// the older node's while it has rounds.
func TestSimulateReadsRoutedToOlderNode(t *testing.T) {
	f := newSimPair(t, domain.Capabilities{Simulate: &testLimits}, domain.Capabilities{Simulate: &newTestLimits},
		RouterOptions{Mode: domain.ModeFallback, RetryBudget: 1}, DirectoryOptions{})
	for _, rb := range readBodies {
		if up := f.simulate(t, rb.body); up != "b" {
			t.Errorf("%s went to %q, want the older node", rb.body, up)
		}
	}
	if n := f.c.Hits("/v2/transactions/simulate"); n != 0 {
		t.Fatalf("the newer node saw %d requests with read extensions", n)
	}
	// Both run many groups and a large budget.
	if up := f.simulate(t, `{"txn-groups":[{"txns":[]},{"txns":[]}],"extra-opcode-budget":500000}`); up == "" {
		t.Fatal("no upstream for what both nodes run")
	}
	if got := f.fleetSimulate(t); got["max-rounds"] != float64(8) || got["resource-limit-multiplier"] != float64(1) {
		t.Fatalf("capabilities of a mixed fleet: %+v", got)
	}
}

// With no node that takes the read extensions, the request still reaches a
// dtalgorand, not the stock node, and the client gets its 400 pointing to
// POST /v2/query. The fleet answer leaves out max-rounds and the
// multiplier, as that node does.
func TestSimulateReadsRefusedByNewerNode(t *testing.T) {
	f := newSimFleet(t, domain.Capabilities{Simulate: &newTestLimits, TimeTravel: true})
	for _, rb := range readBodies {
		rec := f.do(http.MethodPost, "/v2/transactions/simulate", strings.NewReader(rb.body))
		want := "simulate no longer takes " + rb.field + ": use POST /v2/query"
		if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 400 || up != "c" || !strings.Contains(rec.Body.String(), want) {
			t.Errorf("%s: %d from %q: %s, want the newer node's 400", rb.body, rec.Code, up, rec.Body.String())
		}
	}
	if n := f.b.Hits("/v2/transactions/simulate"); n != 0 {
		t.Fatalf("the stock node saw %d requests with read extensions", n)
	}
	// What it still takes runs: several groups at a past round.
	if up := f.simulate(t, `{"txn-groups":[{"txns":[]},{"txns":[]}],"round":50}`); up != "c" {
		t.Fatalf("groups at a past round went to %q", up)
	}
	got := f.fleetSimulate(t)
	if got["max-txn-groups"] != float64(16) {
		t.Fatalf("capabilities: %+v", got)
	}
	for _, k := range []string{"max-rounds", "resource-limit-multiplier"} {
		if _, ok := got[k]; ok {
			t.Errorf("capabilities carry %s for a node without it: %+v", k, got)
		}
	}
}

// An older dtalgorand with rounds disabled is reported with max-rounds 0,
// not without it: it still takes the other read extensions.
func TestCapabilitiesRoundsDisabled(t *testing.T) {
	f := newSimFleet(t, domain.Capabilities{Simulate: &domain.SimulateLimits{MaxTxnGroups: 16, ResourceLimitMultiplier: 4}})
	if got := f.fleetSimulate(t); got["max-rounds"] != float64(0) || got["resource-limit-multiplier"] != float64(1) {
		t.Fatalf("capabilities: %+v", got)
	}
	if up := f.simulate(t, `{"txn-groups":[{"txns":[]}],"parallel-groups":true}`); up != "c" {
		t.Fatalf("parallel-groups went to %q", up)
	}
}

// The limits' old path is gone from dtalgorand: the router does not answer
// it, and forwards it as any other path, for the node's own answer (a 404
// from dtalgorand; the fake echoes it).
func TestOldSimulateLimitsPathForwarded(t *testing.T) {
	f := newSimFleet(t, domain.Capabilities{Simulate: &testLimits, TimeTravel: true})
	rec := f.do(http.MethodGet, "/v2/transactions/simulate/limits", nil)
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || got["node"] == nil || got["simulate"] != nil {
		t.Fatalf("old limits path not the node's answer: %d %s", rec.Code, rec.Body.String())
	}
	if n := f.b.Hits("/v2/transactions/simulate/limits") + f.c.Hits("/v2/transactions/simulate/limits"); n != 1 {
		t.Fatalf("old limits path forwarded %d times, want once", n)
	}
}

// A fleet of stock algods has no capabilities endpoint, and the mesh has
// none either: the request is forwarded and answered 404.
func TestCapabilitiesForwardedWithoutDtalgorand(t *testing.T) {
	f := newSimFleet(t, domain.Capabilities{})
	if rec := f.do(http.MethodGet, "/v2/capabilities", nil); rec.Code != 404 {
		t.Fatalf("capabilities on a stock fleet: %d %s", rec.Code, rec.Body.String())
	}
}

// drainC has c announce that it is draining: up, but taking no requests.
func (f *simFleet) drainC(t *testing.T, caps domain.Capabilities) {
	t.Helper()
	caps.MaxAcctLookback = 4
	wire, err := domain.EncodeHeartbeat(f.cKey, domain.Heartbeat{NodeID: "c", Seq: 2, Online: true, LastRound: 100, Caps: caps, Draining: true})
	if err != nil {
		t.Fatal(err)
	}
	f.h.inject(addrB, wire)
	waitFor(t, 2*time.Second, func() bool {
		cands, _ := f.h.d.Snapshot()
		for _, u := range cands {
			if u.ID == "c" {
				return u.Draining
			}
		}
		return false
	})
}

// A fitting node that is only briefly out (draining here) gets the client a
// 503 to retry, not a stock node's rejection of what it cannot run.
func TestSimulateNotRelaxedWhileFittingNodeIsOut(t *testing.T) {
	caps := domain.Capabilities{Simulate: &testLimits}
	f := newSimFleet(t, caps)
	f.drainC(t, caps)
	rec := f.do(http.MethodPost, "/v2/transactions/simulate", strings.NewReader(`{"txn-groups":[{"txns":[]},{"txns":[]}]}`))
	if rec.Code != 503 {
		t.Fatalf("two groups while the dtalgorand node drains: %d %s", rec.Code, rec.Body.String())
	}
	if n := f.b.Hits("/v2/transactions/simulate"); n != 0 {
		t.Fatalf("stock node saw %d simulations", n)
	}
	// The capabilities are not the stock node's 404 either.
	if rec := f.do(http.MethodGet, "/v2/capabilities", nil); rec.Code != 503 {
		t.Fatalf("capabilities while the dtalgorand node drains: %d %s", rec.Code, rec.Body.String())
	}
}

// A simulation that times out is not run again on the next node.
func TestSimulateNotRetriedAfterTimeout(t *testing.T) {
	f := newSimFleetWith(t, domain.Capabilities{Simulate: &testLimits},
		RouterOptions{Mode: domain.ModeFallback, RetryBudget: 1, UpstreamTimeout: 200 * time.Millisecond, RequestTimeout: 2 * time.Second})
	f.b.SetLatency(time.Second) // past the upstream timeout, but answers in the end
	rec := f.do(http.MethodPost, "/v2/transactions/simulate", strings.NewReader(`{"txn-groups":[{"txns":[]}]}`))
	if rec.Code != 504 {
		t.Fatalf("timed-out simulation: %d %s", rec.Code, rec.Body.String())
	}
	if n := f.c.Hits("/v2/transactions/simulate"); n != 0 {
		t.Fatalf("timed-out simulation retried %d times", n)
	}
}

// With the only dtalgorand node excluded, the fleet answers as a stock one:
// the capabilities request is forwarded and gets algod's 404, and a time-travel
// request is told the fleet has no such node.
func TestExcludedDtalgorandIsAbsent(t *testing.T) {
	f := newSimFleetDir(t, domain.Capabilities{Simulate: &testLimits, TimeTravel: true},
		RouterOptions{Mode: domain.ModeFallback, RetryBudget: 1}, DirectoryOptions{Exclude: []string{"c"}})
	if rec := f.do(http.MethodGet, "/v2/capabilities", nil); rec.Code != 404 {
		t.Fatalf("capabilities with the dtalgorand node excluded: %d %s", rec.Code, rec.Body.String())
	}
	if n := f.c.Hits("/v2/capabilities"); n != 0 {
		t.Fatalf("excluded node hit %d times", n)
	}
	cands, _ := f.h.d.Snapshot()
	if anyTimeTravel(cands) {
		t.Fatal("excluded time-travel node counted")
	}
}

// A busy excluded node does not make the router tell a heavy request that
// every node able to serve it is busy: no retry would reach that node.
func TestExcludedBusyNodeIsNotReported(t *testing.T) {
	busy := domain.Upstream{ID: "x", Stats: domain.Stats{BusySlots: domain.Slots(0).With(domain.SlotHeavy)}}
	if !anyBusy([]domain.Upstream{busy}, domain.SlotHeavy) {
		t.Fatal("busy node not counted")
	}
	if anyBusy([]domain.Upstream{busy}, domain.SlotHolders) {
		t.Fatal("node busy with simulations counted for holders pages")
	}
	busy.Excluded = true
	if anyBusy([]domain.Upstream{busy}, domain.SlotHeavy) {
		t.Fatal("busy excluded node counted")
	}
}
