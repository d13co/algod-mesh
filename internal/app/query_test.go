package app

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/algorand/go-codec/codec"

	"github.com/d13co/algod-mesh/internal/domain"
)

// testQueryLimits is a dtalgorand with POST /v2/query.
var testQueryLimits = func() domain.SimulateLimits {
	l := testLimits
	l.Query = domain.QueryLimits{MaxCalls: 64, MaxCallBudget: 320000, MaxCallLogBytes: 65536, MaxCallStateBytes: 1 << 20, MaxResponseBytes: 1 << 20, MaxRounds: 4}
	return l
}()

func (f *simFleet) query(t *testing.T, body string) string {
	t.Helper()
	rec := f.do(http.MethodPost, "/v2/query", strings.NewReader(body))
	if rec.Code != 200 {
		t.Fatalf("query %s: %d %s", body, rec.Code, rec.Body.String())
	}
	return rec.Header().Get("X-Algod-Mesh-Upstream")
}

// A query goes to the node that has queries, wherever routing would send
// another request, whether its body is JSON or msgpack.
func TestQueryRoutesToQueryNode(t *testing.T) {
	f := newSimFleet(t, domain.Capabilities{Simulate: &testQueryLimits, TimeTravel: true})
	for _, body := range []string{
		`{"program":"AQ==","calls":[{}]}`,
		`{"app-id":5,"calls":[{},{}],"rounds":[0,97],"budget":1000}`,
		`{"program":"AQ==","calls":[{}],"rounds":[12]}`,
		`not a query`, // the node rejects it with its own message
	} {
		if up := f.query(t, body); up != "c" {
			t.Errorf("%s went to %q, want c", body, up)
		}
	}
	type call struct {
		Args [][]byte `codec:"args"`
	}
	type queryReq struct {
		Program []byte   `codec:"program"`
		Calls   []call   `codec:"calls"`
		Rounds  []uint64 `codec:"rounds"`
	}
	var buf bytes.Buffer
	if err := codec.NewEncoder(&buf, &codec.MsgpackHandle{}).Encode(queryReq{Program: []byte{1}, Calls: []call{{}, {}}, Rounds: []uint64{12, 40}}); err != nil {
		t.Fatal(err)
	}
	rec := f.do(http.MethodPost, "/v2/query?format=msgpack", &buf)
	if up := rec.Header().Get("X-Algod-Mesh-Upstream"); rec.Code != 200 || up != "c" {
		t.Fatalf("msgpack: %d upstream %q", rec.Code, up)
	}
	if n := f.b.Hits("/v2/query"); n != 0 {
		t.Fatalf("stock node saw %d queries", n)
	}
}

// A query beyond every node's limits still reaches a node with queries,
// which says what it does not allow. In a fleet without queries, a node
// answers 404, as algod does alone.
func TestQueryRelaxedWhenNothingFits(t *testing.T) {
	f := newSimFleet(t, domain.Capabilities{Simulate: &testQueryLimits})
	calls := strings.TrimSuffix(strings.Repeat(`{},`, 65), ",")
	if up := f.query(t, `{"program":"AQ==","calls":[`+calls+`]}`); up != "c" {
		t.Fatalf("too many calls went to %q, want c", up)
	}
	// c is a plain node at 100: round 12 is past its window, and 5000 past
	// every node. It reports them in their round's result.
	if up := f.query(t, `{"program":"AQ==","calls":[{}],"rounds":[12,5000]}`); up != "c" {
		t.Fatalf("rounds past every node went to %q, want c", up)
	}

	stock := newSimFleet(t, domain.Capabilities{Simulate: &testLimits})
	rec := stock.do(http.MethodPost, "/v2/query", strings.NewReader(`{"program":"AQ==","calls":[{}]}`))
	if rec.Code != 404 || rec.Header().Get("X-Algod-Mesh-Upstream") == "" {
		t.Fatalf("query on a fleet without queries: %d %s", rec.Code, rec.Body.String())
	}
}

// The node with queries being briefly out gets the client a 503 to retry,
// not a stock node's 404.
func TestQueryNotRelaxedWhileQueryNodeIsOut(t *testing.T) {
	caps := domain.Capabilities{Simulate: &testQueryLimits}
	f := newSimFleet(t, caps)
	f.drainC(t, caps)
	rec := f.do(http.MethodPost, "/v2/query", strings.NewReader(`{"program":"AQ==","calls":[{}]}`))
	if rec.Code != 503 {
		t.Fatalf("query while the query node drains: %d %s", rec.Code, rec.Body.String())
	}
	if n := f.b.Hits("/v2/query"); n != 0 {
		t.Fatalf("stock node saw %d queries", n)
	}
}

// The fleet's limits carry the query object of a node with queries.
func TestQueryLimitsForFleet(t *testing.T) {
	f := newSimFleet(t, domain.Capabilities{Simulate: &testQueryLimits, TimeTravel: true})
	rec := f.do(http.MethodGet, "/v2/transactions/simulate/limits", nil)
	if rec.Code != 200 {
		t.Fatalf("limits: %d %s", rec.Code, rec.Body.String())
	}
	var got struct {
		Query *domain.FleetQueryLimits `json:"query"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	want := domain.FleetQueryLimits{MaxCalls: 64, MaxCallBudget: 320000, MaxCallLogBytes: 65536, MaxCallStateBytes: 1 << 20, MaxResponseBytes: 1 << 20, MaxRounds: 4}
	if got.Query == nil || *got.Query != want {
		t.Fatalf("query limits %+v, want %+v", got.Query, want)
	}
}

// newLimitedQueryFleet is newLimitedFleet with queries on c.
func newLimitedQueryFleet(t *testing.T, free domain.Plan, perSecond float64) *simFleet {
	t.Helper()
	return newSimFleetWith(t, domain.Capabilities{Simulate: &testQueryLimits}, RouterOptions{Mode: domain.ModeFallback, RetryBudget: 1,
		AdminToken: "admin-token", RateLimit: &LimiterOptions{
			Plans: map[string]domain.Plan{"free": free}, AnonymousPlan: "free",
			Weights: testWeights, SimulatePerSecond: perSecond}})
}

// A query costs its weight per call and round, and a plan caps what one may
// ask for.
func TestRateLimitQueryCaps(t *testing.T) {
	free := domain.Plan{Minute: perMinute(20), Query: domain.QueryCaps{MaxCalls: 8, MaxRounds: 2, MaxBudget: 1000}}
	f := newLimitedQueryFleet(t, free, 0)
	for _, body := range []string{
		`{"program":"AQ==","calls":[{},{},{},{},{},{},{},{},{}],"budget":1}`,
		`{"program":"AQ==","calls":[{}],"rounds":[1,2,3],"budget":1}`,
		`{"program":"AQ==","calls":[{}],"budget":1001}`,
		`{"program":"AQ==","calls":[{}]}`, // the node's own budget, which may be over the cap
	} {
		if rec := f.call("POST", "/v2/query", "", clientA, body); rec.Code != 403 {
			t.Errorf("%s: %d %s", body, rec.Code, rec.Body.String())
		}
	}
	if rec := f.call("POST", "/v2/query", "", clientA, `not a query`); rec.Code != 400 {
		t.Errorf("unreadable body under caps: %d %s", rec.Code, rec.Body.String())
	}
	rec := f.call("POST", "/v2/query", "", clientA, `{"program":"AQ==","calls":[{},{},{},{}],"rounds":[0,0],"budget":1000}`)
	if rec.Code != 200 || rec.Header().Get(costHeader) != "8" {
		t.Fatalf("within the caps: %d, cost %q: %s", rec.Code, rec.Header().Get(costHeader), rec.Body.String())
	}
	// The unreadable request was charged its weight: 20 - 1 - 8 left.
	if got := rec.Header().Get("RateLimit"); !strings.HasPrefix(got, `"minute";r=11;`) {
		t.Fatalf("RateLimit %q, want 11 left", got)
	}
}

// A query is metered while it runs and cancelled once its client is
// overdrawn, as a simulation is.
func TestRateLimitCancelsQuery(t *testing.T) {
	f := newLimitedQueryFleet(t, domain.Plan{Minute: perMinute(10)}, 100)
	f.c.SetSimulate(10*time.Second, 0, 0)
	done := make(chan *httptest.ResponseRecorder)
	go func() { done <- f.call("POST", "/v2/query", "", clientA, `{"program":"AQ==","calls":[{}]}`) }()
	waitFor(t, 2*time.Second, func() bool { return f.c.Hits("/v2/query") == 1 })
	f.h.fc.Advance(time.Second)
	f.r.Limiter().tick()
	var rec *httptest.ResponseRecorder
	select {
	case rec = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("query not cancelled")
	}
	if rec.Code != 429 || rec.Header().Get(costHeader) != "101" {
		t.Fatalf("cancelled: %d, cost %q: %s", rec.Code, rec.Header().Get(costHeader), rec.Body.String())
	}
	waitFor(t, 2*time.Second, func() bool { return f.c.SimulatesCancelled() == 1 })
}

// A query the node reports its work for costs its weight plus that work.
func TestRateLimitQueryEffort(t *testing.T) {
	f := newLimitedQueryFleet(t, domain.Plan{Minute: perMinute(1000)}, 1)
	f.c.SetEffort("/v2/query", "opcodes=660000, lookups=0, history=0, scanned=0, response-bytes=0, eval-us=5")
	rec := f.call("POST", "/v2/query", "", clientA, `{"program":"AQ==","calls":[{},{}]}`)
	if rec.Code != 200 || rec.Header().Get(costHeader) != "4" || rec.Header().Get(domain.EffortHeader) != "" {
		t.Fatalf("billed client: %d, cost %q, effort %q", rec.Code, rec.Header().Get(costHeader), rec.Header().Get(domain.EffortHeader))
	}
}
