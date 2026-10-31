package domain

import (
	"strings"
	"testing"
)

var dtQuery = QueryLimits{MaxCalls: 1024, MaxCallBudget: 320000, MaxCallLogBytes: 65536, MaxCallStateBytes: 64 << 20, MaxResponseBytes: 64 << 20, MaxRounds: 16}

// queryCaps is a dtalgorand node with queries.
func queryCaps() Capabilities {
	l := dtLimits
	l.Query = dtQuery
	return Capabilities{Simulate: &l}
}

func queryClass(b QueryBody) RequestClass {
	c := Classify("POST", "/v2/query", "")
	c.ApplyQuery(b)
	return c
}

func TestClassifyQuery(t *testing.T) {
	c := Classify("POST", "/v2/query", "format=msgpack")
	if !c.Query || !c.Idempotent || c.Simulate || c.LocalOnly || c.QueryNeeds != nil {
		t.Fatalf("query: %+v", c)
	}
	if c.Slot() != SlotHeavy || !c.Evaluates() || c.StatKey() != "query" {
		t.Fatalf("query: slot %v, evaluates %v, stat key %q", c.Slot(), c.Evaluates(), c.StatKey())
	}
	// No other method or path is a query: any node answers 404 or 405.
	for _, tc := range [][2]string{{"GET", "/v2/query"}, {"POST", "/v2/query/x"}} {
		if c := Classify(tc[0], tc[1], ""); c.Query {
			t.Errorf("%s %s classified as a query", tc[0], tc[1])
		}
	}
}

func TestApplyQuery(t *testing.T) {
	c := queryClass(QueryBody{Calls: make([]struct{}, 3)})
	if c.Round != nil || c.QueryNeeds == nil || c.QueryNeeds.Calls != 3 || c.QueryNeeds.Rounds != 0 || c.QueryNeeds.Latest {
		t.Fatalf("no rounds: %+v %+v", c, c.QueryNeeds)
	}
	c = queryClass(QueryBody{Rounds: []uint64{40, 0, 12, 40}, Budget: 1000})
	if c.Round == nil || *c.Round != 12 || c.RoundHigh != 40 || c.RoundWindow != WindowState || !c.HistoryAnyRound {
		t.Fatalf("rounds: %+v", c)
	}
	if n := c.QueryNeeds; !n.RoundsSpan || !n.Latest || n.Rounds != 4 || n.Budget != 1000 || n.DeveloperAPI {
		t.Fatalf("rounds needs: %+v", n)
	}
	if c = queryClass(QueryBody{Rounds: []uint64{0, 0}}); c.Round != nil || c.QueryNeeds.RoundsSpan || !c.QueryNeeds.Latest {
		t.Fatalf("latest only: %+v %+v", c, c.QueryNeeds)
	}
	if c = queryClass(QueryBody{Protocol: "future"}); !c.QueryNeeds.DeveloperAPI {
		t.Fatalf("protocol future: %+v", c.QueryNeeds)
	}
	if c = queryClass(QueryBody{Protocol: "latest"}); c.QueryNeeds.DeveloperAPI {
		t.Fatalf("protocol latest: %+v", c.QueryNeeds)
	}
	c = queryClass(QueryBody{Calls: make([]struct{}, 5), Rounds: []uint64{3, 9}, Budget: 700})
	for _, want := range []string{"query", "round=3", "round_high=9", "query_calls=5", "query_rounds=2", "query_budget=700"} {
		if !strings.Contains(c.String(), want) {
			t.Errorf("class %q lacks %q", c.String(), want)
		}
	}
}

func TestQueryNeedsFits(t *testing.T) {
	dt := queryCaps()
	dtDev := dt
	dtDev.DeveloperAPI = true
	noQuery := Capabilities{Simulate: &dtLimits}
	for _, tc := range []struct {
		name string
		n    QueryNeeds
		caps Capabilities
		want bool
	}{
		{"stock", QueryNeeds{Calls: 1}, Capabilities{}, false},
		{"dtalgorand without queries", QueryNeeds{Calls: 1}, noQuery, false},
		{"within limits", QueryNeeds{Calls: 1024, Rounds: 16, Budget: 320000}, dt, true},
		{"too many calls", QueryNeeds{Calls: 1025}, dt, false},
		{"too many rounds", QueryNeeds{Rounds: 17}, dt, false},
		{"budget too large", QueryNeeds{Budget: 320001}, dt, false},
		{"future without dev", QueryNeeds{DeveloperAPI: true}, dt, false},
		{"future with dev", QueryNeeds{DeveloperAPI: true}, dtDev, true},
	} {
		if got := tc.n.Fits(tc.caps); got != tc.want {
			t.Errorf("%s: fits %v, want %v", tc.name, got, tc.want)
		}
	}
}

// Overrides are read as the node reads its config.
func TestQueryLimitsClamped(t *testing.T) {
	c := Capabilities{Simulate: &SimulateLimits{Query: QueryLimits{MaxCalls: 5, MaxRounds: 1000, MaxCallBudget: 1 << 40}}}
	if got := c.QueryLimits(); got.MaxRounds != 256 || got.MaxCallBudget != 320_000_000 {
		t.Fatalf("clamped %+v", got)
	}
	c = Capabilities{Simulate: &SimulateLimits{Query: QueryLimits{MaxCalls: 5}}}
	if got := c.QueryLimits(); got.MaxRounds != 1 || got.MaxCallBudget != 1 {
		t.Fatalf("zeros %+v", got)
	}
	if !c.Queries() || (Capabilities{Simulate: &SimulateLimits{Query: QueryLimits{MaxRounds: 4}}}).Queries() {
		t.Fatal("queries are told by max calls")
	}
	if got := (Capabilities{}).QueryLimits(); got != (QueryLimits{}) {
		t.Fatalf("stock %+v", got)
	}
}

func TestEligibleQuery(t *testing.T) {
	stock := Upstream{Health: HealthSynced, LastRound: 100, Caps: Capabilities{MaxAcctLookback: 4}}
	plain := Upstream{Health: HealthSynced, LastRound: 100, Caps: queryCaps()}
	plain.Caps.MaxAcctLookback = 4
	tt := plain
	tt.Caps.TimeTravel = true
	lagging := tt
	lagging.Health, lagging.LastRound = HealthLagging, 90

	unread := Classify("POST", "/v2/query", "")
	if Eligible(stock, unread, 100, 2) || !Eligible(plain, unread, 100, 2) {
		t.Fatal("a query whose body was not read still needs a node with queries")
	}
	past := queryClass(QueryBody{Rounds: []uint64{12, 40}})
	if Eligible(plain, past, 100, 2) || !Eligible(tt, past, 100, 2) {
		t.Fatal("past rounds need the history")
	}
	if !Eligible(plain, queryClass(QueryBody{Rounds: []uint64{97, 100}}), 100, 2) {
		t.Fatal("rounds within the state window refused")
	}
	if !Eligible(lagging, past, 100, 2) || Eligible(lagging, queryClass(QueryBody{Rounds: []uint64{12, 0}}), 100, 2) {
		t.Fatal("a lagging node serves past rounds, but not the latest")
	}
	if Eligible(lagging, unread, 100, 2) {
		t.Fatal("a lagging node took a query at the latest round")
	}
	anyNode := unread
	anyNode.QueryAnyNode = true
	if !Eligible(stock, anyNode, 100, 2) {
		t.Fatal("the last relaxation reaches any node")
	}
}

func TestRelaxedQuery(t *testing.T) {
	r := queryClass(QueryBody{Calls: make([]struct{}, 2), Rounds: []uint64{12, 40}}).Relaxed()
	if len(r) != 3 {
		t.Fatalf("relaxations: %+v", r)
	}
	if r[0].Round != nil || r[0].RoundHigh != 0 || r[0].HistoryAnyRound || r[0].QueryNeeds == nil || r[0].QueryNeeds.Calls != 2 || r[0].QueryAnyNode {
		t.Fatalf("first drops the rounds: %+v %+v", r[0], r[0].QueryNeeds)
	}
	if !r[1].Query || r[1].QueryNeeds != nil || r[1].QueryAnyNode {
		t.Fatalf("second drops the limits: %+v", r[1])
	}
	if !r[2].Query || !r[2].QueryAnyNode {
		t.Fatalf("last reaches any node: %+v", r[2])
	}
	if r := queryClass(QueryBody{Calls: make([]struct{}, 2)}).Relaxed(); len(r) != 2 {
		t.Fatalf("without rounds: %+v", r)
	}
	if r := Classify("POST", "/v2/query", "").Relaxed(); len(r) != 1 || !r[0].QueryAnyNode {
		t.Fatalf("unread body: %+v", r)
	}
	if r := r[len(r)-1].Relaxed(); r != nil {
		t.Fatalf("relaxed past the last step: %+v", r)
	}
}

func TestFleetQueryLimits(t *testing.T) {
	small := dtLimits
	small.Query = QueryLimits{MaxCalls: 64, MaxCallBudget: 700, MaxRounds: 256}
	us := []Upstream{
		{ID: "stock", Health: HealthSynced, LastRound: 100},
		{ID: "noquery", Health: HealthSynced, LastRound: 100, Caps: Capabilities{Simulate: &dtLimits}},
	}
	got, ok := FleetSimulateLimits(us, 100, 2)
	if !ok || got.Query != nil {
		t.Fatalf("no query node: %+v %v", got, ok)
	}
	us = append(us,
		Upstream{ID: "small", Health: HealthSynced, LastRound: 100, Caps: Capabilities{Simulate: &small}},
		Upstream{ID: "dt", Health: HealthSynced, LastRound: 100, Caps: queryCaps()})
	got, _ = FleetSimulateLimits(us, 100, 2)
	want := FleetQueryLimits{MaxCalls: 1024, MaxCallBudget: 320000, MaxCallLogBytes: 65536, MaxCallStateBytes: 64 << 20, MaxResponseBytes: 64 << 20, MaxRounds: 16}
	if got.Query == nil || *got.Query != want {
		t.Fatalf("query limits %+v, want %+v (one node's, the most calls)", got.Query, want)
	}
	us[3].Health = HealthOffline
	if got, _ = FleetSimulateLimits(us, 100, 2); got.Query == nil || got.Query.MaxCalls != 64 {
		t.Fatalf("with dt down: %+v", got.Query)
	}
}

func TestQueryCapsCheck(t *testing.T) {
	caps := QueryCaps{MaxCalls: 8, MaxRounds: 2, MaxBudget: 1000}
	for _, tc := range []struct {
		n    QueryNeeds
		want string
	}{
		{QueryNeeds{Calls: 8, Rounds: 2, Budget: 1000}, ""},
		{QueryNeeds{Calls: 9, Budget: 1}, "calls"},
		{QueryNeeds{Calls: 1, Rounds: 3, Budget: 1}, "rounds"},
		{QueryNeeds{Calls: 1, Budget: 1001}, "budget"},
		{QueryNeeds{Calls: 1}, "set its budget"},
	} {
		got := caps.Check(tc.n)
		if (got == "") != (tc.want == "") || !strings.Contains(got, tc.want) {
			t.Errorf("%+v: %q, want %q", tc.n, got, tc.want)
		}
	}
	if got := (QueryCaps{}).Check(QueryNeeds{Calls: 1 << 20, Rounds: 256}); got != "" {
		t.Errorf("no caps: %q", got)
	}
}
