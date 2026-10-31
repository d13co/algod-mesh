package domain

import (
	"strings"
	"testing"
	"time"
)

var dtLimits = SimulateLimits{MaxTxnGroups: 16, MaxRounds: 8, MaxExtraOpcodeBudget: 1_000_000, ResourceLimitMultiplier: 4}

func simClass(b SimulateBody) RequestClass {
	c := Classify("POST", "/v2/transactions/simulate", "")
	c.ApplySimulate(b)
	return c
}

func TestClassifyCapabilities(t *testing.T) {
	if c := Classify("GET", "/v2/capabilities", ""); !c.Capabilities || c.Simulate || !c.Idempotent {
		t.Fatalf("capabilities: %+v", c)
	}
	if c := Classify("POST", "/v2/capabilities", ""); c.Capabilities || c.Simulate {
		t.Fatalf("POST capabilities is no capabilities request: %+v", c)
	}
	if c := Classify("GET", "/v2/capabilities/x", ""); c.Capabilities {
		t.Fatalf("a subpath is no capabilities request: %+v", c)
	}
	// The limits' old path is an unknown path now, neither the fleet's
	// answer nor a simulation.
	for _, m := range []string{"GET", "POST"} {
		if c := Classify(m, "/v2/transactions/simulate/limits", ""); c.Capabilities || c.Simulate {
			t.Fatalf("%s old limits path: %+v", m, c)
		}
	}
}

func TestApplySimulate(t *testing.T) {
	// A round of 0 is the latest, as omitting it: any synced node.
	if c := simClass(SimulateBody{Round: 0, TxnGroups: make([]struct{}, 1)}); c.Round != nil || c.SimNeeds == nil || c.SimNeeds.Groups != 1 {
		t.Fatalf("round 0: %+v", c)
	}
	c := simClass(SimulateBody{Round: 7})
	if c.Round == nil || *c.Round != 7 || c.RoundWindow != WindowState || !c.HistoryAnyRound || c.RoundHigh != 0 {
		t.Fatalf("round 7: %+v", c)
	}
	// rounds: the span of the non-zero entries; 0 is the node's latest.
	c = simClass(SimulateBody{Rounds: []uint64{40, 0, 12, 40, 30}})
	if c.Round == nil || *c.Round != 12 || c.RoundHigh != 40 || !c.SimNeeds.RoundsSpan || c.SimNeeds.Rounds != 5 || !c.HistoryAnyRound {
		t.Fatalf("rounds: %+v %+v", c, c.SimNeeds)
	}
	if c = simClass(SimulateBody{Rounds: []uint64{0, 0}}); c.Round != nil || c.SimNeeds.RoundsSpan || c.SimNeeds.Rounds != 2 {
		t.Fatalf("rounds of latest only: %+v %+v", c, c.SimNeeds)
	}
	for _, b := range []SimulateBody{{ParallelGroups: true}, {SkipValidityWindow: true}, {RewriteValidityWindow: true}, {Protocol: "v43"}} {
		if c := simClass(b); !c.SimNeeds.Fork || c.SimNeeds.DeveloperAPI {
			t.Errorf("%+v: needs %+v", b, c.SimNeeds)
		}
	}
	var trace SimulateBody
	trace.ExecTraceConfig.Enable = true
	for _, b := range []SimulateBody{trace, {Protocol: "future"}} {
		if c := simClass(b); !c.SimNeeds.DeveloperAPI {
			t.Errorf("%+v: needs %+v", b, c.SimNeeds)
		}
	}
	if c := simClass(SimulateBody{ExtraOpcodeBudget: -5}); c.SimNeeds.ExtraOpcodeBudget != 0 {
		t.Fatalf("negative budget: %+v", c.SimNeeds)
	}
	c = simClass(SimulateBody{Rounds: []uint64{3, 9}, TxnGroups: make([]struct{}, 3), ExtraOpcodeBudget: 500_000, ParallelGroups: true})
	for _, want := range []string{"round=3", "round_high=9", "sim_groups=3", "sim_rounds=2", "sim_budget=500000", "sim_fork"} {
		if !strings.Contains(c.String(), want) {
			t.Errorf("class %q lacks %q", c.String(), want)
		}
	}
	if c.StatKey() != "simulate" {
		t.Fatalf("stat key %q", c.StatKey())
	}
}

func TestSimulateNeedsFits(t *testing.T) {
	stock := Capabilities{}
	dt := Capabilities{Simulate: &dtLimits}
	dtDev := Capabilities{Simulate: &dtLimits, DeveloperAPI: true}
	cases := []struct {
		name  string
		n     SimulateNeeds
		caps  Capabilities
		wants bool
	}{
		{"one group on stock", SimulateNeeds{Groups: 1, ExtraOpcodeBudget: 320000}, stock, true},
		{"two groups on stock", SimulateNeeds{Groups: 2}, stock, false},
		{"two groups on dtalgorand", SimulateNeeds{Groups: 2}, dt, true},
		{"too many groups", SimulateNeeds{Groups: 17}, dt, false},
		{"rounds on stock", SimulateNeeds{Rounds: 1}, stock, false},
		{"rounds within limit", SimulateNeeds{Rounds: 8}, dt, true},
		{"too many rounds", SimulateNeeds{Rounds: 9}, dt, false},
		{"rounds disabled", SimulateNeeds{Rounds: 1}, Capabilities{Simulate: &SimulateLimits{MaxTxnGroups: 16}}, false},
		{"budget above stock", SimulateNeeds{ExtraOpcodeBudget: 320001}, stock, false},
		{"budget within dtalgorand", SimulateNeeds{ExtraOpcodeBudget: 1_000_000}, dt, true},
		{"budget above dtalgorand", SimulateNeeds{ExtraOpcodeBudget: 1_000_001}, dt, false},
		{"fork option on stock", SimulateNeeds{Fork: true}, stock, false},
		{"fork option on dtalgorand", SimulateNeeds{Fork: true}, dt, true},
		{"trace without dev", SimulateNeeds{DeveloperAPI: true}, dt, false},
		{"trace with dev", SimulateNeeds{DeveloperAPI: true}, dtDev, true},
		// A hand-written override of zeros allows what stock algod does.
		{"zero override", SimulateNeeds{Groups: 1, ExtraOpcodeBudget: 320000}, Capabilities{Simulate: &SimulateLimits{}}, true},
	}
	for _, c := range cases {
		if got := c.n.Fits(c.caps); got != c.wants {
			t.Errorf("%s: fits %v, want %v", c.name, got, c.wants)
		}
	}
}

func TestEligibleSimulateRounds(t *testing.T) {
	plain := Upstream{Health: HealthSynced, LastRound: 100, Caps: Capabilities{MaxAcctLookback: 4, Simulate: &dtLimits}}
	tt := plain
	tt.Caps.TimeTravel = true
	behind := tt
	behind.LastRound = 39
	c := simClass(SimulateBody{Rounds: []uint64{12, 40}})
	if Eligible(plain, c, 100, 2) {
		t.Fatal("a plain node outside its state window took the rounds")
	}
	if !Eligible(tt, c, 100, 2) {
		t.Fatal("time-travel node refused rounds it holds")
	}
	if Eligible(behind, c, 100, 2) {
		t.Fatal("a node short of the latest listed round took the rounds")
	}
	if !Eligible(plain, simClass(SimulateBody{Rounds: []uint64{97, 100}}), 100, 2) {
		t.Fatal("plain node refused rounds within its state window")
	}
}

func TestRelaxed(t *testing.T) {
	if r := Classify("GET", "/v2/status", "").Relaxed(); r != nil {
		t.Fatalf("plain request relaxed: %+v", r)
	}
	r := simClass(SimulateBody{Round: 7, TxnGroups: make([]struct{}, 2)}).Relaxed()
	if len(r) != 1 || r[0].SimNeeds != nil || r[0].Round == nil || *r[0].Round != 7 {
		t.Fatalf("single round keeps its round: %+v", r)
	}
	r = simClass(SimulateBody{Rounds: []uint64{12, 40}, TxnGroups: make([]struct{}, 2)}).Relaxed()
	if len(r) != 2 {
		t.Fatalf("rounds relaxations: %+v", r)
	}
	if r[0].Round != nil || r[0].RoundHigh != 0 || r[0].HistoryAnyRound || r[0].SimNeeds == nil || r[0].SimNeeds.Groups != 2 {
		t.Fatalf("first relaxation drops the rounds only: %+v %+v", r[0], r[0].SimNeeds)
	}
	if r[1].Round != nil || r[1].SimNeeds != nil {
		t.Fatalf("second relaxation drops every need: %+v", r[1])
	}
}

func TestFleetCapabilities(t *testing.T) {
	small := SimulateLimits{MaxTxnGroups: 4, MaxRounds: 16, MaxExtraOpcodeBudget: 320000, ResourceLimitMultiplier: 8}
	us := []Upstream{
		{ID: "stock", Health: HealthSynced, LastRound: 100},
		{ID: "dt", Health: HealthSynced, LastRound: 100, Caps: Capabilities{Simulate: &dtLimits, TimeTravel: true}},
		{ID: "small", Health: HealthSynced, LastRound: 100, Caps: Capabilities{Simulate: &small}},
		{ID: "down", Health: HealthOffline, LastRound: 100, Caps: Capabilities{Simulate: &SimulateLimits{MaxTxnGroups: 256}, DeveloperAPI: true}},
	}
	// dt allows more groups, small more rounds: the answer is dt's alone,
	// not small's rounds with dt's groups, which no node runs together.
	// The multiplier is the smallest, the stock node's. The developer API is
	// the down node's: it counts for the fleet's features, with what it
	// last had, though not for the limits.
	got, ok := FleetCapabilities(us, 100, 2, 0)
	want := FleetCapabilitiesReply{TimeTravel: true, DeveloperAPI: true,
		Simulate: FleetSimulateLimits{MaxTxnGroups: 16, MaxRounds: 8, MaxExtraOpcodeBudget: 1_000_000, ResourceLimitMultiplier: 1}}
	if !ok || got != want {
		t.Fatalf("fleet limits %+v %v, want %+v", got, ok, want)
	}
	// A node at least as large everywhere wins whatever the order.
	big := SimulateLimits{MaxTxnGroups: 16, MaxRounds: 16, MaxExtraOpcodeBudget: 1_000_000, ResourceLimitMultiplier: 8}
	more := append([]Upstream{{ID: "big", Health: HealthSynced, LastRound: 100, Caps: Capabilities{Simulate: &big, TimeTravel: true, DeveloperAPI: true}}}, us[1:]...)
	got, _ = FleetCapabilities(more, 100, 2, 0)
	want = FleetCapabilitiesReply{TimeTravel: true, DeveloperAPI: true,
		Simulate: FleetSimulateLimits{MaxTxnGroups: 16, MaxRounds: 16, MaxExtraOpcodeBudget: 1_000_000, ResourceLimitMultiplier: 4}}
	if got != want {
		t.Fatalf("dominant node: %+v, want %+v", got, want)
	}
	// Time travel and the developer API are the fleet's, not only the
	// simulate node's: the larger node lacks both, a smaller one has them.
	mixed := []Upstream{
		{ID: "large", Health: HealthSynced, LastRound: 100, Caps: Capabilities{Simulate: &big}},
		{ID: "history", Health: HealthSynced, LastRound: 100, Caps: Capabilities{Simulate: &small, TimeTravel: true}},
		{ID: "dev", Health: HealthSynced, LastRound: 100, Caps: Capabilities{DeveloperAPI: true}},
	}
	got, _ = FleetCapabilities(mixed, 100, 2, 0)
	if !got.TimeTravel || !got.DeveloperAPI || got.Simulate.MaxTxnGroups != big.MaxTxnGroups {
		t.Fatalf("mixed fleet: %+v", got)
	}
	// The time limit is the smallest set one; 0 is no limit.
	limited := func(id string, secs uint64) Upstream {
		l := small
		l.TimeLimitSeconds = secs
		return Upstream{ID: id, Health: HealthSynced, LastRound: 100, Caps: Capabilities{Simulate: &l}}
	}
	if got, _ = FleetCapabilities([]Upstream{limited("a", 0), limited("b", 30), limited("c", 10)}, 100, 2, 0); got.TimeLimitSeconds != 10 {
		t.Fatalf("time limit %d, want 10", got.TimeLimitSeconds)
	}
	if got, _ = FleetCapabilities([]Upstream{limited("a", 0)}, 100, 2, 0); got.TimeLimitSeconds != 0 {
		t.Fatalf("no limit: %d", got.TimeLimitSeconds)
	}
	// The router gives up on a node at its upstream timeout, so the time
	// limit is at most that, in whole seconds and at least 1.
	for _, tc := range []struct {
		secs    uint64
		timeout time.Duration
		want    uint64
	}{{0, time.Minute, 60}, {30, 10 * time.Second, 10}, {30, time.Minute, 30}, {0, 1500 * time.Millisecond, 1}, {30, 500 * time.Millisecond, 1}} {
		if got, _ = FleetCapabilities([]Upstream{limited("a", tc.secs)}, 100, 2, tc.timeout); got.TimeLimitSeconds != tc.want {
			t.Fatalf("time limit %d with upstream timeout %s: %d, want %d", tc.secs, tc.timeout, got.TimeLimitSeconds, tc.want)
		}
	}
	if _, ok := FleetCapabilities(us[:1], 100, 2, 0); ok {
		t.Fatal("a fleet of stock nodes reported limits")
	}
	if _, ok := FleetCapabilities(us[3:], 100, 2, 0); ok {
		t.Fatal("an unreachable node's limits counted")
	}
}

// A time-travel node only out for now still makes the fleet's time-travel
// true: clients keep the answer, and reads at a round still route to it once
// it is back. One whose capabilities are unknown, or that is excluded, does
// not count.
func TestFleetCapabilitiesFeaturesOfNodesOut(t *testing.T) {
	small := SimulateLimits{MaxTxnGroups: 4, MaxRounds: 16, MaxExtraOpcodeBudget: 320000, ResourceLimitMultiplier: 8}
	tt := Capabilities{Simulate: &small, TimeTravel: true, DeveloperAPI: true}
	for _, tc := range []struct {
		name string
		out  func(*Upstream)
		want bool
	}{
		{"lagging", func(u *Upstream) { u.LastRound = 50 }, true},
		{"busy", func(u *Upstream) { u.Stats.BusySlots = Slots(0).With(SlotHeavy) }, true},
		{"throttled", func(u *Upstream) { u.Stats.Throttled = true }, true},
		{"breaker open", func(u *Upstream) { u.Stats.BreakerOpen = true }, true},
		{"draining", func(u *Upstream) { u.Draining = true }, true},
		{"down", func(u *Upstream) { u.Health, u.Caps, u.LastCaps = HealthOffline, Capabilities{}, &tt }, true},
		{"unknown", func(u *Upstream) { u.Health, u.Caps, u.CapsUnknown = HealthOffline, Capabilities{}, true }, false},
		{"excluded", func(u *Upstream) { u.Excluded = true }, false},
	} {
		us := []Upstream{
			{ID: "dt", Health: HealthSynced, LastRound: 100, Caps: Capabilities{Simulate: &dtLimits}},
			{ID: "history", Health: HealthSynced, LastRound: 100, Caps: tt},
		}
		tc.out(&us[1])
		got, ok := FleetCapabilities(us, 100, 2, 0)
		if !ok || got.TimeTravel != tc.want || got.DeveloperAPI != tc.want || got.Simulate.MaxTxnGroups != dtLimits.MaxTxnGroups {
			t.Fatalf("%s: %+v %v, want time-travel and developer-api %v", tc.name, got, ok, tc.want)
		}
	}
}

func TestSimulateLimitsClamped(t *testing.T) {
	huge := Capabilities{Simulate: &SimulateLimits{MaxTxnGroups: 1000, MaxRounds: 1000, MaxExtraOpcodeBudget: 1 << 40, ResourceLimitMultiplier: 1 << 20}}
	want := SimulateLimits{MaxTxnGroups: 256, MaxRounds: 256, MaxExtraOpcodeBudget: 320_000_000, ResourceLimitMultiplier: 1024}
	if got := huge.SimulateLimits(); got != want {
		t.Fatalf("clamped %+v, want %+v", got, want)
	}
	if (SimulateNeeds{Groups: 257}).Fits(huge) {
		t.Fatal("an override promised more groups than the fork runs")
	}
}

// A rounds list with 0 asks for the node's own latest round, so a lagging
// node is out even though it holds the past rounds.
func TestEligibleSimulateRoundsWithLatest(t *testing.T) {
	lagging := Upstream{Health: HealthLagging, LastRound: 90, Caps: Capabilities{TimeTravel: true, Simulate: &dtLimits}}
	if !Eligible(lagging, simClass(SimulateBody{Rounds: []uint64{12, 40}}), 100, 2) {
		t.Fatal("lagging node refused past rounds it holds")
	}
	if Eligible(lagging, simClass(SimulateBody{Rounds: []uint64{12, 0}}), 100, 2) {
		t.Fatal("lagging node took a rounds list with the latest round")
	}
}

func TestCouldServe(t *testing.T) {
	down := Upstream{Health: HealthOffline, Draining: true, LastRound: 100, Stats: Stats{BreakerOpen: true}, Caps: Capabilities{Simulate: &dtLimits}}
	stock := Upstream{Health: HealthSynced, LastRound: 100}
	two := simClass(SimulateBody{TxnGroups: make([]struct{}, 2)})
	if Eligible(down, two, 100, 2) || !CouldServe([]Upstream{stock, down}, two) {
		t.Fatal("a fitting node that is out must count as able to serve")
	}
	if CouldServe([]Upstream{stock}, two) {
		t.Fatal("a stock node cannot serve two groups")
	}
	if CouldServe([]Upstream{down}, simClass(SimulateBody{Rounds: []uint64{5000}})) {
		t.Fatal("a node whose last round is short of the rounds cannot serve them")
	}
}
