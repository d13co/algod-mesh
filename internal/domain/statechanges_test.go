package domain

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestStateChangesIncludeNames(t *testing.T) {
	s := ParseStateChangesInclude([]string{"trace", "accounts", "nope", "reads"})
	if s != IncludeAccounts|IncludeReads|IncludeTrace || s.Len() != 3 {
		t.Fatalf("parsed %b", s)
	}
	if got := s.Names(); !reflect.DeepEqual(got, []string{"accounts", "reads", "trace"}) {
		t.Fatalf("names %v", got)
	}
	if !s.Has(IncludeReads|IncludeTrace) || s.Has(IncludeBudget) || !s.Has(0) {
		t.Fatal("Has")
	}
	var o struct {
		Include StateChangesInclude `yaml:"include"`
	}
	if err := yaml.Unmarshal([]byte("include: [apps, budget]"), &o); err != nil || o.Include != IncludeApps|IncludeBudget {
		t.Fatalf("yaml %b: %v", o.Include, err)
	}
	if err := yaml.Unmarshal([]byte("include: [apps, traces]"), &o); err == nil || !strings.Contains(err.Error(), `"traces"`) {
		t.Fatalf("unknown name: %v", err)
	}
}

// A node's refusal is matched to the request's round alone.
func TestStateChangesNotInHistory(t *testing.T) {
	msg := func(r uint64) string { return stateChangesNotInHistory(r) }
	switch {
	case !StateChangesNotInHistory(msg(15), 15):
		t.Fatal("same round")
	case StateChangesNotInHistory(msg(150), 15), StateChangesNotInHistory(msg(115), 15), StateChangesNotInHistory(msg(5), 15):
		t.Fatal("another round")
	case StateChangesNotInHistory("failed retrieving State Delta: transaction X is not in round 15", 15):
		t.Fatal("another refusal")
	}
}

// scCaps is a time-travel dtalgorand that serves state changes with include.
func scCaps(include StateChangesInclude, maxBytes uint64) Capabilities {
	return Capabilities{TimeTravel: true, Simulate: &SimulateLimits{MaxTxnGroups: 1,
		StateChanges: StateChangesLimits{Served: true, MaxResponseBytes: maxBytes, Include: include}}}
}

const groupIncludes = IncludeAccounts | IncludeAssets | IncludeApps

// State changes go only to a time-travel node that serves them, takes every
// include value asked for, and has the round's block stored.
func TestEligibleStateChanges(t *testing.T) {
	up := func(c Capabilities) Upstream {
		c.OldestRound = 20
		return Upstream{ID: "u", Health: HealthSynced, LastRound: 100, Caps: c}
	}
	c := Classify("GET", "/v2/deltas/50/txn/state-changes", "include=reads")
	cases := []struct {
		name string
		u    Upstream
		want bool
	}{
		{"serves with reads", up(scCaps(groupIncludes|IncludeReads, 0)), true},
		{"without reads", up(scCaps(groupIncludes, 0)), false},
		{"time travel without the route", up(Capabilities{TimeTravel: true, Simulate: &SimulateLimits{}}), false},
		{"route declared without time travel", func() Upstream { u := up(scCaps(IncludeReads, 0)); u.Caps.TimeTravel = false; return u }(), false},
		{"untrusted history", func() Upstream { u := up(scCaps(IncludeReads, 0)); u.Caps.HistoryUntrusted = true; return u }(), false},
		{"block not stored", func() Upstream { u := up(scCaps(IncludeReads, 0)); u.Caps.OldestRound = 60; return u }(), false},
		{"round not reached", func() Upstream { u := up(scCaps(IncludeReads, 0)); u.LastRound = 40; return u }(), false},
	}
	for _, tc := range cases {
		if got := Eligible(tc.u, c, 100, 2); got != tc.want {
			t.Errorf("%s: eligible %v, want %v", tc.name, got, tc.want)
		}
	}
	if c.Slot() != SlotHeavy || c.StatKey() != CostStateChanges || CostKey(c, 100) != CostStateChanges {
		t.Fatalf("slot %v, stat key %q", c.Slot(), c.StatKey())
	}
	relaxed := c.Relaxed()
	if len(relaxed) != 1 || relaxed[0].StateChangesInclude != 0 || !relaxed[0].StateChanges || len(relaxed[0].Relaxed()) != 0 {
		t.Fatalf("relaxed %+v", relaxed)
	}
}

// The fleet's capabilities report the state-changes object of the node that
// takes the most include values, and simulate's exec-trace-config.
func TestFleetCapabilitiesStateChanges(t *testing.T) {
	up := func(id string, c Capabilities) Upstream {
		return Upstream{ID: id, Health: HealthSynced, LastRound: 100, Caps: c}
	}
	a := scCaps(groupIncludes|IncludeBudget|IncludeReads, 1<<24)
	b := scCaps(groupIncludes, 0)
	b.Simulate.StateChangeAppID = true
	got, ok := FleetCapabilities([]Upstream{up("a", a), up("b", b)}, 100, 2, 0)
	if !ok || got.StateChanges == nil || got.StateChanges.MaxResponseBytes != 1<<24 ||
		!reflect.DeepEqual(got.StateChanges.Include, []string{"accounts", "assets", "apps", "budget", "reads"}) {
		t.Fatalf("state-changes %+v", got.StateChanges)
	}
	if !reflect.DeepEqual(got.Simulate.ExecTraceConfig, []string{"state-change-app-id"}) {
		t.Fatalf("exec-trace-config %v", got.Simulate.ExecTraceConfig)
	}
	// Equal includes: no limit beats a limit.
	b.Simulate.StateChanges.Include = a.Simulate.StateChanges.Include
	got, _ = FleetCapabilities([]Upstream{up("a", a), up("b", b)}, 100, 2, 0)
	if got.StateChanges.MaxResponseBytes != 0 {
		t.Fatalf("max-response-bytes %d, want 0 (no limit)", got.StateChanges.MaxResponseBytes)
	}
	// Without a node that serves it, and without the option, both are left out.
	got, _ = FleetCapabilities([]Upstream{up("c", Capabilities{TimeTravel: true, Simulate: &SimulateLimits{MaxTxnGroups: 1}})}, 100, 2, 0)
	j, _ := json.Marshal(got)
	if strings.Contains(string(j), "state-changes") || strings.Contains(string(j), "exec-trace-config") {
		t.Fatalf("reply %s", j)
	}
}

// exec-trace-config's state-change-app-id needs a node that lists it; when
// none does the request is relaxed to any node, which refuses it.
func TestSimulateStateChangeAppID(t *testing.T) {
	var c RequestClass
	var b SimulateBody
	if err := json.Unmarshal([]byte(`{"txn-groups":[{}],"exec-trace-config":{"enable":true,"state-change":true,"state-change-app-id":true}}`), &b); err != nil {
		t.Fatal(err)
	}
	c.Simulate = true
	c.ApplySimulate(b)
	if !c.SimNeeds.StateAppID || !c.SimNeeds.DeveloperAPI {
		t.Fatalf("needs %+v", c.SimNeeds)
	}
	with := Capabilities{DeveloperAPI: true, Simulate: &SimulateLimits{MaxTxnGroups: 1, StateChangeAppID: true}}
	without := Capabilities{DeveloperAPI: true, Simulate: &SimulateLimits{MaxTxnGroups: 1}}
	if !c.SimNeeds.Fits(with) || c.SimNeeds.Fits(without) || c.SimNeeds.Fits(Capabilities{DeveloperAPI: true}) {
		t.Fatal("Fits")
	}
	if r := c.Relaxed(); len(r) != 1 || r[0].SimNeeds != nil {
		t.Fatalf("relaxed %+v", r)
	}
}

// A simulation at one round past the fleet gets dtalgorand's 400, which
// names the latest round.
func TestSimulateRoundAhead(t *testing.T) {
	var c RequestClass
	c.ApplySimulate(SimulateBody{Round: 500})
	if c.Ahead.Status != 400 || c.Ahead.Text(120) != "round 500 is after the latest round 120" {
		t.Fatalf("ahead %+v: %q", c.Ahead, c.Ahead.Text(120))
	}
	c = RequestClass{}
	c.ApplySimulate(SimulateBody{Rounds: []uint64{5, 500}})
	if c.Ahead.Status != 0 {
		t.Fatalf("rounds list ahead %+v", c.Ahead)
	}
}
