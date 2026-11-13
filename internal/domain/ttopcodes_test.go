package domain

import (
	"crypto/ed25519"
	crand "crypto/rand"
	"encoding/json"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestTTOpcodesNames(t *testing.T) {
	all := ttOpcodes
	s := ParseTTOpcodes(append([]string{"tt_future_op"}, all...))
	if !reflect.DeepEqual(s.Names(), all) || s.Len() != len(all) {
		t.Fatalf("round trip %v (%d)", s.Names(), s.Len())
	}
	if s := ParseTTOpcodes([]string{"tt_box_change", "tt_seek"}); !reflect.DeepEqual(s.Names(), []string{"tt_seek", "tt_box_change"}) {
		t.Fatalf("order: %v", s.Names())
	}
	var o struct {
		Ops TTOpcodes `yaml:"ops"`
	}
	if err := yaml.Unmarshal([]byte("ops: [tt_seek, tt_app_change]"), &o); err != nil || !reflect.DeepEqual(o.Ops.Names(), []string{"tt_seek", "tt_app_change"}) {
		t.Fatalf("yaml: %v %v", o.Ops.Names(), err)
	}
	if err := yaml.Unmarshal([]byte("ops: [tt_seek, tt_sek]"), &o); err == nil || !strings.Contains(err.Error(), "tt_sek") {
		t.Fatalf("yaml with a typo: %v", err)
	}
	out, _ := yaml.Marshal(o)
	if !strings.Contains(string(out), "- tt_seek") {
		t.Fatalf("yaml out: %s", out)
	}
}

func TestAcceptsEventStream(t *testing.T) {
	for _, tc := range []struct {
		accept []string
		want   bool
	}{
		{nil, false},
		{[]string{"application/json"}, false},
		{[]string{"text/event-stream"}, true},
		{[]string{"application/json, text/event-stream;q=0.5"}, true},
		{[]string{"application/json", "Text/Event-Stream"}, true},
		{[]string{"text/event-stream;q=0"}, false},
		{[]string{"text/event-stream;q=x"}, false},
		{[]string{"*/*"}, false},
	} {
		if got := AcceptsEventStream(tc.accept); got != tc.want {
			t.Errorf("%q: %v, want %v", tc.accept, got, tc.want)
		}
	}
	if !IsEventStream("text/event-stream; charset=utf-8") || IsEventStream("application/json") || IsEventStream("") {
		t.Fatal("IsEventStream")
	}
}

// oneCall is the calls of a query with one call.
func oneCall() []struct{} { return make([]struct{}, 1) }

// ttQueryCaps is a dtalgorand with queries and the given time-travel
// opcodes, and history when it has more than one.
func ttQueryCaps(ops ...string) Capabilities {
	l := dtLimits
	l.Query = dtQuery
	l.Query.Opcodes, l.Query.MaxSeekRounds = ParseTTOpcodes(ops), 64
	return Capabilities{Simulate: &l, TimeTravel: len(ops) > 1}
}

// A query that accepts a stream prefers a node that streams, which does not
// make another node ineligible; a program's opcodes do not matter.
func TestSelectQueryPreferences(t *testing.T) {
	plain := Upstream{ID: "a-plain", Health: HealthSynced, LastRound: 100, Caps: ttQueryCaps("tt_seek")}
	plain.Stats.LatencyMS = map[string]float64{"query": 1}
	rich := Upstream{ID: "b-rich", Health: HealthSynced, LastRound: 100, Caps: ttQueryCaps("tt_seek", "tt_acct_change")}
	rich.Caps.Simulate.Query.Stream = true
	rich.Stats.LatencyMS = map[string]float64{"query": 500}
	both := []Upstream{plain, rich}
	pick := func(c RequestClass, us []Upstream, mode Mode) string {
		t.Helper()
		sel, ok := Select(mode, us, c, 100, 0, Weights{}, rand.New(rand.NewPCG(1, 2)))
		if !ok {
			t.Fatalf("%s: nothing selected", c)
		}
		return sel.Chosen.ID
	}
	plainQuery := queryClass(QueryBody{Calls: oneCall()})
	if got := pick(plainQuery, both, ModeFallback); got != "a-plain" {
		t.Fatalf("plain query went to %s, want the faster node", got)
	}
	stream := plainQuery
	stream.EventStream = true
	for _, mode := range []Mode{ModeFallback, ModeLoadBalancer} {
		for i := 0; i < 20; i++ {
			if got := pick(stream, both, mode); got != "b-rich" {
				t.Fatalf("%s: stream went to %s", mode, got)
			}
		}
	}
	if got := pick(stream, []Upstream{plain}, ModeFallback); got != "a-plain" {
		t.Fatalf("stream with no streaming node went to %s", got)
	}
}

// The fleet's query object carries the opcodes and their limits of the node
// it is taken from, and stream when any node streams.
func TestFleetQueryOpcodesAndStream(t *testing.T) {
	big := ttQueryCaps("tt_seek", "tt_acct_change", "tt_global_key_change") // the most calls
	big.Simulate.Query.MaxCalls, big.Simulate.Query.MaxTotalCalls = 2048, 2048
	big.Simulate.Query.MaxScanStepBytes, big.Simulate.Query.MaxCallScanBytes = 1<<22, 1<<26
	seek := ttQueryCaps("tt_seek")
	seek.Simulate.Query.MaxSeekRounds = 128
	seek.Simulate.Query.Stream = true
	us := []Upstream{
		{ID: "big", Health: HealthSynced, LastRound: 100, Caps: big},
		{ID: "seek", Health: HealthSynced, LastRound: 100, Caps: seek},
		{ID: "plain", Health: HealthSynced, LastRound: 100, Caps: queryCaps()},
	}
	got, _ := FleetCapabilities(us, 100, 2, 0)
	q := got.Query
	if q == nil || q.MaxCalls != 2048 || q.MaxSeekRounds != 64 || q.MaxScanStepBytes != 1<<22 || q.MaxCallScanBytes != 1<<26 ||
		!reflect.DeepEqual(q.Opcodes, []string{"tt_seek", "tt_acct_change", "tt_global_key_change"}) || !reflect.DeepEqual(q.Stream, []string{"sse"}) {
		t.Fatalf("query %+v", q)
	}
	us[0].Health, us[1].Health = HealthOffline, HealthOffline
	got, _ = FleetCapabilities(us, 100, 2, 0)
	b, _ := json.Marshal(got.Query)
	if strings.Contains(string(b), "opcodes") || strings.Contains(string(b), "stream") || !strings.Contains(string(b), `"max-seek-rounds":0`) {
		t.Fatalf("no opcodes or streams: %s", b)
	}
}

// A heartbeat with every field at its largest still fits a datagram
// (gossipudp.MaxDatagram, 1400 bytes, less the 96-byte signature header).
func TestHeartbeatFitsDatagram(t *testing.T) {
	l := SimulateLimits{MaxTxnGroups: 256, MaxRounds: 256, MaxExtraOpcodeBudget: maxExtraOpcodeBudget, ResourceLimitMultiplier: 1024, ReadsRemoved: true,
		TimeLimitSeconds: 1 << 20, Query: QueryLimits{MaxCalls: 1 << 20, MaxCallBudget: maxExtraOpcodeBudget, MaxCallLogBytes: 1 << 30,
			MaxCallStateBytes: 1 << 40, MaxResponseBytes: 1 << 40, MaxRounds: 256, MaxTotalCalls: 1 << 28, MaxSeekRounds: 1 << 20,
			MaxScanStepBytes: 1 << 40, MaxCallScanBytes: 1 << 40, Opcodes: ParseTTOpcodes(TTOpcodes(1<<len(ttOpcodes) - 1).Names()), Stream: true}}
	hb := Heartbeat{NodeID: strings.Repeat("n", 32), Seq: 1 << 62, LastRound: 1 << 40, Online: true, Draining: true, Caps: Capabilities{OldestRound: 1 << 40,
		Archival: Archival{Kind: ArchivalTrailing, N: 1 << 40}, DeveloperAPI: true, FollowMode: true, ExperimentalAPI: true, TimeTravel: true,
		HoldersIndex: true, HistoryUntrusted: true, TTRoundHeader: true, Simulate: &l, AlgodVersion: "5.10.10.stable",
		AlgodCommit: strings.Repeat("e", 40), Dtalgorand: true, GenesisID: "mainnet-v1.0", StorageEngine: "pebbledb", MaxAcctLookback: 1 << 20}}
	_, priv, _ := ed25519.GenerateKey(crand.Reader)
	wire, err := EncodeHeartbeat(priv, hb)
	if err != nil || len(wire) > 1400 {
		t.Fatalf("heartbeat of %d bytes: %v", len(wire), err)
	}
	got, _, err := DecodeHeartbeat(wire)
	if err != nil || *got.Caps.Simulate != l {
		t.Fatalf("decoded %+v: %v", got.Caps.Simulate, err)
	}
}
