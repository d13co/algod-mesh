package algodhttp

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/d13co/algod-mesh/internal/domain"
	"github.com/d13co/algod-mesh/internal/fakealgod"
	"github.com/d13co/algod-mesh/internal/ports"
)

func TestClientAgainstFakeAlgod(t *testing.T) {
	n := fakealgod.New(fakealgod.Options{ID: "a", StartRound: 500, OldestRound: 100})
	defer n.Close()
	c := New(n.URL(), n.Token(), nil)
	ctx := context.Background()

	st, err := c.Status(ctx)
	if err != nil || st.LastRound != 500 || len(st.Raw) == 0 {
		t.Fatalf("status: %v %+v", err, st)
	}
	for r, want := range map[uint64]bool{99: false, 100: true, 500: true, 501: false} {
		got, err := c.HasBlock(ctx, r)
		if err != nil || got != want {
			t.Fatalf("HasBlock(%d)=%v,%v want %v", r, got, err, want)
		}
	}
	v, err := c.Versions(ctx)
	if err != nil || v.GenesisID != "fakenet-v1" {
		t.Fatalf("versions: %v %+v", err, v)
	}
	go func() { time.Sleep(50 * time.Millisecond); n.Advance(1) }()
	st, err = c.WaitForBlockAfter(ctx, 500)
	if err != nil || st.LastRound != 501 {
		t.Fatalf("wait: %v %+v", err, st)
	}
	n.PutBox(7, []byte("nx"), []byte("val"))
	names, err := c.BoxNames(ctx, 7)
	if err != nil || len(names) != 1 || string(names[0]) != "nx" {
		t.Fatalf("boxes: %v %q", err, names)
	}
	val, err := c.Box(ctx, 7, []byte("nx"))
	if err != nil || string(val) != "val" {
		t.Fatalf("box: %v %q", err, val)
	}
	if _, err := c.Box(ctx, 7, []byte("nope")); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("missing box: %v", err)
	}
	if lim, err := c.Capabilities(ctx); err != nil || lim != nil {
		t.Fatalf("stock simulate limits: %v %+v", err, lim)
	}
	want := domain.SimulateLimits{MaxTxnGroups: 16, MaxRounds: 4, MaxExtraOpcodeBudget: 640000, ResourceLimitMultiplier: 2, TimeLimitSeconds: 64}
	dt := fakealgod.New(fakealgod.Options{ID: "dt", StartRound: 5, Simulate: &want, TimeTravel: true})
	defer dt.Close()
	if lim, err := New(dt.URL(), dt.Token(), nil).Capabilities(ctx); err != nil || lim == nil || lim.Limits != want ||
		!lim.TimeTravel || lim.DeveloperAPI {
		t.Fatalf("dtalgorand capabilities: %v %+v", err, lim)
	}
	withQuery := want
	withQuery.Query = domain.QueryLimits{MaxCalls: 1024, MaxCallBudget: 320000, MaxCallLogBytes: 65536, MaxCallStateBytes: 64 << 20, MaxResponseBytes: 64 << 20, MaxRounds: 16,
		MaxTotalCalls: 1024}
	dq := fakealgod.New(fakealgod.Options{ID: "dq", StartRound: 5, Simulate: &withQuery})
	defer dq.Close()
	if lim, err := New(dq.URL(), dq.Token(), nil).Capabilities(ctx); err != nil || lim == nil || lim.Limits != withQuery {
		t.Fatalf("dtalgorand query limits: %v %+v", err, lim)
	}
	// An older node with rounds disabled sends max-rounds 0, and still takes
	// the read extensions; a newer one leaves out max-rounds and the
	// multiplier.
	noRounds := domain.SimulateLimits{MaxTxnGroups: 16, MaxExtraOpcodeBudget: 640000, ResourceLimitMultiplier: 1}
	d0 := fakealgod.New(fakealgod.Options{ID: "d0", StartRound: 5, Simulate: &noRounds})
	defer d0.Close()
	if lim, err := New(d0.URL(), d0.Token(), nil).Capabilities(ctx); err != nil || lim == nil || lim.Limits != noRounds {
		t.Fatalf("dtalgorand with rounds disabled: %v %+v", err, lim)
	}
	newer := domain.SimulateLimits{MaxTxnGroups: 16, MaxRounds: 4, MaxExtraOpcodeBudget: 640000, ResourceLimitMultiplier: 2, ReadsRemoved: true}
	dn := fakealgod.New(fakealgod.Options{ID: "dn", StartRound: 5, Simulate: &newer})
	defer dn.Close()
	wantNewer := domain.SimulateLimits{MaxTxnGroups: 16, MaxExtraOpcodeBudget: 640000, ReadsRemoved: true}
	if lim, err := New(dn.URL(), dn.Token(), nil).Capabilities(ctx); err != nil || lim == nil || lim.Limits != wantNewer {
		t.Fatalf("dtalgorand without the read extensions: %v %+v", err, lim)
	}
	older := withQuery // a node from before max-total-calls leaves it out
	older.Query.MaxTotalCalls = 0
	do := fakealgod.New(fakealgod.Options{ID: "do", StartRound: 5, Simulate: &older})
	defer do.Close()
	if lim, err := New(do.URL(), do.Token(), nil).Capabilities(ctx); err != nil || lim == nil || lim.Limits != older {
		t.Fatalf("query limits without max-total-calls: %v %+v", err, lim)
	}
	timeTravel := withQuery
	timeTravel.Query.MaxSeekRounds, timeTravel.Query.MaxScanStepBytes, timeTravel.Query.MaxCallScanBytes = 64, 4<<20, 64<<20
	timeTravel.Query.Opcodes, timeTravel.Query.Stream = domain.ParseTTOpcodes([]string{"tt_seek", "tt_box_change", "tt_box_prefix_change"}), true
	dtt := fakealgod.New(fakealgod.Options{ID: "dtt", StartRound: 5, Simulate: &timeTravel, TimeTravel: true})
	defer dtt.Close()
	if lim, err := New(dtt.URL(), dtt.Token(), nil).Capabilities(ctx); err != nil || lim == nil || lim.Limits != timeTravel {
		t.Fatalf("query limits with time-travel opcodes and streams: %v %+v", err, lim)
	}
	// dtalgorand's own answer; an opcode this mesh does not know is left out.
	raw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"time-travel":true,"developer-api":false,"time-limit-seconds":59,"simulate":{"max-txn-groups":16,"max-extra-opcode-budget":320000},` +
			`"query":{"max-calls":1024,"max-call-budget":320000,"max-call-log-bytes":65536,"max-call-scan-bytes":67108864,"max-call-state-bytes":67108864,` +
			`"max-response-bytes":67108864,"max-rounds":16,"max-scan-step-bytes":4194304,"max-seek-rounds":64,"max-total-calls":1024,` +
			`"opcodes":["tt_seek","tt_acct_change","tt_someday"],"stream":["sse","ws"]}}`))
	}))
	defer raw.Close()
	lim, err := New(raw.URL, "t", nil).Capabilities(ctx)
	if err != nil || lim == nil {
		t.Fatalf("raw capabilities: %v", err)
	}
	if q := lim.Limits.Query; q.MaxSeekRounds != 64 || q.MaxScanStepBytes != 4<<20 || q.MaxCallScanBytes != 64<<20 || !q.Stream ||
		strings.Join(q.Opcodes.Names(), ",") != "tt_seek,tt_acct_change" {
		t.Fatalf("raw query limits %+v", q)
	}
	bad := New(n.URL(), "wrong", nil)
	if _, err := bad.Status(ctx); err == nil {
		t.Fatal("wrong token must fail")
	}
}
