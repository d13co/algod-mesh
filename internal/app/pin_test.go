package app

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/d13co/algod-mesh/internal/adapters/logging"
	"github.com/d13co/algod-mesh/internal/domain"
)

// pinnedGet sends a GET with the given token and pinned upstream.
func (h *waitHarness) pinnedGet(path, token, upstream string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if token != "" {
		req.Header.Set("X-Algo-API-Token", token)
	}
	req.Header.Set("X-Algod-Mesh-Upstream", upstream)
	h.r.ServeHTTP(rec, req)
	return rec
}

func withTokens(o *RouterOptions) { o.AdminToken, o.ClientToken = "admin-token", "client-token" }

// An admin's request names the upstream that answers it: a peer the router
// would not pick first, or the external while the mesh is healthy.
func TestPinnedRequestGoesToTheNamedUpstream(t *testing.T) {
	h := newTwoPeerHarnessOpts(t, nil, withTokens)
	rec := h.pinnedGet("/v2/status", "admin-token", "c")
	if rec.Code != 200 || rec.Header().Get("X-Algod-Mesh-Upstream") != "c" {
		t.Fatalf("pinned to c: %d %s %s", rec.Code, rec.Header().Get("X-Algod-Mesh-Upstream"), rec.Body.String())
	}
	if b, c := h.n.Hits("/v2/status"), h.c.Hits("/v2/status"); b != 0 || c != 1 {
		t.Fatalf("hits b=%d c=%d, want c only", b, c)
	}
	rec = h.pinnedGet("/v2/status", "admin-token", "ext")
	if rec.Code != 200 || rec.Header().Get("X-Algod-Mesh-Upstream") != "ext" {
		t.Fatalf("pinned to ext: %d %s %s", rec.Code, rec.Header().Get("X-Algod-Mesh-Upstream"), rec.Body.String())
	}
	if n := h.ext.Hits("/v2/status"); n != 1 {
		t.Fatalf("external hit %d times, want the request only", n)
	}
	if !strings.Contains(h.metricsText(), `mesh_requests_pinned_total{upstream="c"} 1`) {
		t.Fatalf("pinned requests not counted:\n%s", h.metricsText())
	}
}

// A pinned request is tried once: a failing node's answer is the client's,
// and no other node sees the request.
func TestPinnedRequestIsNotRetried(t *testing.T) {
	h := newTwoPeerHarnessOpts(t, nil, withTokens)
	h.c.SetFailing(true)
	rec := h.pinnedGet("/v2/status", "admin-token", "c")
	if rec.Code != 503 || rec.Header().Get("X-Algod-Mesh-Upstream") != "c" || !strings.Contains(rec.Body.String(), "fake node is failing") {
		t.Fatalf("pinned to a failing node: %d %s %s", rec.Code, rec.Header().Get("X-Algod-Mesh-Upstream"), rec.Body.String())
	}
	if b, e := h.n.Hits("/v2/status"), h.ext.Hits("/v2/status"); b != 0 || e != 0 {
		t.Fatalf("retried: b=%d ext=%d", b, e)
	}
}

// A pinned request leaves the router's view of the node alone: failures
// past the breaker's threshold don't take it out of routing.
func TestPinnedFailuresDoNotOpenTheBreaker(t *testing.T) {
	h := newTwoPeerHarnessOpts(t, nil, withTokens)
	h.c.SetFailing(true)
	for i := 0; i < 2*DefaultBreaker.Threshold; i++ {
		if rec := h.pinnedGet("/v2/status", "admin-token", "c"); rec.Code != 503 {
			t.Fatalf("pinned to a failing node: %d %s", rec.Code, rec.Body.String())
		}
	}
	cands, _ := h.d.Snapshot()
	for _, u := range cands {
		if u.ID == "c" && (u.Stats.BreakerOpen || u.Stats.ErrorRate != 0) {
			t.Fatalf("pinned failures counted against the node: %+v", u.Stats)
		}
	}
}

// Only the admin may pin: the client token gets a 403 and a wrong token the
// usual 401, and neither request reaches a node.
func TestPinnedRequestNeedsTheAdminToken(t *testing.T) {
	h := newTwoPeerHarnessOpts(t, nil, withTokens)
	if rec := h.pinnedGet("/v2/status", "client-token", "c"); rec.Code != 403 {
		t.Fatalf("client token: %d %s", rec.Code, rec.Body.String())
	}
	if rec := h.pinnedGet("/v2/status", "wrong", "c"); rec.Code != 401 {
		t.Fatalf("wrong token: %d %s", rec.Code, rec.Body.String())
	}
	if b, c, e := h.n.Hits("/v2/status"), h.c.Hits("/v2/status"), h.ext.Hits("/v2/status"); b+c+e != 0 {
		t.Fatalf("forwarded: b=%d c=%d ext=%d", b, c, e)
	}
	// A client token and no admin token: /mesh is closed, and so is pinning.
	h = newTwoPeerHarnessOpts(t, nil, func(o *RouterOptions) { o.ClientToken = "client-token" })
	if rec := h.pinnedGet("/v2/status", "client-token", "c"); rec.Code != 403 {
		t.Fatalf("client token, no admin token: %d %s", rec.Code, rec.Body.String())
	}
}

// With auth off nobody may pin, though /mesh is open: a pin reaches
// externals and excluded nodes, which an open agent must not hand out.
func TestPinnedRequestWithAuthOff(t *testing.T) {
	h := newTwoPeerHarness(t, false)
	for _, id := range []string{"c", "ext"} {
		if rec := h.pinnedGet("/v2/status", "", id); rec.Code != 403 || !strings.Contains(rec.Body.String(), "none is configured") {
			t.Fatalf("auth off, pinned to %s: %d %s", id, rec.Code, rec.Body.String())
		}
	}
	if c, e := h.c.Hits("/v2/status"), h.ext.Hits("/v2/status"); c+e != 0 {
		t.Fatalf("forwarded: c=%d ext=%d", c, e)
	}
	// Routing itself is open as before.
	rec := httptest.NewRecorder()
	h.r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v2/status", nil))
	if rec.Code != 200 {
		t.Fatalf("routed request with auth off: %d %s", rec.Code, rec.Body.String())
	}
}

func TestPinnedRequestToAnUnknownUpstream(t *testing.T) {
	h := newTwoPeerHarnessOpts(t, nil, withTokens)
	if rec := h.pinnedGet("/v2/status", "admin-token", "nope"); rec.Code != 400 || !strings.Contains(rec.Body.String(), `unknown upstream \"nope\"`) {
		t.Fatalf("unknown upstream: %d %s", rec.Code, rec.Body.String())
	}
}

// Excluding an upstream keeps the router off it, not an admin.
func TestPinnedRequestReachesAnExcludedUpstream(t *testing.T) {
	h := newTwoPeerHarnessOpts(t, func(o *DirectoryOptions) { o.Exclude = []string{"c"} }, withTokens)
	if rec := h.pinnedGet("/v2/status", "admin-token", "c"); rec.Code != 200 || rec.Header().Get("X-Algod-Mesh-Upstream") != "c" {
		t.Fatalf("excluded: %d %s %s", rec.Code, rec.Header().Get("X-Algod-Mesh-Upstream"), rec.Body.String())
	}
}

// pinnedStats is the router's view of upstream id.
func pinnedStats(t *testing.T, h *harness, id string) domain.Stats {
	t.Helper()
	cands, _ := h.d.Snapshot()
	for _, u := range cands {
		if u.ID == id {
			return u.Stats
		}
	}
	t.Fatalf("no upstream %q", id)
	return domain.Stats{}
}

// A node's 429 reaches the admin as it was sent, Retry-After included: not
// the 503 a routed client gets, and without throttling the node.
func TestPinnedThrottleIsPassedThrough(t *testing.T) {
	h := newTwoPeerHarnessOpts(t, nil, withTokens)
	h.c.SetPathStatus("/v2/status", 429)
	h.c.SetPathRetryAfter("/v2/status", 7)
	rec := h.pinnedGet("/v2/status", "admin-token", "c")
	if rec.Code != 429 || rec.Header().Get("Retry-After") != "7" {
		t.Fatalf("pinned 429: %d Retry-After %q %s", rec.Code, rec.Header().Get("Retry-After"), rec.Body.String())
	}
	if pinnedStats(t, h.harness, "c").Throttled {
		t.Fatal("pinned 429 throttled the node")
	}
}

// An answer routing withholds, here one at a round without the node's
// round mark, reaches the admin, and the node is not passed over for it.
func TestPinnedRequestGetsAWithheldAnswer(t *testing.T) {
	f := ttHeaderFleet(t)
	f.b.SetOmitTTRound(true)
	f.r.opts.AdminToken = "admin-token"
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, pastAccount+"?round=50", nil)
	req.Header.Set("X-Algo-API-Token", "admin-token")
	req.Header.Set("X-Algod-Mesh-Upstream", "b")
	f.r.ServeHTTP(rec, req)
	if rec.Code != 200 || rec.Header().Get("X-Algod-Mesh-Upstream") != "b" || rec.Header().Get(domain.TTRoundHeader) != "" {
		t.Fatalf("pinned to b: %d %s mark %q", rec.Code, rec.Header().Get("X-Algod-Mesh-Upstream"), rec.Header().Get(domain.TTRoundHeader))
	}
	if n := f.c.Hits(pastAccount); n != 0 {
		t.Fatalf("retried on c %d times", n)
	}
	if f.stats("b").RoundUnhonored {
		t.Fatal("b passed over for a pinned answer")
	}
}

// Under rate limits only the admin may pin: a known client's token and an
// anonymous client, each of which the limiter would serve, get 403.
func TestPinnedRequestUnderRateLimits(t *testing.T) {
	f := newLimitedFleet(t, domain.Plan{Minute: perMinute(100)}, domain.Plan{Minute: perMinute(100)}, 0)
	pin := func(token string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v2/status", nil)
		req.RemoteAddr = clientA
		if token != "" {
			req.Header.Set("X-Algo-API-Token", token)
		}
		req.Header.Set("X-Algod-Mesh-Upstream", "c")
		f.r.ServeHTTP(rec, req)
		return rec
	}
	if rec := pin("admin-token"); rec.Code != 200 || rec.Header().Get("X-Algod-Mesh-Upstream") != "c" {
		t.Fatalf("admin: %d %s %s", rec.Code, rec.Header().Get("X-Algod-Mesh-Upstream"), rec.Body.String())
	}
	if rec := pin("acme-token"); rec.Code != 403 {
		t.Fatalf("plan token: %d %s", rec.Code, rec.Body.String())
	}
	if rec := pin(""); rec.Code != 403 {
		t.Fatalf("anonymous: %d %s", rec.Code, rec.Body.String())
	}
	if n := f.c.Hits("/v2/status"); n != 1 {
		t.Fatalf("c hit %d times, want the admin's request only", n)
	}
}

// A pinned write goes to the named node alone, body intact, even with
// multi_broadcast.
func TestPinnedBroadcastGoesToOneNode(t *testing.T) {
	h := newTwoPeerHarnessOpts(t, nil, func(o *RouterOptions) { withTokens(o); o.MultiBroadcast = true })
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v2/transactions", strings.NewReader("txnbytes"))
	req.Header.Set("X-Algo-API-Token", "admin-token")
	req.Header.Set("X-Algod-Mesh-Upstream", "c")
	h.r.ServeHTTP(rec, req)
	if rec.Code != 200 || rec.Header().Get("X-Algod-Mesh-Upstream") != "c" {
		t.Fatalf("pinned write: %d %s %s", rec.Code, rec.Header().Get("X-Algod-Mesh-Upstream"), rec.Body.String())
	}
	if b, c, e := h.n.Hits("/v2/transactions"), h.c.Hits("/v2/transactions"), h.ext.Hits("/v2/transactions"); b != 0 || c != 1 || e != 0 {
		t.Fatalf("write sent to b=%d c=%d ext=%d, want c only", b, c, e)
	}
	id, want := txIDFrom(rec.Body.Bytes()), directTxID(t, h.c, "txnbytes")
	if id == "" || id != want {
		t.Fatalf("body not forwarded intact: got id %q want %q", id, want)
	}
	// Pending lookups for it go where it was sent.
	if up, ok := h.r.recallTxn(id); !ok || up != "c" {
		t.Fatalf("broadcast remembered at %q (%v), want c", up, ok)
	}
}

// A wait for a block is forwarded to the named node, not held or answered
// for the fleet.
func TestPinnedWaitIsForwarded(t *testing.T) {
	h := newTwoPeerHarnessOpts(t, nil, withTokens)
	const path = "/v2/status/wait-for-block-after/5"
	if rec := h.pinnedGet(path, "admin-token", "c"); rec.Code != 200 || rec.Header().Get("X-Algod-Mesh-Upstream") != "c" {
		t.Fatalf("pinned wait: %d %s %s", rec.Code, rec.Header().Get("X-Algod-Mesh-Upstream"), rec.Body.String())
	}
	if b, c := h.n.Hits(path), h.c.Hits(path); b != 0 || c != 1 {
		t.Fatalf("wait sent to b=%d c=%d, want c only", b, c)
	}
}

// Ids name one upstream: a peer with no address can't be pinned, and an id
// that two upstreams have is refused rather than guessed.
func TestPinnedRequestToAnAddresslessOrAmbiguousUpstream(t *testing.T) {
	h := newTwoPeerHarnessOpts(t, nil, withTokens)
	recB := h.peerRecord(addrA)
	recB.Endpoints, recB.Token = []string{h.n.URL()}, h.n.Token()
	recC := domain.NodeRecord{ID: "c", Network: "n", Agent: domain.AgentInfo{Addrs: []string{addrB}, PubKey: []byte(h.cKey.Public().(ed25519.PublicKey))},
		Endpoints: []string{h.c.URL()}, Token: h.c.Token()}
	// d advertises no algod endpoint; the peer "ext" has the external's name.
	recD := domain.NodeRecord{ID: "d", Network: "n", Agent: domain.AgentInfo{Addrs: []string{"192.0.2.9:4001"}}}
	recExt := domain.NodeRecord{ID: "ext", Network: "n", Agent: domain.AgentInfo{Addrs: []string{"192.0.2.10:4001"}}, Endpoints: []string{h.n.URL()}}
	h.d.SetRecords([]domain.NodeRecord{recB, recC, recD, recExt})

	if rec := h.pinnedGet("/v2/status", "admin-token", "d"); rec.Code != 503 || !strings.Contains(rec.Body.String(), "no known address") || rec.Header().Get("X-Algod-Mesh-Mode") == "" {
		t.Fatalf("no address: %d %s mode %q", rec.Code, rec.Body.String(), rec.Header().Get("X-Algod-Mesh-Mode"))
	}
	if rec := h.pinnedGet("/v2/status", "admin-token", "ext"); rec.Code != 400 || !strings.Contains(rec.Body.String(), "ambiguous") {
		t.Fatalf("ambiguous: %d %s", rec.Code, rec.Body.String())
	}
	if rec := h.pinnedGet("/v2/status", "admin-token", "nope"); rec.Header().Get("X-Algod-Mesh-Mode") == "" {
		t.Fatal("unknown upstream answered without X-Algod-Mesh-Mode")
	}
	if n := h.ext.Hits("/v2/status") + h.n.Hits("/v2/status"); n != 0 {
		t.Fatalf("forwarded %d times", n)
	}
}

// Neither the pin header nor the admin's token reaches the backend.
func TestPinnedRequestHeadersAtTheBackend(t *testing.T) {
	var pin, tok []string
	spy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pin, tok = r.Header.Values("X-Algod-Mesh-Upstream"), r.Header.Values("X-Algo-API-Token")
		w.Write([]byte(`{}`))
	}))
	t.Cleanup(spy.Close)
	h := newTwoPeerHarnessOpts(t, func(o *DirectoryOptions) {
		o.Externals = append(o.Externals, ExternalUpstream{Name: "spy", URL: spy.URL, Token: "spy-token", HealthCheck: 5 * time.Second})
	}, withTokens)
	if rec := h.pinnedGet("/v2/status", "admin-token", "spy"); rec.Code != 200 || rec.Header().Get("X-Algod-Mesh-Upstream") != "spy" {
		t.Fatalf("pinned to spy: %d %s %s", rec.Code, rec.Header().Get("X-Algod-Mesh-Upstream"), rec.Body.String())
	}
	if len(pin) != 0 || len(tok) != 1 || tok[0] != "spy-token" {
		t.Fatalf("backend saw pin %q token %q", pin, tok)
	}
}

// Browsers may send the header.
func TestPreflightAllowsTheUpstreamHeader(t *testing.T) {
	h := newTwoPeerHarness(t, false)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/v2/status", nil)
	req.Header.Set("Origin", "https://example.com")
	h.r.ServeHTTP(rec, req)
	if got := rec.Header().Get("Access-Control-Allow-Headers"); !strings.Contains(got, "X-Algod-Mesh-Upstream") {
		t.Fatalf("Access-Control-Allow-Headers %q", got)
	}
}

// A pinned request in flight is not counted as load on the node: routing
// must not steer clients away from a node an admin is looking at.
func TestPinnedRequestInFlightIsNotLoad(t *testing.T) {
	h := newTwoPeerHarnessOpts(t, nil, withTokens)
	h.c.SetHang(true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		req := httptest.NewRequest(http.MethodGet, "/v2/status", nil).WithContext(ctx)
		req.Header.Set("X-Algo-API-Token", "admin-token")
		req.Header.Set("X-Algod-Mesh-Upstream", "c")
		h.r.ServeHTTP(httptest.NewRecorder(), req)
	}()
	waitFor(t, 2*time.Second, func() bool { return h.c.Hits("/v2/status") > 0 })
	n := pinnedStats(t, h.harness, "c").Inflight
	cancel()
	<-done
	if n != 0 {
		t.Fatalf("pinned request counted as %d in flight on c", n)
	}
}

// A pinned wait for a block may take as long as algod holds it, past the
// bound on an attempt; any other pinned request may not.
func TestPinnedWaitOutlastsTheAttemptBound(t *testing.T) {
	h := newTwoPeerHarnessOpts(t, nil, func(o *RouterOptions) { withTokens(o); o.UpstreamTimeout = 100 * time.Millisecond })
	h.c.SetLatency(300 * time.Millisecond)
	if rec := h.pinnedGet("/v2/status/wait-for-block-after/5", "admin-token", "c"); rec.Code != 200 || rec.Header().Get("X-Algod-Mesh-Upstream") != "c" {
		t.Fatalf("pinned wait: %d %s %s", rec.Code, rec.Header().Get("X-Algod-Mesh-Upstream"), rec.Body.String())
	}
	if rec := h.pinnedGet("/v2/status", "admin-token", "c"); rec.Code != 504 {
		t.Fatalf("pinned status past the attempt bound: %d %s", rec.Code, rec.Body.String())
	}
}

// The access log marks a pinned request, so that one sent to an excluded
// node does not read as a routing decision.
func TestPinnedRequestIsMarkedInTheLog(t *testing.T) {
	h := newTwoPeerHarnessOpts(t, nil, withTokens)
	var buf bytes.Buffer
	h.r.log = logging.New(&buf, slog.LevelInfo, false)
	h.pinnedGet("/v2/status", "admin-token", "c")
	if line := buf.String(); !strings.Contains(line, "pinned=true") || !strings.Contains(line, "upstream=c") {
		t.Fatalf("access log: %s", line)
	}
	buf.Reset()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v2/status", nil)
	req.Header.Set("X-Algo-API-Token", "admin-token")
	h.r.ServeHTTP(rec, req)
	if strings.Contains(buf.String(), "pinned") {
		t.Fatalf("routed request marked pinned: %s", buf.String())
	}
}

// A pinned wait-for-block-after gets as long as algod holds it, or
// upstream_timeout if longer, whatever request_timeout is; any other pinned
// request gets one attempt within the request's bound.
func TestPinnedTimeout(t *testing.T) {
	round := uint64(5)
	wait, status := domain.RequestClass{WaitAfter: &round}, domain.RequestClass{}
	for _, tc := range []struct {
		request, upstream, wait, other time.Duration
	}{
		{request: 30 * time.Second, upstream: 100 * time.Second, wait: 100 * time.Second, other: 30 * time.Second},
		{request: 120 * time.Second, upstream: 60 * time.Second, wait: pinnedWaitTimeout, other: 60 * time.Second},
		{request: 10 * time.Second, upstream: 20 * time.Second, wait: pinnedWaitTimeout, other: 10 * time.Second},
	} {
		r := &Router{opts: RouterOptions{RequestTimeout: tc.request, UpstreamTimeout: tc.upstream}}
		if got := r.pinnedTimeout(wait); got != tc.wait {
			t.Errorf("request %v upstream %v: wait bound %v, want %v", tc.request, tc.upstream, got, tc.wait)
		}
		if got := r.pinnedTimeout(status); got != tc.other {
			t.Errorf("request %v upstream %v: status bound %v, want %v", tc.request, tc.upstream, got, tc.other)
		}
	}
}
