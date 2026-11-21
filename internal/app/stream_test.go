package app

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/d13co/algod-mesh/internal/domain"
)

// streamQueryLimits is a dtalgorand whose queries stream.
var streamQueryLimits = func() domain.SimulateLimits {
	l := testQueryLimits
	l.Query.Stream = true
	return l
}()

// streamServer serves f's router over HTTP, as clientA, so that a stream is
// read as it is written and its trailers arrive.
func streamServer(t *testing.T, f *simFleet) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.RemoteAddr = clientA
		f.r.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// postStream posts a query that accepts a stream, and reads its first
// event.
func postStream(t *testing.T, srv *httptest.Server, body string) (*http.Response, *bufio.Reader) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v2/query", strings.NewReader(body))
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	if resp.StatusCode != 200 || !domain.IsEventStream(resp.Header.Get("Content-Type")) {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("stream: %d %q %s", resp.StatusCode, resp.Header.Get("Content-Type"), b)
	}
	rd := bufio.NewReader(resp.Body)
	if line, err := rd.ReadString('\n'); err != nil || line != "event: start\n" {
		t.Fatalf("first line %q: %v", line, err)
	}
	return resp, rd
}

// A query that accepts a stream goes to a node that streams, ahead of one
// routing would otherwise pick, and reaches the client as Server-Sent
// Events; one that does not goes where routing sends it.
func TestQueryStreamRoutesToStreamingNode(t *testing.T) {
	f := newSimPair(t, domain.Capabilities{Simulate: &testQueryLimits}, domain.Capabilities{Simulate: &streamQueryLimits},
		RouterOptions{Mode: domain.ModeFallback, RetryBudget: 1}, DirectoryOptions{})
	body := `{"program":"AQ==","calls":[{}]}`
	if up := f.query(t, body); up != "b" {
		t.Fatalf("query went to %q, want b", up)
	}
	resp, rd := postStream(t, streamServer(t, f), body)
	if up := resp.Header.Get("X-Algod-Mesh-Upstream"); up != "c" {
		t.Fatalf("stream went to %q, want c", up)
	}
	rest, _ := io.ReadAll(rd)
	if !strings.Contains(string(rest), "event: log\n") || !strings.HasSuffix(string(rest), "event: end\ndata: {}\n\n") {
		t.Fatalf("stream %q", rest)
	}
	// Without a node that streams, the answer comes whole.
	f = newSimFleet(t, domain.Capabilities{Simulate: &testQueryLimits})
	req := httptest.NewRequest(http.MethodPost, "/v2/query", strings.NewReader(body))
	req.Header.Set("Accept", "text/event-stream")
	rec := httptest.NewRecorder()
	f.r.ServeHTTP(rec, req)
	if rec.Code != 200 || rec.Header().Get("X-Algod-Mesh-Upstream") != "c" || domain.IsEventStream(rec.Header().Get("Content-Type")) {
		t.Fatalf("no streaming node: %d %q %q", rec.Code, rec.Header().Get("X-Algod-Mesh-Upstream"), rec.Header().Get("Content-Type"))
	}
}

// newStreamQueryFleet is newLimitedQueryFleet with c streaming.
func newStreamQueryFleet(t *testing.T, free domain.Plan, perSecond float64) *simFleet {
	t.Helper()
	return newSimFleetWith(t, domain.Capabilities{Simulate: &streamQueryLimits}, RouterOptions{Mode: domain.ModeFallback, RetryBudget: 1,
		AdminToken: "admin-token", RateLimit: &LimiterOptions{
			Plans: map[string]domain.Plan{"free": free}, AnonymousPlan: "free",
			Weights: testWeights, SimulatePerSecond: perSecond}})
}

// A streamed query is billed by the effort its node reports in a trailer,
// which the client does not get, and its cost goes out as a trailer.
func TestQueryStreamBilledByTrailerEffort(t *testing.T) {
	f := newStreamQueryFleet(t, domain.Plan{Minute: perMinute(1000)}, 1)
	f.c.SetEffort("/v2/query", "opcodes=660000, lookups=0, history=0, scanned=0, response-bytes=0, eval-us=5")
	resp, rd := postStream(t, streamServer(t, f), `{"program":"AQ==","calls":[{},{}]}`)
	if resp.Header.Get(costHeader) != "" || resp.Header.Get("RateLimit") == "" || resp.Header.Get(domain.EffortHeader) != "" {
		t.Fatalf("headers %v", resp.Header)
	}
	if _, err := io.ReadAll(rd); err != nil {
		t.Fatal(err)
	}
	if got := resp.Trailer.Get(costHeader); got != "4" {
		t.Fatalf("cost trailer %q, want 4: %v", got, resp.Trailer)
	}
	if _, ok := resp.Trailer[domain.EffortHeader]; ok {
		t.Fatalf("effort trailer reached a billed client: %v", resp.Trailer)
	}
}

// A cost trailer a node sends does not reach the client: the mesh's is the
// only one.
func TestQueryStreamNodeCostTrailerDropped(t *testing.T) {
	f := newStreamQueryFleet(t, domain.Plan{Minute: perMinute(1000)}, 1)
	f.c.SetEffort("/v2/query", "opcodes=660000, lookups=0, history=0, scanned=0, response-bytes=0, eval-us=5")
	f.c.SetStreamTrailer(costHeader, "0")
	resp, rd := postStream(t, streamServer(t, f), `{"program":"AQ==","calls":[{},{}]}`)
	if _, err := io.ReadAll(rd); err != nil {
		t.Fatal(err)
	}
	if got := resp.Trailer.Values(costHeader); len(got) != 1 || got[0] != "4" {
		t.Fatalf("cost trailers %q, want [4]", got)
	}
}

// A streamed query whose node reports no effort is metered until it ends,
// not until its headers go out.
func TestQueryStreamMeteredUntilItEnds(t *testing.T) {
	f := newStreamQueryFleet(t, domain.Plan{Minute: perMinute(1000)}, 100)
	f.c.SetSimulate(time.Second, 0, 0)
	resp, rd := postStream(t, streamServer(t, f), `{"program":"AQ==","calls":[{}]}`)
	f.h.fc.Advance(time.Second)
	rest, err := io.ReadAll(rd)
	if err != nil || !strings.Contains(string(rest), "event: end\n") {
		t.Fatalf("stream %q: %v", rest, err)
	}
	if got := resp.Trailer.Get(costHeader); got != "101" {
		t.Fatalf("cost trailer %q, want 101", got)
	}
}

// A streamed query is cancelled once its client is overdrawn, though its
// headers went out: the stream ends without its end event.
func TestQueryStreamCancelledWhenOverdrawn(t *testing.T) {
	f := newStreamQueryFleet(t, domain.Plan{Minute: perMinute(10)}, 100)
	f.c.SetSimulate(10*time.Second, 0, 0)
	_, rd := postStream(t, streamServer(t, f), `{"program":"AQ==","calls":[{}]}`)
	f.h.fc.Advance(time.Second)
	f.r.Limiter().tick()
	done := make(chan string)
	go func() {
		rest, _ := io.ReadAll(rd)
		done <- string(rest)
	}()
	select {
	case rest := <-done:
		if strings.Contains(rest, "event: end") {
			t.Fatalf("cancelled stream ended normally: %q", rest)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stream not cancelled")
	}
	waitFor(t, 2*time.Second, func() bool { return f.c.SimulatesCancelled() == 1 })
}

// An admin, who is not billed, gets the node's effort trailer as it sent it.
func TestQueryStreamEffortForAdmin(t *testing.T) {
	f := newStreamQueryFleet(t, domain.Plan{Minute: perMinute(1000)}, 1)
	f.c.SetEffort("/v2/query", "opcodes=1, lookups=0, history=0, scanned=0, response-bytes=0, eval-us=5")
	srv := streamServer(t, f)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v2/query", strings.NewReader(`{"program":"AQ==","calls":[{}]}`))
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("X-Algo-API-Token", "admin-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Trailer.Get(domain.EffortHeader), "opcodes=1,") || resp.Trailer.Get(costHeader) != "" {
		t.Fatalf("admin: %d trailers %v", resp.StatusCode, resp.Trailer)
	}
}

// A query program that runs time-travel opcodes goes where any other query
// goes, every node with queries being taken to run them; the fleet's
// capabilities list those of its query node.
func TestQueryOpcodesNotRouted(t *testing.T) {
	ops := streamQueryLimits
	ops.Query.MaxCalls, ops.Query.MaxTotalCalls = 2048, 2048
	ops.Query.Opcodes, ops.Query.MaxSeekRounds = domain.ParseTTOpcodes([]string{"tt_seek", "tt_acct_change"}), 64
	f := newSimPair(t, domain.Capabilities{Simulate: &testQueryLimits}, domain.Capabilities{Simulate: &ops, TimeTravel: true},
		RouterOptions{Mode: domain.ModeFallback, RetryBudget: 1}, DirectoryOptions{})
	// program: version 13, tt_acct_change (0xff 0x10), base64.
	if up := f.query(t, `{"program":"Df8Q","calls":[{}]}`); up != "b" {
		t.Fatalf("change lookup went to %q, want b", up)
	}
	var fleet struct {
		Query domain.FleetQueryLimits `json:"query"`
	}
	rec := f.do(http.MethodGet, "/v2/capabilities", nil)
	if err := json.Unmarshal(rec.Body.Bytes(), &fleet); err != nil || strings.Join(fleet.Query.Opcodes, ",") != "tt_seek,tt_acct_change" ||
		strings.Join(fleet.Query.Stream, ",") != "sse" || fleet.Query.MaxSeekRounds != 64 {
		t.Fatalf("capabilities %s: %v", rec.Body.String(), err)
	}
}
