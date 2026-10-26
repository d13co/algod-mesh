package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/d13co/algod-mesh/internal/ports"
)

func TestForwardStreamsAndSetsToken(t *testing.T) {
	var gotToken, gotHost string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotToken, gotHost = r.Header.Get("X-Algo-API-Token"), r.Host
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("X-Up", "1")
		w.WriteHeader(201)
		w.Write([]byte("echo:" + string(body)))
	}))
	defer up.Close()
	f := New(nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v2/transactions?x=1", strings.NewReader("payload"))
	out := f.Forward(rec, req, ports.Target{BaseURL: up.URL, Token: "tok"})
	if out.Err != nil || out.Status != 201 || !out.HeadersSent {
		t.Fatalf("outcome %+v", out)
	}
	if rec.Code != 201 || rec.Body.String() != "echo:payload" || rec.Header().Get("X-Up") != "1" {
		t.Fatalf("response %d %q", rec.Code, rec.Body.String())
	}
	if gotToken != "tok" || gotHost != strings.TrimPrefix(up.URL, "http://") {
		t.Fatalf("token %q host %q", gotToken, gotHost)
	}
}

func TestForwardRetryableStatusIsNotStreamed(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
		w.Write([]byte("down"))
	}))
	defer up.Close()
	f := New(nil)
	rec := httptest.NewRecorder()
	out := f.Forward(rec, httptest.NewRequest("GET", "/v2/status", nil), ports.Target{BaseURL: up.URL})
	if out.Status != 503 || !out.Failed() || out.HeadersSent {
		t.Fatalf("outcome %+v", out)
	}
	if rec.Body.Len() != 0 {
		t.Fatal("503 body must not reach the client")
	}
	// A per-target retry status (404 for pending lookups) behaves the same.
	up404 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(404) }))
	defer up404.Close()
	rec = httptest.NewRecorder()
	out = f.Forward(rec, httptest.NewRequest("GET", "/x", nil), ports.Target{BaseURL: up404.URL, RetryStatus: map[int]bool{404: true}})
	if out.Status != 404 || out.HeadersSent {
		t.Fatalf("outcome %+v", out)
	}
	rec = httptest.NewRecorder()
	out = f.Forward(rec, httptest.NewRequest("GET", "/x", nil), ports.Target{BaseURL: up404.URL})
	if out.Status != 404 || !out.HeadersSent || rec.Code != 404 {
		t.Fatalf("plain 404 must stream: %+v", out)
	}
}

func TestForwardTransportError(t *testing.T) {
	f := New(nil)
	rec := httptest.NewRecorder()
	out := f.Forward(rec, httptest.NewRequest("GET", "/v2/status", nil), ports.Target{BaseURL: "http://127.0.0.1:1"})
	if out.Err == nil || out.HeadersSent || out.Status != 0 {
		t.Fatalf("outcome %+v", out)
	}
}

// A withheld answer's JSON message reaches the router, so that it can tell
// dtalgorand's reasons for a 503 apart; the client sees none of it.
func TestForwardWithheldMessage(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(503)
		w.Write([]byte(`{"message":"timetravel: the asset holders index is not complete"}`))
	}))
	defer up.Close()
	rec := httptest.NewRecorder()
	out := New(nil).Forward(rec, httptest.NewRequest("GET", "/v2/assets/7/holders", nil), ports.Target{BaseURL: up.URL})
	if out.Status != 503 || out.Message != "timetravel: the asset holders index is not complete" || rec.Body.Len() != 0 {
		t.Fatalf("outcome %+v, client saw %q", out, rec.Body.String())
	}
	if m := errorMessage(strings.NewReader("down")); m != "" {
		t.Fatalf("message of a body that is not JSON: %q", m)
	}
}

// Retry-After reads in seconds or as an HTTP date; 0 and a past date still
// ask for a retry, and what cannot be read is none.
func TestRetryAfter(t *testing.T) {
	now := time.Date(2026, 10, 21, 7, 28, 0, 0, time.UTC)
	for v, want := range map[string]time.Duration{
		"":                              0,
		"5":                             5 * time.Second,
		" 2 ":                           2 * time.Second,
		"0":                             time.Second,
		"-1":                            0,
		"soon":                          0,
		"Wed, 21 Oct 2026 07:28:07 GMT": 7 * time.Second,
		"Wed, 21 Oct 2026 07:00:00 GMT": time.Second,
	} {
		if got := retryAfter(v, now); got != want {
			t.Errorf("retryAfter(%q) = %v, want %v", v, got, want)
		}
	}
}
