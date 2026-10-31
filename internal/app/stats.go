package app

import (
	"sync"
	"time"

	"github.com/d13co/algod-mesh/internal/domain"
	"github.com/d13co/algod-mesh/internal/ports"
)

// BreakerOptions tunes the per-upstream circuit breaker.
type BreakerOptions struct {
	Threshold int           // consecutive failures that open the breaker
	OpenFor   time.Duration // how long requests are refused
	Window    int           // outcomes kept for the error rate
	Alpha     float64       // EWMA weight of the newest latency sample
}

// DefaultBreaker is a conservative default.
var DefaultBreaker = BreakerOptions{Threshold: 5, OpenFor: 10 * time.Second, Window: 50, Alpha: 0.2}

// StatsBook records passive observations per upstream id and produces the
// domain.Stats snapshot the selector consumes.
type StatsBook struct {
	opts  BreakerOptions
	clock ports.Clock

	mu sync.Mutex
	m  map[string]*upstreamStats
}

type upstreamStats struct {
	latency        map[string]float64
	window         []bool // true = failed
	windowPos      int
	windowFull     bool
	inflight       int
	failures       int
	probing        bool
	openUntil      time.Time
	throttledUntil time.Time
	busyUntil      [domain.SlotCount]time.Time
	holdersUntil   time.Time // holders index missing or stale until then
	untrustedUntil time.Time // time-travel history untrusted until then
	unhonoredUntil time.Time // answered without the round asked for; passed over until then
}

// NewStatsBook creates an empty book.
func NewStatsBook(opts BreakerOptions, clock ports.Clock) *StatsBook {
	if opts.Threshold == 0 {
		opts = DefaultBreaker
	}
	return &StatsBook{opts: opts, clock: clock, m: map[string]*upstreamStats{}}
}

func (b *StatsBook) get(id string) *upstreamStats {
	s, ok := b.m[id]
	if !ok {
		s = &upstreamStats{latency: map[string]float64{}, window: make([]bool, b.opts.Window)}
		b.m[id] = s
	}
	return s
}

// Begin marks a request in flight; call the returned func when done.
func (b *StatsBook) Begin(id string) func() {
	b.mu.Lock()
	b.get(id).inflight++
	b.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			b.mu.Lock()
			b.get(id).inflight--
			b.mu.Unlock()
		})
	}
}

// Record feeds one outcome for the given stat key.
func (b *StatsBook) Record(id, key string, out ports.Outcome) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.get(id)
	now := b.clock.Now()
	failed := out.Failed()
	s.window[s.windowPos] = failed
	s.windowPos = (s.windowPos + 1) % len(s.window)
	if s.windowPos == 0 {
		s.windowFull = true
	}
	if failed {
		s.failures++
		if s.failures >= b.opts.Threshold || s.probing {
			s.openUntil = now.Add(b.opts.OpenFor)
			s.probing = true
		}
		if out.Status == 429 {
			s.throttledUntil = now.Add(b.opts.OpenFor)
		}
		return
	}
	s.failures = 0
	s.probing = false
	ms := float64(out.Duration) / float64(time.Millisecond)
	if prev, ok := s.latency[key]; ok {
		s.latency[key] = prev + b.opts.Alpha*(ms-prev)
	} else {
		s.latency[key] = ms
	}
}

// Busy marks an upstream that answered a request taking slot with 503 and
// Retry-After: it has no room for more of them for d. It is not a failure.
func (b *StatsBook) Busy(id string, slot domain.Slot, d time.Duration) {
	if slot == domain.SlotNone {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.get(id)
	if until := b.clock.Now().Add(d); until.After(s.busyUntil[slot]) {
		s.busyUntil[slot] = until
	}
}

// HoldersMissing marks an upstream whose asset holders index is missing or
// stale, as its 503 to a holders page says, for d. It is not a failure.
func (b *StatsBook) HoldersMissing(id string, d time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.get(id)
	if until := b.clock.Now().Add(d); until.After(s.holdersUntil) {
		s.holdersUntil = until
	}
}

// HistoryUntrusted marks an upstream whose time-travel history is not
// served, as its 503 to a request at a round says, for d. It is not a
// failure.
func (b *StatsBook) HistoryUntrusted(id string, d time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.get(id)
	if until := b.clock.Now().Add(d); until.After(s.untrustedUntil) {
		s.untrustedUntil = until
	}
}

// RoundUnhonored marks an upstream that answered a request at a round
// without marking the answer with that round, for d. It is not a failure:
// its answers without a round are unaffected.
func (b *StatsBook) RoundUnhonored(id string, d time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.get(id)
	if until := b.clock.Now().Add(d); until.After(s.unhonoredUntil) {
		s.unhonoredUntil = until
	}
}

// Snapshot returns the current stats for an upstream.
func (b *StatsBook) Snapshot(id string) domain.Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.m[id]
	if !ok {
		return domain.Stats{}
	}
	now := b.clock.Now()
	n := s.windowPos
	if s.windowFull {
		n = len(s.window)
	}
	fails := 0
	for i := 0; i < n; i++ {
		if s.window[i] {
			fails++
		}
	}
	lat := make(map[string]float64, len(s.latency))
	for k, v := range s.latency {
		lat[k] = v
	}
	st := domain.Stats{LatencyMS: lat, Inflight: s.inflight,
		BreakerOpen: now.Before(s.openUntil), Throttled: now.Before(s.throttledUntil),
		HoldersMissing: now.Before(s.holdersUntil), HistoryUntrusted: now.Before(s.untrustedUntil),
		RoundUnhonored: now.Before(s.unhonoredUntil)}
	for slot, until := range s.busyUntil {
		if now.Before(until) {
			st.BusySlots = st.BusySlots.With(domain.Slot(slot))
		}
	}
	st.Busy = st.BusySlots != 0
	if n > 0 {
		st.ErrorRate = float64(fails) / float64(n)
	}
	return st
}
