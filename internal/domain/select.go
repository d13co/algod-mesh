package domain

import (
	"math"
	"sort"
)

// Weights tunes the load-balancer score. Zero values are replaced by
// DefaultWeights in Score.
type Weights struct {
	ErrorPenaltyMS  float64 // added per unit of error rate (0..1)
	LagPenaltyMS    float64 // added per round of lag behind best
	InflightMS      float64 // added per in-flight request
	LocalBonusMS    float64 // subtracted for the local node (free network hop)
	UnknownLatMS    float64 // assumed latency when no sample exists
	ExternalPenalty float64 // added to external upstreams so the mesh wins ties
}

// DefaultWeights are sane starting values in milliseconds.
var DefaultWeights = Weights{
	ErrorPenaltyMS:  2000,
	LagPenaltyMS:    50,
	InflightMS:      5,
	LocalBonusMS:    2,
	UnknownLatMS:    20,
	ExternalPenalty: 100,
}

// Score is lower-is-better. It is a pure function of the snapshot.
func Score(u Upstream, c RequestClass, bestRound uint64, w Weights) float64 {
	if w == (Weights{}) {
		w = DefaultWeights
	}
	lat, ok := u.Stats.LatencyMS[c.StatKey()]
	if !ok {
		// Fall back to the default bucket, then to the unknown assumption.
		if lat, ok = u.Stats.LatencyMS["default"]; !ok {
			lat = w.UnknownLatMS
		}
	}
	s := lat
	s += w.ErrorPenaltyMS * u.Stats.ErrorRate
	if bestRound > u.LastRound {
		s += w.LagPenaltyMS * float64(bestRound-u.LastRound)
	}
	s += w.InflightMS * float64(u.Stats.Inflight)
	if u.Kind == KindLocal {
		s -= w.LocalBonusMS
	}
	if u.Kind == KindExternal {
		s += w.ExternalPenalty
	}
	return s
}

// Rand is the randomness the selector needs. math/rand/v2 satisfies it.
type Rand interface {
	IntN(n int) int
}

// Selection is the router's decision plus the information needed to retry.
type Selection struct {
	Chosen     Upstream
	Alternates []Upstream // remaining eligible upstreams, best first
	Tier       int
}

// Select picks an upstream for class c out of candidates according to mode.
//
// Tiers are consulted in order. The local node is its own tier ahead of
// everything in fallback mode; in loadbalancer mode it competes with peers of
// the same tier. Within a tier, fallback picks the best score
// deterministically (stable, no herd because it is one client per host) and
// loadbalancer uses power-of-two-choices on score.
func Select(mode Mode, cands []Upstream, c RequestClass, bestRound, tolerance uint64, w Weights, rnd Rand) (Selection, bool) {
	elig := make([]Upstream, 0, len(cands))
	for _, u := range cands {
		if Eligible(u, c, bestRound, tolerance) {
			u.Score = Score(u, c, bestRound, w)
			elig = append(elig, u)
		}
	}
	if len(elig) == 0 {
		return Selection{}, false
	}
	// Preferences eligibility leaves out, since every eligible node serves
	// the request, order the nodes within a tier (see rank).
	var p preferences
	for _, u := range elig {
		p.tt = p.tt || u.ServesHistory()
		p.stream = p.stream || u.Caps.QueryLimits().Stream
	}
	rank := make(map[string]int, len(elig))
	for _, u := range elig {
		rank[u.ID] = p.rank(u, c)
	}
	// Order: effective tier, then rank, then score. Ties broken by ID for
	// determinism.
	sort.SliceStable(elig, func(i, j int) bool {
		ti, tj := effectiveTier(mode, elig[i]), effectiveTier(mode, elig[j])
		if ti != tj {
			return ti < tj
		}
		if ri, rj := rank[elig[i].ID], rank[elig[j].ID]; ri != rj {
			return ri < rj
		}
		if elig[i].Score != elig[j].Score {
			return elig[i].Score < elig[j].Score
		}
		return elig[i].ID < elig[j].ID
	})
	// The pool is the top tier's best rank, so that power-of-two-choices
	// cannot fall on a node another one is preferred to.
	top, best := effectiveTier(mode, elig[0]), rank[elig[0].ID]
	n := 0
	for n < len(elig) && effectiveTier(mode, elig[n]) == top && rank[elig[n].ID] == best {
		n++
	}
	idx := 0
	if mode == ModeLoadBalancer && n > 1 && rnd != nil {
		a := rnd.IntN(n)
		b := rnd.IntN(n)
		if elig[b].Score < elig[a].Score {
			a = b
		}
		idx = a
	}
	chosen := elig[idx]
	alts := make([]Upstream, 0, len(elig)-1)
	alts = append(alts, elig[:idx]...)
	alts = append(alts, elig[idx+1:]...)
	return Selection{Chosen: chosen, Alternates: alts, Tier: chosen.Tier}, true
}

// preferences are what the eligible nodes offer that a request may prefer.
type preferences struct {
	tt     bool // a node serves its time-travel history
	stream bool // a node streams query answers
}

// rank is how far u is from the node c prefers, 0 for none: a sum of the
// preferences it misses, the largest weighing more than the rest together.
// Each counts only when another eligible node meets it:
//
//   - a query that accepts text/event-stream prefers a node that streams the
//     answer to one that sends it whole;
//   - a time-travel node is the tie-breaker against a peer sitting on the
//     edge of its state window: its LastRound comes from a heartbeat that can
//     trail the node by a round, so the round may already be unservable
//     there, while history answers it exactly.
func (p preferences) rank(u Upstream, c RequestClass) int {
	r := 0
	if p.stream && c.EventStream && !u.Caps.QueryLimits().Stream {
		r += 2
	}
	if p.tt && stateWindowEdge(u, c) {
		r++
	}
	return r
}

// stateWindowEdge reports whether u is a non-local, non-time-travel node whose
// account-state window (or delta window) begins exactly at the requested
// round. It is a selection-only preference: eligible stays exact, and a fleet
// with no time-travel node still routes such a round to this node.
func stateWindowEdge(u Upstream, c RequestClass) bool {
	if c.Round == nil || c.RoundWindow == WindowBlock || u.ServesHistory() || u.Kind == KindLocal {
		return false
	}
	if u.LastRound < u.Caps.MaxAcctLookback {
		return false // the window reaches genesis: no edge to fall off
	}
	oldest, ok := stateOldest(u, c.RoundWindow)
	return ok && *c.Round == oldest
}

// effectiveTier places the local node ahead of everything in fallback mode
// and externals behind everything in both modes.
func effectiveTier(mode Mode, u Upstream) int {
	switch u.Kind {
	case KindLocal:
		if mode == ModeFallback {
			return math.MinInt32
		}
		return u.Tier
	case KindExternal:
		return math.MaxInt32/2 + u.Tier
	}
	return u.Tier
}
