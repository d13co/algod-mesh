package domain

import "time"

// Eligible reports whether an upstream may serve a request of class c given
// the best round known across the fleet and the sync tolerance.
//
// The rules, in order: not excluded by config; reachable; not draining;
// breaker closed; not throttled, nor busy for the request's slot; time-travel
// history when the request needs it, and the asset holders index, not known
// to be missing; synced (within tolerance of best) unless the class is
// local-only or the request is for a specific round the node holds; the
// round is in the window the class needs (block or account state), and
// every round a simulation lists has been reached; developer API
// when needed; the simulate or query limits and options the request needs;
// no broadcast to followers.
func Eligible(u Upstream, c RequestClass, bestRound, tolerance uint64) bool {
	if u.Excluded || !u.Health.Reachable() || u.Draining {
		return false
	}
	if u.Stats.BreakerOpen || u.Stats.Throttled || u.Stats.BusySlots.Has(c.Slot()) {
		return false
	}
	if c.LocalOnly && u.Kind != KindLocal {
		return false
	}
	if c.TimeTravel && !u.Caps.TimeTravel {
		return false
	}
	if c.Holders && (!u.Caps.HoldersIndex || u.Stats.HoldersMissing) {
		return false
	}
	if c.Round != nil {
		r := *c.Round
		if r > u.LastRound || c.RoundHigh > u.LastRound {
			return false
		}
		switch {
		case u.Caps.TimeTravel && c.HistoryAnyRound:
			// The history covers every round at or before the node's last.
		case u.Caps.TimeTravel:
			// History does not extend the block window here (state deltas,
			// full blocks): the stored block is still required.
			if r < u.Caps.OldestRound {
				return false
			}
		case c.RoundWindow != WindowBlock:
			if !stateWindowCovers(u, r, c.RoundWindow) {
				return false
			}
		default:
			if r < u.Caps.OldestRound {
				return false
			}
		}
		// A node that holds the round can serve it even while lagging,
		// unless a simulation also asks for the latest round.
		if c.needsLatest() && !inSync(u, bestRound, tolerance) {
			return false
		}
	} else if !c.LocalOnly && !inSync(u, bestRound, tolerance) {
		return false
	}
	if c.WaitAfter != nil && *c.WaitAfter > u.LastRound+1 {
		// Waiting on a node that is behind the caller's round would block
		// for longer than necessary; prefer nodes at or beyond it.
		return false
	}
	if c.NeedsDev && !u.Caps.DeveloperAPI {
		return false
	}
	if c.SimNeeds != nil && !c.SimNeeds.Fits(u.Caps) {
		return false
	}
	if c.Query && !c.QueryAnyNode && !u.Caps.Queries() {
		return false
	}
	if c.QueryNeeds != nil && !c.QueryNeeds.Fits(u.Caps) {
		return false
	}
	if c.Broadcast && u.Caps.FollowMode {
		return false
	}
	return true
}

// stateWindowCovers reports whether a plain node answers a state question at
// round r in window w. A zero lookback (a heartbeat from an older agent, or a
// config that omits the key) falls back to the block window rather than
// treating the state window as empty.
func stateWindowCovers(u Upstream, r uint64, w RoundWindow) bool {
	oldest, ok := stateOldest(u, w)
	if !ok {
		return r >= u.Caps.OldestRound
	}
	return r >= oldest
}

// stateOldest returns the oldest round a plain node answers in state window w:
// the ledger answers Latest-MaxAcctLookback..Latest, the oldest round from
// disk, but keeps a delta only for the rounds after it. ok is false when the
// lookback is unknown (zero).
func stateOldest(u Upstream, w RoundWindow) (oldest uint64, ok bool) {
	lb := u.Caps.MaxAcctLookback
	if lb == 0 {
		return 0, false
	}
	if u.LastRound > lb {
		oldest = u.LastRound - lb // otherwise the window reaches genesis
	}
	if w == WindowDelta {
		oldest++
	}
	return oldest, true
}

// inSync trusts a Synced/Lagging judgement when one was made (it may carry
// hysteresis) and falls back to comparing rounds for a plain Online upstream.
func inSync(u Upstream, best, tol uint64) bool {
	switch u.Health {
	case HealthSynced:
		return true
	case HealthLagging:
		return false
	}
	return synced(u.LastRound, best, tol)
}

func synced(round, best, tol uint64) bool {
	return round+tol >= best
}

// SyncHealth judges Synced vs Lagging for a reachable upstream. Unreachable
// health values pass through unchanged.
func SyncHealth(h Health, round, best, tol uint64) Health {
	if !h.Reachable() {
		return h
	}
	if synced(round, best, tol) {
		return HealthSynced
	}
	return HealthLagging
}

// BestRound returns the highest last round among upstreams that are reachable
// and not excluded: the round some upstream a request may go to has reached.
// External upstreams count too: if the whole mesh is lagging behind the world
// we want to know.
func BestRound(us []Upstream) uint64 {
	var best uint64
	for _, u := range us {
		if u.Health.Reachable() && !u.Excluded && u.LastRound > best {
			best = u.LastRound
		}
	}
	return best
}

// CouldServe reports whether some upstream has what class c needs, whatever
// its state: reachable or not, draining, breaker open, lagging. Its last
// known round still has to cover the rounds c names. The router relaxes a
// simulate request only when nothing could serve it, not while the nodes
// that could are briefly out, which a client should hear as a 503 and retry.
func CouldServe(us []Upstream, c RequestClass) bool {
	for _, u := range us {
		u.Health, u.Draining, u.Stats = HealthSynced, false, Stats{}
		if Eligible(u, c, u.LastRound, 0) {
			return true
		}
	}
	return false
}

// FleetCapabilitiesReply is GET /v2/capabilities, in dtalgorand's own
// form, answered for the fleet.
type FleetCapabilitiesReply struct {
	TimeTravel       bool                `json:"time-travel"`
	DeveloperAPI     bool                `json:"developer-api"`
	TimeLimitSeconds uint64              `json:"time-limit-seconds"`
	Simulate         FleetSimulateLimits `json:"simulate"`
	// Query is what a POST /v2/query sent to the mesh may ask for; absent
	// when no eligible node has queries.
	Query *FleetQueryLimits `json:"query,omitempty"`
}

// FleetSimulateLimits is the capabilities response's simulate object, in
// dtalgorand's own form.
type FleetSimulateLimits struct {
	MaxTxnGroups            uint64 `json:"max-txn-groups"`
	MaxRounds               uint64 `json:"max-rounds"`
	MaxExtraOpcodeBudget    uint64 `json:"max-extra-opcode-budget"`
	ResourceLimitMultiplier uint64 `json:"resource-limit-multiplier"`
}

// FleetQueryLimits is the capabilities response's query object, in
// dtalgorand's own form.
type FleetQueryLimits struct {
	MaxCalls         uint64 `json:"max-calls"`
	MaxCallBudget    uint64 `json:"max-call-budget"`
	MaxCallLogBytes  uint64 `json:"max-call-log-bytes"`
	MaxResponseBytes uint64 `json:"max-response-bytes"`
	MaxRounds        uint64 `json:"max-rounds"`
}

// FleetCapabilities is GET /v2/capabilities answered for the fleet. Its
// simulate object is what a simulate request sent to the mesh may ask for:
// the limits of one eligible dtalgorand node, in full, so that any
// request within them fits that node and the router can send it there. Limits
// combined across nodes could promise a request no single node runs (many
// groups from one, many rounds from another). The node is the one whose
// limits are largest, compared in order: groups, rounds, extra opcode budget,
// time travel, developer API, then its id for a stable answer. A node whose
// limits are all at least another's always wins. The resource limit
// multiplier is the smallest of every eligible node instead, stock ones
// included: no request asks for it, so the router may send a request to any
// of them, and that is what it is sure to get. ok is false when no
// eligible upstream reports limits, a fleet of stock algods.
//
// Time travel and the developer API are the fleet's: true when any node not
// excluded has them, since the router sends the requests that need them to
// such a node. A client (go-dtalgorand-sdk) reads time-travel to choose
// between reads at a round and the latest state, keeps the answer, and
// would wrongly fall back if it were only the simulate node's. A node that
// is only out for now (lagging, busy, down) counts, with what it last had:
// a read at a round still routes to it once it is back, and until then gets
// a 503 to retry. So the features can come from a different node than the
// simulate limits: a simulation at an old round, or with an execution
// trace, may fit no node though it is within them.
//
// The time limit is the smallest any eligible dtalgorand node reports (0,
// no limit, counts as none), since deltas and versions pages may go to any
// of them, and at most upstreamTimeout in whole seconds (at least 1), after
// which the router gives up on a node with its own 504.
//
// The query object is that of the eligible node with the largest query
// limits, compared in order: calls, rounds, call budget, time travel, then
// its id. It is picked apart from the simulate limits, as a query is routed
// apart from a simulation.
func FleetCapabilities(us []Upstream, bestRound, tolerance uint64, upstreamTimeout time.Duration) (FleetCapabilitiesReply, bool) {
	var best, bestQuery *Upstream
	var multiplier, timeLimit uint64
	var timeTravel, developerAPI bool
	plain := RequestClass{Simulate: true, Idempotent: true}
	query := RequestClass{Query: true, Idempotent: true}
	for i := range us {
		u := &us[i]
		if c, ok := u.KnownCaps(); ok && !u.Excluded {
			timeTravel = timeTravel || c.TimeTravel
			developerAPI = developerAPI || c.DeveloperAPI
		}
		if Eligible(*u, query, bestRound, tolerance) && (bestQuery == nil || queryLimitsAbove(*u, *bestQuery)) {
			bestQuery = u
		}
		if !Eligible(*u, plain, bestRound, tolerance) {
			continue
		}
		if m := u.Caps.SimulateLimits().ResourceLimitMultiplier; multiplier == 0 || m < multiplier {
			multiplier = m
		}
		if u.Caps.Simulate == nil {
			continue
		}
		if t := u.Caps.Simulate.TimeLimitSeconds; t > 0 && (timeLimit == 0 || t < timeLimit) {
			timeLimit = t
		}
		if best == nil || simulateLimitsAbove(*u, *best) {
			best = u
		}
	}
	if best == nil {
		return FleetCapabilitiesReply{}, false
	}
	if upstreamTimeout > 0 {
		cut := max(uint64(upstreamTimeout/time.Second), 1)
		if timeLimit == 0 || timeLimit > cut {
			timeLimit = cut
		}
	}
	l := best.Caps.SimulateLimits()
	reply := FleetCapabilitiesReply{TimeTravel: timeTravel, DeveloperAPI: developerAPI, TimeLimitSeconds: timeLimit,
		Simulate: FleetSimulateLimits{MaxTxnGroups: l.MaxTxnGroups, MaxRounds: l.MaxRounds,
			MaxExtraOpcodeBudget: l.MaxExtraOpcodeBudget, ResourceLimitMultiplier: multiplier}}
	if bestQuery != nil {
		q := bestQuery.Caps.QueryLimits()
		reply.Query = &FleetQueryLimits{MaxCalls: q.MaxCalls, MaxCallBudget: q.MaxCallBudget, MaxCallLogBytes: q.MaxCallLogBytes,
			MaxResponseBytes: q.MaxResponseBytes, MaxRounds: q.MaxRounds}
	}
	return reply, true
}

// queryLimitsAbove orders nodes for the query object of FleetCapabilities.
func queryLimitsAbove(a, b Upstream) bool {
	la, lb := a.Caps.QueryLimits(), b.Caps.QueryLimits()
	switch {
	case la.MaxCalls != lb.MaxCalls:
		return la.MaxCalls > lb.MaxCalls
	case la.MaxRounds != lb.MaxRounds:
		return la.MaxRounds > lb.MaxRounds
	case la.MaxCallBudget != lb.MaxCallBudget:
		return la.MaxCallBudget > lb.MaxCallBudget
	case a.Caps.TimeTravel != b.Caps.TimeTravel:
		return a.Caps.TimeTravel
	}
	return a.ID < b.ID
}

// simulateLimitsAbove orders nodes for FleetCapabilities: whether a's
// limits come before b's.
func simulateLimitsAbove(a, b Upstream) bool {
	la, lb := a.Caps.SimulateLimits(), b.Caps.SimulateLimits()
	switch {
	case la.MaxTxnGroups != lb.MaxTxnGroups:
		return la.MaxTxnGroups > lb.MaxTxnGroups
	case la.MaxRounds != lb.MaxRounds:
		return la.MaxRounds > lb.MaxRounds
	case la.MaxExtraOpcodeBudget != lb.MaxExtraOpcodeBudget:
		return la.MaxExtraOpcodeBudget > lb.MaxExtraOpcodeBudget
	case a.Caps.TimeTravel != b.Caps.TimeTravel:
		return a.Caps.TimeTravel
	case a.Caps.DeveloperAPI != b.Caps.DeveloperAPI:
		return a.Caps.DeveloperAPI
	}
	return a.ID < b.ID
}
