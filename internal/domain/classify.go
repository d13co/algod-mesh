package domain

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// RoundWindow names the range in which a plain (non-time-travel) node can
// answer a request for one specific round.
type RoundWindow int

const (
	// WindowBlock is the block store: OldestRound..LastRound. Full blocks,
	// hashes, state proofs and block headers live here.
	WindowBlock RoundWindow = iota
	// WindowState is the account-state lookback: LastRound-MaxAcctLookback..
	// LastRound. Box listings and simulations at a round live here; the
	// block window overstates them by up to ~1000 rounds.
	WindowState
	// WindowDelta is the account-state lookback without its oldest round:
	// the ledger answers state at its oldest round from disk, but keeps a
	// delta only for the rounds after it. State deltas live here.
	WindowDelta
)

// RequestClass is everything routing needs to know about a client request.
type RequestClass struct {
	Round      *uint64 // request concerns one specific round
	Ahead      Reply   // algod's reply when Round is past its latest; zero when unknown
	NeedsDev   bool    // /v2/teal/* requires EnableDeveloperAPI
	Broadcast  bool    // POST /v2/transactions
	PendingID  string  // /v2/transactions/pending/{txid}
	WaitAfter  *uint64 // /v2/status/wait-for-block-after/{r}
	LocalOnly  bool    // must be answered by the local node (or the agent)
	Agent      bool    // /mesh/* served by the agent itself
	Idempotent bool    // safe to retry on another upstream before bytes were sent
	// TimeTravel marks a request only a time-travel node can answer: state at
	// a past round (?round=) or an object history (/versions). A stock node
	// ignores round on those endpoints and would answer the latest state, so
	// they must never reach one without the history.
	TimeTravel bool
	// RoundWindow is the window a plain node must cover the round with. It is
	// irrelevant to a time-travel node, which answers the round from its
	// history for every round at or before its last when HistoryAnyRound is
	// set, and from its (stored) block window when not.
	RoundWindow RoundWindow
	// HistoryAnyRound marks a round a time-travel node serves from its
	// history for any round at or below its last, whatever RoundWindow says:
	// block headers, box listings and simulations at a round. Requests that
	// still need the stored block (full blocks, state deltas) leave it clear,
	// so a time-travel node serves deltas only within its block window.
	HistoryAnyRound bool
	// Simulate marks POST /v2/transactions/simulate, whose body may carry a
	// round the router refines the class with before it selects.
	Simulate bool
	// SimNeeds is what a simulate request's body asks of the node that runs
	// it. The router fills it in once it has read the body (ApplySimulate);
	// it is nil before that and for every other request.
	SimNeeds *SimulateNeeds
	// Query marks POST /v2/query, dtalgorand's read-only TEAL calls, whose
	// body the router reads as it does a simulation's. Only a node that
	// reports query limits answers it, unless QueryAnyNode.
	Query bool
	// QueryNeeds is what a query's body asks of the node that runs it, filled
	// in once the router has read the body (ApplyQuery); nil before that, for
	// a body it could not read, and for every other request.
	QueryNeeds *QueryNeeds
	// QueryAnyNode lets a query reach a node without queries: the last
	// relaxation, when no node in the fleet has them, so that algod answers
	// with its 404 as it would alone.
	QueryAnyNode bool
	// RoundHigh is the latest round a simulate or query request's "rounds"
	// lists: a node must have reached it too, while Round, the earliest, is
	// the one its window must cover. 0 when the request names at most one
	// round. On a versions page it is max-round or the last of rounds, with
	// Round unset: the history covers every round before it.
	RoundHigh uint64
	// SimulateLimits marks GET /v2/transactions/simulate/limits, which the
	// router answers for the fleet (dtalgorand).
	SimulateLimits bool
	// Versions marks an object history page (.../versions).
	Versions bool
	// Deltas marks GET /v2/deltas/{round}, which a time-travel node
	// re-evaluates from its history for a round past its lookback.
	Deltas bool
	// Holders marks GET /v2/assets/{asset-id}/holders, an asset's holders
	// at a round, which only a node with TimeTravelHoldersIndex answers.
	Holders bool
}

// Slot is a kind of request that can keep a node busy for long. A node
// answers one with 503 and Retry-After when it has no room for more of its
// kind, and is then passed over for that kind for a while, while it keeps
// serving the rest.
type Slot uint8

const (
	SlotNone    Slot = iota // not limited this way
	SlotHeavy               // simulations and state deltas
	SlotHolders             // asset holders pages
	SlotCount               // the number of kinds, SlotNone included
)

// Slots is a set of Slot.
type Slots uint8

// Has reports whether s is in the set; SlotNone never is.
func (ss Slots) Has(s Slot) bool { return s != SlotNone && ss&(1<<s) != 0 }

// With returns the set with s added.
func (ss Slots) With(s Slot) Slots { return ss | 1<<s }

// Slot reports the kind of slot the request takes, SlotNone for most.
func (c RequestClass) Slot() Slot {
	switch {
	case c.Evaluates() || c.Deltas:
		return SlotHeavy
	case c.Holders:
		return SlotHolders
	}
	return SlotNone
}

// Evaluates reports whether the request runs TEAL: a simulation or a query.
// One can run for seconds, so it is billed by its running time and stopped
// when its client's allowance runs out, and it is not retried after it ran
// out of time.
func (c RequestClass) Evaluates() bool { return c.Simulate || c.Query }

// needsLatest reports whether a request that names past rounds also asks
// for the node's own latest round, which the node must then be in sync for.
func (c RequestClass) needsLatest() bool {
	return (c.SimNeeds != nil && c.SimNeeds.Latest) || (c.QueryNeeds != nil && c.QueryNeeds.Latest)
}

// SimulateNeeds is what a simulate request needs of the node that runs it.
// It is the router's preference, not algod's rule: when no node fits, the
// router relaxes it (Relaxed) and sends the request on, so that a node
// answers it, with its own error if need be, as a single algod would.
type SimulateNeeds struct {
	Groups            int    // len(txn-groups)
	Rounds            int    // len(rounds), duplicates included
	ExtraOpcodeBudget uint64 // extra-opcode-budget
	// Fork marks an option only dtalgorand has: parallel-groups, a validity
	// window mode or a protocol. Groups and Rounds beyond stock algod's are
	// told by the limits instead.
	Fork bool
	// DeveloperAPI marks an execution trace or protocol future, which algod
	// simulates only with EnableDeveloperAPI.
	DeveloperAPI bool
	// RoundsSpan marks Round and RoundHigh as taken from rounds, which a
	// node answers round by round: one it cannot simulate is an error in
	// that round's result, not in the response.
	RoundsSpan bool
	// Latest marks a rounds list with a 0 entry, which the node resolves to
	// its own last round: the node must be in sync, even though it holds
	// the past rounds the list names too.
	Latest bool
	// ExecTrace marks exec-trace-config, and UnnamedResources
	// allow-unnamed-resources: options a rate-limit plan may deny.
	ExecTrace        bool
	UnnamedResources bool
}

// Fits reports whether a node with capabilities c accepts the request.
func (n SimulateNeeds) Fits(c Capabilities) bool {
	if n.Fork && c.Simulate == nil {
		return false
	}
	if n.DeveloperAPI && !c.DeveloperAPI {
		return false
	}
	l := c.SimulateLimits()
	return uint64(n.Groups) <= l.MaxTxnGroups && uint64(n.Rounds) <= l.MaxRounds && n.ExtraOpcodeBudget <= l.MaxExtraOpcodeBudget
}

// SimulateBody is the part of a simulate request body routing reads. The
// tags are algod's for both msgpack and JSON; every other field is skipped.
type SimulateBody struct {
	TxnGroups         []struct{} `codec:"txn-groups" json:"txn-groups"`
	Round             uint64     `codec:"round" json:"round"`
	Rounds            []uint64   `codec:"rounds" json:"rounds"`
	ExtraOpcodeBudget int64      `codec:"extra-opcode-budget" json:"extra-opcode-budget"`
	ExecTraceConfig   struct {
		Enable bool `codec:"enable" json:"enable"`
	} `codec:"exec-trace-config" json:"exec-trace-config"`
	AllowUnnamedResources bool   `codec:"allow-unnamed-resources" json:"allow-unnamed-resources"`
	ParallelGroups        bool   `codec:"parallel-groups" json:"parallel-groups"`
	SkipValidityWindow    bool   `codec:"skip-validity-window" json:"skip-validity-window"`
	RewriteValidityWindow bool   `codec:"rewrite-validity-window" json:"rewrite-validity-window"`
	Protocol              string `codec:"protocol" json:"protocol"`
}

// protocolFuture is the protocol name only a node with the developer API
// simulates under (protocol.ConsensusFuture).
const protocolFuture = "future"

// ApplySimulate refines a simulate request's class with what its body asks.
// A round other than 0 (which means the latest) needs a node holding it:
// a plain node within its account-state lookback, a time-travel node at any
// round. A rounds list needs a node holding every round it names other than
// 0, so its earliest and latest bound the node's window.
func (c *RequestClass) ApplySimulate(b SimulateBody) {
	n := SimulateNeeds{Groups: len(b.TxnGroups), Rounds: len(b.Rounds),
		Fork:         b.ParallelGroups || b.SkipValidityWindow || b.RewriteValidityWindow || b.Protocol != "",
		DeveloperAPI: b.ExecTraceConfig.Enable || b.Protocol == protocolFuture,
		ExecTrace:    b.ExecTraceConfig.Enable, UnnamedResources: b.AllowUnnamedResources}
	if b.ExtraOpcodeBudget > 0 {
		n.ExtraOpcodeBudget = uint64(b.ExtraOpcodeBudget)
	}
	setRound := func(r uint64) {
		c.Round = &r
		c.RoundWindow = WindowState
		c.HistoryAnyRound = true
	}
	switch {
	case b.Round != 0:
		// With rounds as well the node rejects the request; routing it by
		// round gets that answer from a node that would otherwise serve it.
		setRound(b.Round)
	case len(b.Rounds) > 0:
		var lo, hi uint64
		for _, r := range b.Rounds {
			if r == 0 {
				n.Latest = true // the node's own latest round
				continue
			}
			if lo == 0 || r < lo {
				lo = r
			}
			hi = max(hi, r)
		}
		if hi > 0 {
			setRound(lo)
			c.RoundHigh = hi
			n.RoundsSpan = true
		}
	}
	c.SimNeeds = &n
}

// QueryNeeds is what a query needs of the node that runs it. Like
// SimulateNeeds it is the router's preference, relaxed when no node fits.
type QueryNeeds struct {
	Calls  int    // len(calls)
	Rounds int    // len(rounds), duplicates included; 0 is the latest alone
	Budget uint64 // the opcode budget each call asks for; 0 is the node's own
	// DeveloperAPI marks protocol future, which the node runs only with
	// EnableDeveloperAPI.
	DeveloperAPI bool
	// RoundsSpan marks Round and RoundHigh as taken from rounds. The node
	// answers each round alone: one it cannot read is an error in that
	// round's result.
	RoundsSpan bool
	// Latest marks a rounds list with a 0 entry, which the node resolves to
	// its own last round.
	Latest bool
}

// Fits reports whether a node with capabilities c accepts the query.
func (n QueryNeeds) Fits(c Capabilities) bool {
	if !c.Queries() || (n.DeveloperAPI && !c.DeveloperAPI) {
		return false
	}
	l := c.QueryLimits()
	return uint64(n.Calls) <= l.MaxCalls && uint64(n.Rounds) <= l.MaxRounds && n.Budget <= l.MaxCallBudget
}

// QueryBody is the part of a query body routing reads, with dtalgorand's
// tags for both msgpack and JSON; every other field is skipped.
type QueryBody struct {
	Calls    []struct{} `codec:"calls" json:"calls"`
	Rounds   []uint64   `codec:"rounds" json:"rounds"`
	Budget   uint64     `codec:"budget" json:"budget"`
	Protocol string     `codec:"protocol" json:"protocol"`
}

// ApplyQuery refines a query's class with what its body asks. Its rounds
// bound the node's window as a simulation's rounds list does: each is read
// from the account-state lookback, or from the history at any round. With
// no rounds, or only 0s, the query runs at the node's latest round.
func (c *RequestClass) ApplyQuery(b QueryBody) {
	n := QueryNeeds{Calls: len(b.Calls), Rounds: len(b.Rounds), Budget: b.Budget,
		DeveloperAPI: b.Protocol == protocolFuture}
	var lo, hi uint64
	for _, r := range b.Rounds {
		if r == 0 {
			n.Latest = true
			continue
		}
		if lo == 0 || r < lo {
			lo = r
		}
		hi = max(hi, r)
	}
	if hi > 0 {
		c.Round = &lo
		c.RoundWindow = WindowState
		c.HistoryAnyRound = true
		c.RoundHigh = hi
		n.RoundsSpan = true
	}
	c.QueryNeeds = &n
}

// Relaxed lists the classes selection falls back to, in order, when no
// upstream is eligible for c. A simulate request with a rounds list first
// drops the rounds, which the node reports one by one as it cannot simulate
// them, then the rest of its needs, so that a node rejects the request with
// its own message rather than the router with a 503. A single round is kept:
// a node that lacks it would answer the latest state or an error.
//
// A query drops its rounds the same way, then its limits, so that a node
// with queries says what it does not allow, and last its need for queries
// at all, so that algod answers 404 as it does alone.
func (c RequestClass) Relaxed() []RequestClass {
	if c.Query {
		return c.relaxedQuery()
	}
	if c.SimNeeds == nil {
		return nil
	}
	var out []RequestClass
	if c.SimNeeds.RoundsSpan {
		c.Round, c.RoundHigh, c.RoundWindow, c.HistoryAnyRound = nil, 0, WindowBlock, false
		out = append(out, c)
	}
	c.SimNeeds = nil
	return append(out, c)
}

func (c RequestClass) relaxedQuery() []RequestClass {
	if c.QueryAnyNode {
		return nil
	}
	var out []RequestClass
	if c.QueryNeeds != nil {
		if c.QueryNeeds.RoundsSpan {
			c.Round, c.RoundHigh, c.RoundWindow, c.HistoryAnyRound = nil, 0, WindowBlock, false
			out = append(out, c)
		}
		c.QueryNeeds = nil
		out = append(out, c)
	}
	c.QueryAnyNode = true
	return append(out, c)
}

// Reply is a status and message the router answers with in algod's place.
type Reply struct {
	Status  int
	Message string
}

// HighestRound is the latest round the request names, Round or RoundHigh;
// 0 when it names none.
func (c RequestClass) HighestRound() uint64 {
	if c.Round != nil {
		return max(*c.Round, c.RoundHigh)
	}
	return c.RoundHigh
}

// String renders the class for logs.
func (c RequestClass) String() string {
	var parts []string
	if c.Round != nil {
		parts = append(parts, fmt.Sprintf("round=%d", *c.Round))
	}
	if c.RoundHigh != 0 {
		parts = append(parts, fmt.Sprintf("round_high=%d", c.RoundHigh))
	}
	if c.WaitAfter != nil {
		parts = append(parts, fmt.Sprintf("wait_after=%d", *c.WaitAfter))
	}
	if c.NeedsDev {
		parts = append(parts, "dev")
	}
	if c.Broadcast {
		parts = append(parts, "broadcast")
	}
	if c.PendingID != "" {
		parts = append(parts, "pending="+c.PendingID)
	}
	if c.TimeTravel {
		parts = append(parts, "time_travel")
	}
	if c.Versions {
		parts = append(parts, "versions")
	}
	if c.Deltas {
		parts = append(parts, "deltas")
	}
	if c.Holders {
		parts = append(parts, "holders")
	}
	if c.Round != nil {
		switch c.RoundWindow {
		case WindowState:
			parts = append(parts, "state_window")
		case WindowDelta:
			parts = append(parts, "delta_window")
		}
	}
	if c.HistoryAnyRound {
		parts = append(parts, "history_any_round")
	}
	if n := c.SimNeeds; n != nil {
		if n.Groups > 1 {
			parts = append(parts, fmt.Sprintf("sim_groups=%d", n.Groups))
		}
		if n.Rounds > 0 {
			parts = append(parts, fmt.Sprintf("sim_rounds=%d", n.Rounds))
		}
		if n.ExtraOpcodeBudget > StockMaxExtraOpcodeBudget {
			parts = append(parts, fmt.Sprintf("sim_budget=%d", n.ExtraOpcodeBudget))
		}
		if n.Fork {
			parts = append(parts, "sim_fork")
		}
		if n.DeveloperAPI {
			parts = append(parts, "sim_dev")
		}
	}
	if c.Query {
		parts = append(parts, "query")
	}
	if n := c.QueryNeeds; n != nil {
		parts = append(parts, fmt.Sprintf("query_calls=%d", n.Calls))
		if n.Rounds > 0 {
			parts = append(parts, fmt.Sprintf("query_rounds=%d", n.Rounds))
		}
		if n.Budget > 0 {
			parts = append(parts, fmt.Sprintf("query_budget=%d", n.Budget))
		}
		if n.DeveloperAPI {
			parts = append(parts, "query_dev")
		}
	}
	if c.QueryAnyNode {
		parts = append(parts, "query_any_node")
	}
	if c.LocalOnly {
		parts = append(parts, "local_only")
	}
	if c.Idempotent {
		parts = append(parts, "idempotent")
	}
	return strings.Join(parts, ",")
}

// StatKey buckets requests for per-class latency tracking.
func (c RequestClass) StatKey() string {
	switch {
	case c.TimeTravel:
		// A history read, whether ?round= or /versions, is its own bucket.
		return "timetravel"
	case c.Simulate:
		// One simulation can run for seconds (many groups, many rounds):
		// it must not skew the latency of the requests it would share with.
		return "simulate"
	case c.Query:
		// Like a simulation, a query of many calls and rounds runs long.
		return "query"
	case c.Round != nil:
		return "round"
	case c.Broadcast:
		return "broadcast"
	case c.PendingID != "":
		return "pending"
	case c.WaitAfter != nil:
		return "wait"
	case c.NeedsDev:
		return "teal"
	}
	return "default"
}

// Classify derives the RequestClass from method, path and raw query. It is a
// pure function of its inputs and is the only place route knowledge lives.
func Classify(method, path, rawQuery string) RequestClass {
	var c RequestClass
	method = strings.ToUpper(method)
	c.Idempotent = method == "GET" || method == "HEAD"
	p := strings.TrimSuffix(path, "/")
	segs := strings.Split(strings.TrimPrefix(p, "/"), "/")
	seg := func(i int) string {
		if i < len(segs) {
			return segs[i]
		}
		return ""
	}

	if seg(0) == "mesh" {
		c.Agent = true
		c.LocalOnly = true
		return c
	}
	switch p {
	case "/health", "/ready", "/metrics", "/swagger.json", "/versions", "/genesis":
		c.LocalOnly = true
		return c
	}
	if seg(0) != "v2" {
		// Unknown top-level: keep it local, it is probably admin or a probe.
		c.LocalOnly = true
		return c
	}
	q := queryValues(rawQuery)
	roundParam := func() (uint64, bool) {
		s := q.Get("round")
		if s == "" {
			return 0, false
		}
		return parseRound(s)
	}
	// Every .../versions endpoint is the time-travel object history, and is
	// registered only when the node has EnableTimeTravel.
	if seg(len(segs)-1) == "versions" {
		c.TimeTravel = true
		c.Versions = true
		if hi, ok := versionsHigh(q); ok {
			c.RoundHigh = hi
			c.Ahead = Reply{400, "given round is greater than the latest round"}
		}
		return c
	}
	switch seg(1) {
	case "blocks", "deltas", "stateproofs":
		if r, ok := parseRound(seg(2)); ok {
			c.Round = &r
			const ledger = "failed to retrieve information from the ledger"
			sub := strings.Join(segs[3:], "/")
			switch {
			case method != "GET":
				// algod has no such route; Ahead stays unset.
			case seg(1) == "stateproofs" && sub == "", seg(1) == "blocks" && sub == "lightheader/proof":
				c.Ahead = Reply{500, "given round is greater than the latest round"}
			case seg(1) == "blocks" && len(segs) == 6 && seg(3) == "transactions" && seg(5) == "proof":
				c.Ahead = Reply{500, ledger}
			case seg(1) == "blocks" && (sub == "" || sub == "hash" || sub == "txids" || sub == "logs"),
				seg(1) == "deltas" && sub == "":
				c.Ahead = Reply{404, ledger}
			}
			if seg(1) == "deltas" && sub == "" {
				// A plain node serves a delta only from its account-state
				// lookback; a time-travel node re-evaluates one whose block
				// is still stored, so it stays on the block window.
				c.RoundWindow = WindowDelta
				c.Deltas = method == "GET"
			}
		}
		if seg(1) == "blocks" && q.Get("header-only") == "true" {
			// A time-travel node answers a header from its history at any
			// round; a plain node needs the stored block (WindowBlock).
			c.HistoryAnyRound = true
		}
		if seg(1) == "deltas" && seg(2) == "txn" {
			// /v2/deltas/txn/group/{id}: round unknown, any synced node.
			c.Round = nil
		}
	case "accounts", "assets", "applications", "ledger":
		// Of the ledger group only supply is proxied; the rest is admin API.
		if seg(1) == "ledger" && seg(2) != "supply" {
			c.LocalOnly = true
			return c
		}
		r, ok := roundParam()
		switch {
		case seg(1) == "applications" && seg(3) == "boxes":
			// Stock algod takes round on a box listing, within its
			// account-state lookback; a time-travel node serves any round.
			if ok {
				c.Round = &r
				c.RoundWindow = WindowState
				c.HistoryAnyRound = true
			}
		case seg(1) == "assets" && seg(3) == "holders" && len(segs) == 4:
			// dtalgorand's holders index: its own route, GET only, at any
			// round of the history, the latest without one. Other methods
			// have no route anywhere: any node answers 405.
			if method != "GET" {
				break
			}
			c.Holders, c.TimeTravel = true, true
			if ok {
				c.Round = &r
				c.RoundWindow = WindowState
				c.HistoryAnyRound = true
			}
		case seg(1) == "applications" && seg(3) == "box":
			// Stock algod ignores round here: only the history answers it.
			if ok {
				c.Round = &r
				c.RoundWindow = WindowState
				c.HistoryAnyRound = true
				c.TimeTravel = true
			}
		default:
			if ok {
				c.Round = &r
				c.RoundWindow = WindowState
				c.HistoryAnyRound = true
				c.TimeTravel = true
			}
		}
		if seg(1) == "ledger" && !ok {
			// Supply at the latest round is still the local node's to answer.
			c.LocalOnly = true
		}
	case "status":
		if seg(2) == "wait-for-block-after" {
			if r, ok := parseRound(seg(3)); ok {
				c.WaitAfter = &r
			}
		}
	case "teal":
		c.NeedsDev = true
		c.Idempotent = true // compile/dryrun/disassemble are side-effect free
	case "transactions":
		switch {
		case seg(2) == "" && method == "POST":
			c.Broadcast = true
			c.Idempotent = false
		case seg(2) == "pending" && seg(3) != "":
			c.PendingID = seg(3)
		case seg(2) == "pending":
			c.LocalOnly = true // pool dump is only meaningful per node
		case seg(2) == "simulate" && seg(3) == "limits":
			// dtalgorand's limits: a GET, answered for the fleet by the router.
			c.SimulateLimits = method == "GET"
		case seg(2) == "simulate":
			c.Idempotent = true
			c.Simulate = true
		case seg(2) == "params":
			// suggested params: any synced node
		}
	case "query":
		// dtalgorand's read-only TEAL calls: POST only, with no subpath.
		// Any other request has no route on any node.
		if method == "POST" && seg(2) == "" {
			c.Query = true
			c.Idempotent = true
		}
	case "participation", "shutdown", "catchup", "devmode":
		c.LocalOnly = true // admin API is never proxied to other nodes
	}
	return c
}

// versionsHigh returns the last round a versions page reads: max-round, or
// the largest of rounds, which dtalgorand takes as one comma-separated
// value. A node that has not reached it answers 400. ok is false when the
// page names no round past 0, and when every node rejects the query anyway:
// a malformed or repeated value, or rounds with min-round or max-round.
func versionsHigh(q url.Values) (uint64, bool) {
	maxs, rounds := q["max-round"], q["rounds"]
	var list []string
	switch {
	case rounds == nil && len(maxs) == 1:
		list = maxs
	case len(rounds) == 1 && maxs == nil && q["min-round"] == nil:
		list = strings.Split(rounds[0], ",")
	default:
		return 0, false
	}
	var hi uint64
	for _, s := range list {
		r, ok := parseRound(s)
		if !ok {
			return 0, false
		}
		hi = max(hi, r)
	}
	return hi, hi > 0
}

// queryValues parses a raw query string. url.ParseQuery returns every valid
// pair alongside the first error, so a malformed pair must not discard a
// well-formed round (which would let the request reach a plain node and be
// answered with the latest state).
func queryValues(rawQuery string) url.Values {
	if rawQuery == "" {
		return nil
	}
	v, _ := url.ParseQuery(rawQuery)
	return v
}

func parseRound(s string) (uint64, bool) {
	if s == "" {
		return 0, false
	}
	r, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return r, true
}
