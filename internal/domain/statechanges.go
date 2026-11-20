package domain

import (
	"fmt"
	"net/url"
	"strings"
)

// StateChangesInclude is a set of the values the include parameter of
// dtalgorand's GET /v2/deltas/{round}/txn/state-changes takes: bit i is
// stateChangesIncludes[i]. A node lists the ones it takes in its
// capabilities, and the router sends a request only to a node that takes
// every one it asks for.
type StateChangesInclude uint8

// The include values the router routes by.
const (
	IncludeAccounts StateChangesInclude = 1 << iota
	IncludeAssets
	IncludeApps
	IncludeBudget
	IncludeReads
	IncludeTrace
)

// stateChangesIncludes are the include values, in dtalgorand's order. New
// ones go at the end, so that a bit keeps its meaning across heartbeats.
var stateChangesIncludes = []string{"accounts", "assets", "apps", "budget", "reads", "trace"}

// ParseStateChangesInclude reads include values: the capabilities
// response's list, or a request's. Names this mesh does not know are left
// out: it could not route by them, and a node that does not take one
// answers the request with its own 400.
func ParseStateChangesInclude(names []string) StateChangesInclude {
	var s StateChangesInclude
	for _, n := range names {
		for i, v := range stateChangesIncludes {
			if v == n {
				s |= 1 << i
			}
		}
	}
	return s
}

// stateChangesIncludeParam reads a request's include parameter, which
// dtalgorand takes as one comma-separated value.
func stateChangesIncludeParam(q url.Values) StateChangesInclude {
	var names []string
	for _, v := range q["include"] {
		names = append(names, strings.Split(v, ",")...)
	}
	return ParseStateChangesInclude(names)
}

// Names lists the set's values in dtalgorand's order; nil when empty.
func (s StateChangesInclude) Names() []string {
	var out []string
	for i, v := range stateChangesIncludes {
		if s&(1<<i) != 0 {
			out = append(out, v)
		}
	}
	return out
}

// Has reports whether s holds every value of o.
func (s StateChangesInclude) Has(o StateChangesInclude) bool { return s&o == o }

// Len is the number of values in s.
func (s StateChangesInclude) Len() int {
	n := 0
	for ; s != 0; s &= s - 1 {
		n++
	}
	return n
}

// UnmarshalYAML reads the values of a config override by name, as the
// capabilities response lists them; an unknown name is an error.
func (s *StateChangesInclude) UnmarshalYAML(unmarshal func(any) error) error {
	var names []string
	if err := unmarshal(&names); err != nil {
		return err
	}
	for _, n := range names {
		if ParseStateChangesInclude([]string{n}) == 0 {
			return fmt.Errorf("unknown state-changes include %q", n)
		}
	}
	*s = ParseStateChangesInclude(names)
	return nil
}

// MarshalYAML writes the values by name.
func (s StateChangesInclude) MarshalYAML() (any, error) { return s.Names(), nil }

// StateChangesLimits is what a dtalgorand node serves of GET
// /v2/deltas/{round}/txn/state-changes, the capabilities response's
// state-changes object. The zero value is a node without the route: a stock
// algod, a dtalgorand without time travel, or one from before it.
type StateChangesLimits struct {
	// Served is a node that reports the object, and so serves the route.
	Served bool `json:"served" yaml:"served"`
	// MaxResponseBytes caps one answer; a larger one is a 400. 0: no limit.
	MaxResponseBytes uint64 `json:"max_response_bytes,omitempty" yaml:"max_response_bytes,omitempty"`
	// Include is the include values the node takes. trace is listed only
	// with the developer API.
	Include StateChangesInclude `json:"include,omitempty" yaml:"include,omitempty"`
}

// StateChanges is what the node serves of the state-changes route, the zero
// value for a node without it. It needs the history, so a node without time
// travel has none whatever an override says.
func (c Capabilities) StateChanges() StateChangesLimits {
	if c.Simulate == nil || !c.TimeTravel || !c.Simulate.StateChanges.Served {
		return StateChangesLimits{}
	}
	return c.Simulate.StateChanges
}

// stateChangesAbove orders nodes for the state-changes object of
// FleetCapabilities: the most include values, then the largest answer
// (0, no limit, the largest), then the id.
func stateChangesAbove(a, b Upstream) bool {
	la, lb := a.Caps.StateChanges(), b.Caps.StateChanges()
	switch {
	case la.Include.Len() != lb.Include.Len():
		return la.Include.Len() > lb.Include.Len()
	case la.MaxResponseBytes != lb.MaxResponseBytes:
		return la.MaxResponseBytes == 0 || lb.MaxResponseBytes != 0 && la.MaxResponseBytes > lb.MaxResponseBytes
	}
	return a.ID < b.ID
}

// stateChangesNotInHistory is dtalgorand's message for state changes at a
// round its history does not hold: one past its latest, or any while its
// history is untrusted (errFailedRetrievingStateDelta around the ledger's
// error).
func stateChangesNotInHistory(round uint64) string {
	return fmt.Sprintf("failed retrieving State Delta: round %d is not in the history", round)
}

// StateChangesNotInHistory reports whether msg is a node's answer to state
// changes at round: the history does not hold it.
func StateChangesNotInHistory(msg string, round uint64) bool {
	return strings.Contains(msg, fmt.Sprintf("round %d is not in the history", round))
}
