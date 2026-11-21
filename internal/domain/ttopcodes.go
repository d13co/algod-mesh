package domain

import (
	"fmt"
	"mime"
	"strconv"
	"strings"
)

// TTOpcodes is a set of dtalgorand's time-travel opcodes, which a node runs
// only in POST /v2/query programs and lists in its capabilities: bit i is
// ttOpcodes[i]. The router does not route by them, as every node with
// queries is taken to have them; it reports them in the fleet's
// capabilities.
type TTOpcodes uint32

// ttOpcodes are the time-travel opcodes, in dtalgorand's order: the seek,
// the change lookups of whole objects, and the searches of a state key or
// box name prefix. New ones go at the end, so that a bit keeps its meaning
// across heartbeats.
var ttOpcodes = []string{
	"tt_seek",
	"tt_acct_change",
	"tt_holding_change",
	"tt_local_change",
	"tt_asset_change",
	"tt_app_change",
	"tt_box_change",
	"tt_global_key_change",
	"tt_local_key_change",
	"tt_box_prefix_change",
}

// ParseTTOpcodes reads the capabilities response's opcodes list. Names this
// mesh does not know are left out: it could not route a program by them.
func ParseTTOpcodes(names []string) TTOpcodes {
	var s TTOpcodes
	for _, n := range names {
		for i, op := range ttOpcodes {
			if op == n {
				s |= 1 << i
			}
		}
	}
	return s
}

// Names lists the set's opcodes in dtalgorand's order; nil when empty.
func (s TTOpcodes) Names() []string {
	var out []string
	for i, op := range ttOpcodes {
		if s&(1<<i) != 0 {
			out = append(out, op)
		}
	}
	return out
}

// UnmarshalYAML reads the opcodes of a config override by name, as the
// capabilities response lists them; an unknown name is an error.
func (s *TTOpcodes) UnmarshalYAML(unmarshal func(any) error) error {
	var names []string
	if err := unmarshal(&names); err != nil {
		return err
	}
	for _, n := range names {
		if ParseTTOpcodes([]string{n}) == 0 {
			return fmt.Errorf("unknown time-travel opcode %q", n)
		}
	}
	*s = ParseTTOpcodes(names)
	return nil
}

// MarshalYAML writes the opcodes by name.
func (s TTOpcodes) MarshalYAML() (any, error) { return s.Names(), nil }

// Len is the number of opcodes in s.
func (s TTOpcodes) Len() int {
	n := 0
	for ; s != 0; s &= s - 1 {
		n++
	}
	return n
}

// EventStreamType is the media type of a streamed POST /v2/query answer.
const EventStreamType = "text/event-stream"

// AcceptsEventStream reports whether a request's Accept values take
// text/event-stream with a q above 0, as dtalgorand reads them to stream a
// query's answer (wantsQueryStream).
func AcceptsEventStream(accept []string) bool {
	for _, v := range accept {
		for _, part := range strings.Split(v, ",") {
			mt, params, err := mime.ParseMediaType(part)
			if err != nil || mt != EventStreamType {
				continue
			}
			if q, ok := params["q"]; ok {
				if f, err := strconv.ParseFloat(q, 64); err != nil || f <= 0 {
					continue
				}
			}
			return true
		}
	}
	return false
}

// IsEventStream reports whether a response's Content-Type is a stream of
// Server-Sent Events.
func IsEventStream(contentType string) bool {
	mt, _, err := mime.ParseMediaType(contentType)
	return err == nil && mt == EventStreamType
}
