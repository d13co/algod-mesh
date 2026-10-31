package app

import (
	"net/http"
	"regexp"
	"testing"

	"github.com/d13co/algod-mesh/internal/domain"
)

// go-dtalgorand-sdk tells errors apart by status and message
// (client/v2/algod/dt_errors.go, dt_holders.go, dt_versions.go): these are
// its patterns. The answers the mesh writes itself must match the ones the
// node's own answer would, or a client behind the mesh misreads them.
var (
	sdkHistory     = regexp.MustCompile(`box index is incomplete|asset holders index is not complete|history is not served`)
	sdkPastRound   = regexp.MustCompile(`requires? EnableTimeTravel|given round is no longer available`)
	sdkHoldersOff  = regexp.MustCompile(`asset holders require EnableTimeTravel and TimeTravelHoldersIndex`)
	sdkAssetAbsent = regexp.MustCompile(`asset does not exist`)
)

func TestMeshAnswersMatchSDK(t *testing.T) {
	for _, tc := range []struct {
		name, msg string
		re        *regexp.Regexp
	}{
		{"holders index missing everywhere (IsHistoryUnavailable)", holdersMissingMessage, sdkHistory},
		{"holders index missing on the nodes tried (IsHistoryUnavailable)", holdersMissingTriedMessage, sdkHistory},
		{"no time-travel node (DTUnsupportedError)", noTimeTravelMessage, sdkPastRound},
		{"no holders index node (DTUnsupportedError)", noHoldersIndexMessage, sdkHoldersOff},
	} {
		if !tc.re.MatchString(tc.msg) {
			t.Errorf("%s: %q does not match %s", tc.name, tc.msg, tc.re)
		}
	}
	// A versions 404 is unsupported on any message; a holders 404 only when
	// it doesn't say the asset is missing.
	if sdkAssetAbsent.MatchString(noVersionsMessage) {
		t.Errorf("versions 404 %q reads as a missing asset", noVersionsMessage)
	}
}

// A versions page in a fleet without time travel gets the 404 of a node
// without the routes.
func TestVersionsWithoutHistoryNode(t *testing.T) {
	f := newSimFleet(t, domain.Capabilities{})
	rec := f.do(http.MethodGet, "/v2/accounts/ADDR/versions", nil)
	if rec.Code != 404 || !sdkPastRound.MatchString(rec.Body.String()) {
		t.Fatalf("versions without history node: %d %s", rec.Code, rec.Body.String())
	}
}
