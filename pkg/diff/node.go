package diff

import (
	"fmt"
	"time"

	"github.com/boringsql/dryrun/pkg/snapshot"
)

// two addresses prove two machines: refuse; a moved boot alone is a caveat
// (pg_stat_* and pgss survive a clean restart)
func serverChanged(from, to snapshot.NodeIdentity) (why string, refuse bool) {
	// both identity fields are optional: the strong signal must not sit behind the weak one
	fa, fb := from.ServerAddr, to.ServerAddr
	if fa != "" && fb != "" && fa != fb {
		return fmt.Sprintf("label %q covered two servers (%s and %s): their counters are unrelated",
			to.Source, fa, fb), true
	}
	if !bootMoved(from, to) {
		return "", false
	}
	return fmt.Sprintf("the server behind %q restarted or was replaced between the captures (started %s, then %s); "+
		"if it was replaced these deltas are not increments",
		to.Source, from.PostmasterStartTime.UTC().Format(time.RFC3339), to.PostmasterStartTime.UTC().Format(time.RFC3339)), false
}

// unknown on either side is not a move
func bootMoved(from, to snapshot.NodeIdentity) bool {
	a, b := from.PostmasterStartTime, to.PostmasterStartTime
	return a != nil && b != nil && !a.Equal(*b)
}
