package schema

import (
	"strings"
	"testing"
)

// The omni marker must stay narrow. An earlier version broadened it to omni_%,
// which matched managed AlloyDB's unrelated omni_enable_post_startup_helper GUC
// and misclassified managed as Omni. This guards against reintroducing that.
func TestOmniMarkerExprStaysNarrow(t *testing.T) {
	if !strings.Contains(omniMarkerExpr, `'alloydb\_omni%'`) {
		t.Errorf("omni marker must match the alloydb_omni namespace, got: %s", omniMarkerExpr)
	}
	if strings.Contains(omniMarkerExpr, `'omni\_%'`) || strings.Contains(omniMarkerExpr, `'omni_%'`) {
		t.Errorf("omni marker must not match the broad omni_%% namespace (matches managed's omni_enable_post_startup_helper): %s", omniMarkerExpr)
	}
}
