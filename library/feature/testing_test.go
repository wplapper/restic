package feature_test

import (
	"testing"

	"github.com/wplapper/restic/library/feature"
	rtest "github.com/wplapper/restic/library/test"
)

func TestSetFeatureFlag(t *testing.T) {
	flags := buildTestFlagSet()
	rtest.Assert(t, !flags.Enabled(alpha), "expected alpha feature to be disabled")

	restore := feature.TestSetFlag(t, flags, alpha, true)
	rtest.Assert(t, flags.Enabled(alpha), "expected alpha feature to be enabled")

	restore()
	rtest.Assert(t, !flags.Enabled(alpha), "expected alpha feature to be disabled again")
}
