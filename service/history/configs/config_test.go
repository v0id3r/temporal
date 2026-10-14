package configs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
)

func TestRangeCompleteTasksTimeout_DefaultAndOverride(t *testing.T) {
	client := dynamicconfig.NewMemoryClient()
	dc := dynamicconfig.NewCollection(client, log.NewNoopLogger())
	cfg := NewConfig(dc, 1)

	// default is identical to the regular queue IO timeout
	require.Equal(t, 5*time.Second, cfg.RangeCompleteTasksTimeout())

	// the same Config instance observes a runtime change of the setting
	cleanup := client.OverrideSetting(dynamicconfig.HistoryRangeCompleteTasksTimeout, 30*time.Minute)
	require.Equal(t, 30*time.Minute, cfg.RangeCompleteTasksTimeout())
	cleanup()
	require.Equal(t, 5*time.Second, cfg.RangeCompleteTasksTimeout())
}
