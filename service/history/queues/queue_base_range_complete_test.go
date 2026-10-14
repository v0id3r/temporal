package queues

import (
	"context"
	"sync/atomic"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

// deadlineOf returns the context deadline that rangeCompleteTasks handed to
// persistence, measured from the moment the call was made.
func (s *queueBaseSuite) rangeCompleteDeadline(base *queueBase, mockShard *shard.ContextTest) time.Duration {
	var got time.Duration
	mockShard.Resource.ExecutionMgr.EXPECT().RangeCompleteHistoryTasks(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, _ *persistence.RangeCompleteHistoryTasksRequest) error {
			deadline, ok := ctx.Deadline()
			s.True(ok, "range delete context must carry a deadline")
			got = time.Until(deadline)
			return nil
		},
	).Times(1)
	s.NoError(base.rangeCompleteTasks(tasks.MinimumKey, tasks.NewKey(time.Now(), 100)))
	return got
}

func (s *queueBaseSuite) newRangeCompleteTestBase() (*queueBase, *shard.ContextTest) {
	mockShard := shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{ShardId: 0, RangeId: 10},
		s.config,
	)
	mockShard.Resource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	base := s.newQueueBase(mockShard, tasks.CategoryTimer, nil)
	return base, mockShard
}

func (s *queueBaseSuite) TestRangeCompleteTasks_DefaultTimeoutIsQueueIOTimeout() {
	s.options.RangeCompleteTasksTimeout = nil
	base, mockShard := s.newRangeCompleteTestBase()

	d := s.rangeCompleteDeadline(base, mockShard)
	s.InDelta(float64(queueIOTimeout), float64(d), float64(time.Second))
}

func (s *queueBaseSuite) TestRangeCompleteTasks_ZeroFallsBackToQueueIOTimeout() {
	s.options.RangeCompleteTasksTimeout = func() time.Duration { return 0 }
	base, mockShard := s.newRangeCompleteTestBase()

	d := s.rangeCompleteDeadline(base, mockShard)
	s.InDelta(float64(queueIOTimeout), float64(d), float64(time.Second))
}

func (s *queueBaseSuite) TestRangeCompleteTasks_TimeoutFollowsDynamicConfigAtRuntime() {
	var configured atomic.Int64
	configured.Store(int64(30 * time.Minute))
	s.options.RangeCompleteTasksTimeout = func() time.Duration { return time.Duration(configured.Load()) }
	base, mockShard := s.newRangeCompleteTestBase()

	d := s.rangeCompleteDeadline(base, mockShard)
	s.InDelta(float64(30*time.Minute), float64(d), float64(time.Second))

	// change the value without recreating the queue: the next delete must see it
	configured.Store(int64(10 * time.Minute))
	d = s.rangeCompleteDeadline(base, mockShard)
	s.InDelta(float64(10*time.Minute), float64(d), float64(time.Second))
}

func (s *queueBaseSuite) TestRangeCompleteTasks_TimeoutIsEnforced() {
	s.options.RangeCompleteTasksTimeout = func() time.Duration { return 50 * time.Millisecond }
	base, mockShard := s.newRangeCompleteTestBase()

	mockShard.Resource.ExecutionMgr.EXPECT().RangeCompleteHistoryTasks(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, _ *persistence.RangeCompleteHistoryTasksRequest) error {
			// simulate a DELETE that never finishes on its own
			<-ctx.Done()
			return ctx.Err()
		},
	).Times(1)

	start := time.Now()
	err := base.rangeCompleteTasks(tasks.MinimumKey, tasks.NewKey(time.Now(), 100))
	s.ErrorIs(err, context.DeadlineExceeded)
	s.Less(time.Since(start), 5*time.Second, "must give up at the configured timeout, not the default")
}
