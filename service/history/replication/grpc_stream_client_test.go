package replication

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

type recordingReplicationStreamReceiverRateLimiter struct {
	burst     int
	waits     []int
	waitError error
}

func (l *recordingReplicationStreamReceiverRateLimiter) Burst() int {
	return l.burst
}

func (l *recordingReplicationStreamReceiverRateLimiter) WaitN(_ context.Context, tokens int) error {
	l.waits = append(l.waits, tokens)
	return l.waitError
}

func TestRateLimitedReplicationStreamClientRecv(t *testing.T) {
	controller := gomock.NewController(t)
	streamClient := adminservicemock.NewMockAdminService_StreamWorkflowReplicationMessagesClient(controller)
	response := &adminservice.StreamWorkflowReplicationMessagesResponse{
		Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
			Messages: &replicationspb.WorkflowReplicationMessages{ExclusiveHighWatermark: 123},
		},
	}
	streamClient.EXPECT().Recv().Return(response, nil)
	rateLimiter := &recordingReplicationStreamReceiverRateLimiter{burst: replicationStreamReceiverBurstBytes}
	client := &rateLimitedReplicationStreamClient{
		BiDirectionStreamClient: streamClient,
		ctx:                     context.Background(),
		rateLimiter:             rateLimiter,
	}

	actual, err := client.Recv()

	require.NoError(t, err)
	require.Same(t, response, actual)
	require.Equal(t, []int{proto.Size(response)}, rateLimiter.waits)
}

func TestWaitForReplicationStreamReceiverRateLimitChunksLargeMessages(t *testing.T) {
	rateLimiter := &recordingReplicationStreamReceiverRateLimiter{burst: 100}

	err := waitForReplicationStreamReceiverRateLimit(context.Background(), rateLimiter, 250)

	require.NoError(t, err)
	require.Equal(t, []int{100, 100, 50}, rateLimiter.waits)
}

func TestWaitForReplicationStreamReceiverRateLimitReturnsError(t *testing.T) {
	waitError := errors.New("rate limiter failed")
	rateLimiter := &recordingReplicationStreamReceiverRateLimiter{
		burst:     100,
		waitError: waitError,
	}

	err := waitForReplicationStreamReceiverRateLimit(context.Background(), rateLimiter, 250)

	require.ErrorIs(t, err, waitError)
	require.Equal(t, []int{100}, rateLimiter.waits)
}
