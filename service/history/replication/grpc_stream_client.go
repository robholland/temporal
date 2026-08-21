package replication

import (
	"context"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/quotas"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

const (
	replicationStreamReceiverBytesPerSecond = 1 << 20
	replicationStreamReceiverBurstBytes     = 64 << 10
)

// This test-only branch deliberately caps aggregate receive bandwidth per history process so
// transport backpressure develops gradually instead of stopping replication completely.
var replicationStreamReceiverLimiter = quotas.NewRateLimiter(
	replicationStreamReceiverBytesPerSecond,
	replicationStreamReceiverBurstBytes,
)

type (
	StreamBiDirectionStreamClientProvider struct {
		clusterMetadata cluster.Metadata
		clientBean      client.Bean
	}
	replicationStreamReceiverRateLimiter interface {
		Burst() int
		WaitN(context.Context, int) error
	}
	rateLimitedReplicationStreamClient struct {
		BiDirectionStreamClient[*adminservice.StreamWorkflowReplicationMessagesRequest, *adminservice.StreamWorkflowReplicationMessagesResponse]
		ctx         context.Context
		rateLimiter replicationStreamReceiverRateLimiter
	}
)

func NewStreamBiDirectionStreamClientProvider(
	clusterMetadata cluster.Metadata,
	clientBean client.Bean,
) *StreamBiDirectionStreamClientProvider {
	return &StreamBiDirectionStreamClientProvider{
		clusterMetadata: clusterMetadata,
		clientBean:      clientBean,
	}
}

func (p *StreamBiDirectionStreamClientProvider) Get(
	ctx context.Context,
	clientShardKey ClusterShardKey,
	serverShardKey ClusterShardKey,
) (BiDirectionStreamClient[*adminservice.StreamWorkflowReplicationMessagesRequest, *adminservice.StreamWorkflowReplicationMessagesResponse], error) {
	allClusterInfo := p.clusterMetadata.GetAllClusterInfo()
	clusterName, _, err := ClusterIDToClusterNameShardCount(allClusterInfo, serverShardKey.ClusterID)
	if err != nil {
		return nil, err
	}
	adminClient, err := p.clientBean.GetRemoteAdminClient(clusterName)
	if err != nil {
		return nil, err
	}
	ctx = metadata.NewOutgoingContext(ctx, history.EncodeClusterShardMD(
		history.ClusterShardID{
			ClusterID: clientShardKey.ClusterID,
			ShardID:   clientShardKey.ShardID,
		},
		history.ClusterShardID{
			ClusterID: serverShardKey.ClusterID,
			ShardID:   serverShardKey.ShardID,
		},
	))
	streamClient, err := adminClient.StreamWorkflowReplicationMessages(ctx)
	if err != nil {
		return nil, err
	}
	return &rateLimitedReplicationStreamClient{
		BiDirectionStreamClient: streamClient,
		ctx:                     ctx,
		rateLimiter:             replicationStreamReceiverLimiter,
	}, nil
}

func (c *rateLimitedReplicationStreamClient) Recv() (*adminservice.StreamWorkflowReplicationMessagesResponse, error) {
	response, err := c.BiDirectionStreamClient.Recv()
	if err != nil {
		return nil, err
	}
	if err := waitForReplicationStreamReceiverRateLimit(c.ctx, c.rateLimiter, proto.Size(response)); err != nil {
		return nil, err
	}
	return response, nil
}

func waitForReplicationStreamReceiverRateLimit(
	ctx context.Context,
	rateLimiter replicationStreamReceiverRateLimiter,
	messageSize int,
) error {
	for messageSize > 0 {
		chunkSize := min(messageSize, rateLimiter.Burst())
		if err := rateLimiter.WaitN(ctx, chunkSize); err != nil {
			return err
		}
		messageSize -= chunkSize
	}
	return nil
}
