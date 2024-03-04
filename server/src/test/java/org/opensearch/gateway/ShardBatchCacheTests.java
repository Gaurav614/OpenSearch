/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.store.ShardAttributes;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class ShardBatchCacheTests extends OpenSearchTestCase {
    private final ShardId shardId = new ShardId("test", "_na_", 0);

    private Map<ShardId, ShardsBatchGatewayAllocator.ShardEntry> batchInfo = new HashMap<>();
    private ShardBatchCache<
        TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShardsBatch,
        TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShard> shardCache;

    public void setupShardBatchCache(String batchId) {
        Map<ShardId, ShardAttributes> shardAttributesMap = new HashMap<>();
        fillShards(shardAttributesMap);
        this.shardCache = new ShardBatchCache<>(
            logger,
            "batch_shards_started",
            shardAttributesMap,
            "BatchID=[" + batchId + "]",
            TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShard.class,
            TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShardsBatch::new,
            TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShardsBatch::getNodeGatewayStartedShardsBatch,
            () -> new TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShard(null, false, null, null),
            this::removeShard
        );
    }

    public void removeShard(ShardId shardId) {
        batchInfo.remove(shardId);
    }

    private void fillShards(Map<ShardId, ShardAttributes> shardAttributesMap) {
        BatchTestUtil.setUpShards(10);
        // ToDo
    }

}
