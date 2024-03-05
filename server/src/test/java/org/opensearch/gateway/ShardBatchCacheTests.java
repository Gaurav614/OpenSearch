/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.gateway.TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShard;
import org.opensearch.gateway.TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShardsBatch;
import org.opensearch.indices.store.ShardAttributes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShardBatchCacheTests extends OpenSearchAllocationTestCase {
    private static final String BATCH_ID = "b1";
    private final DiscoveryNode node1 = newNode("node1");
    private final DiscoveryNode node2 = newNode("node2");
    private final DiscoveryNode node3 = newNode("node3");
    private Map<ShardId, ShardsBatchGatewayAllocator.ShardEntry> batchInfo = new HashMap<>();
    private ShardBatchCache<NodeGatewayStartedShardsBatch, NodeGatewayStartedShard> shardCache;
    private List<ShardId> shardsInBatch = new ArrayList<>();

    public void setupShardBatchCache(String batchId) {
        Map<ShardId, ShardAttributes> shardAttributesMap = new HashMap<>();
        fillShards(shardAttributesMap);
        this.shardCache = new ShardBatchCache<>(
            logger,
            "batch_shards_started",
            shardAttributesMap,
            "BatchID=[" + batchId + "]",
            NodeGatewayStartedShard.class,
            NodeGatewayStartedShardsBatch::new,
            NodeGatewayStartedShardsBatch::getNodeGatewayStartedShardsBatch,
            () -> new NodeGatewayStartedShard(null, false, null, null),
            this::removeShard
        );
    }

    public void testClearShardCache() {
        setupShardBatchCache(BATCH_ID);
        ShardId shard = shardsInBatch.iterator().next();
        this.shardCache.initData(node1);
        this.shardCache.markAsFetching(List.of(node1.getId()), 1);
        this.shardCache.putData(node1, new NodeGatewayStartedShardsBatch(node1, getEmptyPrimaryResponse(shardsInBatch)));
        assertTrue(
            this.shardCache.getCacheData(DiscoveryNodes.builder().add(node1).build(), null)
                .get(node1)
                .getNodeGatewayStartedShardsBatch()
                .containsKey(shard)
        );
        this.shardCache.clearShardCache(shard);
        assertFalse(
            this.shardCache.getCacheData(DiscoveryNodes.builder().add(node1).build(), null)
                .get(node1)
                .getNodeGatewayStartedShardsBatch()
                .containsKey(shard)
        );
    }

    public void testGetCacheData() {
        setupShardBatchCache(BATCH_ID);
        ShardId shard = shardsInBatch.iterator().next();
        this.shardCache.initData(node1);
        this.shardCache.initData(node2);
        this.shardCache.markAsFetching(List.of(node1.getId(), node2.getId()), 1);
        this.shardCache.putData(node1, new NodeGatewayStartedShardsBatch(node1, getEmptyPrimaryResponse(shardsInBatch)));
        assertTrue(
            this.shardCache.getCacheData(DiscoveryNodes.builder().add(node1).build(), null)
                .get(node1)
                .getNodeGatewayStartedShardsBatch()
                .containsKey(shard)
        );
        assertTrue(
            this.shardCache.getCacheData(DiscoveryNodes.builder().add(node2).build(), null)
                .get(node2)
                .getNodeGatewayStartedShardsBatch()
                .isEmpty()
        );
    }

    public void testInitCacheData() {
        setupShardBatchCache(BATCH_ID);
        this.shardCache.initData(node1);
        this.shardCache.initData(node2);
        assertEquals(2, shardCache.getCache().size());
    }

    public void testPutData() {
        setupShardBatchCache(BATCH_ID);
        ShardId shard = shardsInBatch.iterator().next();
        this.shardCache.initData(node1);
        this.shardCache.initData(node2);
        this.shardCache.markAsFetching(List.of(node1.getId(), node2.getId()), 1);
        this.shardCache.putData(node1, new NodeGatewayStartedShardsBatch(node1, getActualPrimaryResponse(shardsInBatch)));
        this.shardCache.putData(node2, new NodeGatewayStartedShardsBatch(node1, getEmptyPrimaryResponse(shardsInBatch)));

        Map<DiscoveryNode, NodeGatewayStartedShardsBatch> fetchData = shardCache.getCacheData(
            DiscoveryNodes.builder().add(node1).add(node2).build(),
            null
        );
        assertEquals(2, fetchData.size());
        assertEquals(10, fetchData.get(node1).getNodeGatewayStartedShardsBatch().size());
        assertEquals("alloc-1", fetchData.get(node1).getNodeGatewayStartedShardsBatch().get(shard).allocationId());

        assertEquals(10, fetchData.get(node2).getNodeGatewayStartedShardsBatch().size());
        assertTrue(fetchData.get(node2).getNodeGatewayStartedShardsBatch().get(shard).isEmpty());
    }

    public void testFilterFailedShards() {
        // ToDo
    }

    private Map<ShardId, NodeGatewayStartedShard> getEmptyPrimaryResponse(List<ShardId> shards) {
        Map<ShardId, NodeGatewayStartedShard> shardData = new HashMap<>();
        for (ShardId shard : shards) {
            shardData.put(shard, new NodeGatewayStartedShard(null, false, null, null));
        }
        return shardData;
    }

    private Map<ShardId, NodeGatewayStartedShard> getActualPrimaryResponse(List<ShardId> shards) {
        int allocationId = 1;
        Map<ShardId, NodeGatewayStartedShard> shardData = new HashMap<>();
        for (ShardId shard : shards) {
            shardData.put(shard, new NodeGatewayStartedShard("alloc-" + allocationId++, false, null, null));
        }
        return shardData;
    }

    public void removeShard(ShardId shardId) {
        batchInfo.remove(shardId);
    }

    private void fillShards(Map<ShardId, ShardAttributes> shardAttributesMap) {
        shardsInBatch = BatchTestUtil.setUpShards(10);
        for (ShardId shardId : shardsInBatch) {
            ShardAttributes attr = new ShardAttributes("");
            shardAttributesMap.put(shardId, attr);
            batchInfo.put(
                shardId,
                new ShardsBatchGatewayAllocator.ShardEntry(attr, randomShardRouting(shardId.getIndexName(), shardId.id()))
            );
        }
    }

    private ShardRouting randomShardRouting(String index, int shard) {
        ShardRoutingState state = randomFrom(ShardRoutingState.values());
        return TestShardRouting.newShardRouting(
            index,
            shard,
            state == ShardRoutingState.UNASSIGNED ? null : "1",
            state == ShardRoutingState.RELOCATING ? "2" : null,
            state != ShardRoutingState.UNASSIGNED && randomBoolean(),
            state
        );
    }
}
