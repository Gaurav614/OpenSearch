/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.store.ShardAttributes;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Implementation of AsyncShardFetch with batching support. This class is responsible for executing the fetch
 * part using the base class {@link AsyncShardFetch}. Other functionalities needed for a batch are only written here.
 * Cleanup of failed shards is necessary in a batch and based on that a reroute should be triggered to take care of
 * those in the next run. This separation also takes care of the extra generic type V which is only needed for batch
 * transport actions like {@link TransportNodesListGatewayStartedShardsBatch}.
 *
 * @param <T> Response type of the transport action.
 * @param <V> Data type of shard level response.
 */
public abstract class AsyncShardBatchFetch<T extends BaseNodeResponse, V extends BaseShardResponse> extends AsyncShardFetch<T> {

    @SuppressWarnings("unchecked")
    AsyncShardBatchFetch(
        Logger logger,
        String type,
        Map<ShardId, ShardAttributes> shardAttributesMap,
        AsyncShardFetch.Lister<? extends BaseNodesResponse<T>, T> action,
        String batchId,
        Class<V> clazz,
        BiFunction<DiscoveryNode, Map<ShardId, V>, T> responseGetter,
        Function<T, Map<ShardId, V>> shardsBatchDataGetter,
        Supplier<V> emptyResponseBuilder,
        Consumer<ShardId> handleFailedShard
    ) {
        super(logger, type, shardAttributesMap, action, batchId);
        this.shardCache = new ShardBatchCache<>(
            logger,
            type,
            shardAttributesMap,
            "BatchID=[" + batchId + "]",
            clazz,
            responseGetter,
            shardsBatchDataGetter,
            emptyResponseBuilder,
            handleFailedShard
        );
    }

    public synchronized FetchResult<T> fetchData(DiscoveryNodes nodes, Map<ShardId, Set<String>> ignoreNodes) {
        List<ShardId> failedShards = cleanUpFailedShards();
        if (failedShards.isEmpty() == false) {
            // trigger a reroute if there are any shards failed, to make sure they're picked up in next run
            logger.trace("triggering another reroute for failed shards in {}", reroutingKey);
            reroute("shards-failed", "shards failed in " + reroutingKey);
        }
        return super.fetchData(nodes, ignoreNodes);
    }

    private List<ShardId> cleanUpFailedShards() {
        List<ShardId> failedShards = shardCache.getFailedShards();
        if (failedShards != null && failedShards.isEmpty() == false) {
            shardAttributesMap.keySet().removeIf(failedShards::contains);
        }
        return failedShards;
    }

    /**
     * Remove a shard from the cache maintaining a full batch of shards. This is needed to clear the shard once it's
     * assigned or failed.
     *
     * @param shardId shardId to be removed from the batch.
     */
    public void clearShard(ShardId shardId) {
        this.shardAttributesMap.remove(shardId);
        this.shardCache.clearShardCache(shardId);
    }
}
