/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.Nullable;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.store.ShardAttributes;
import org.opensearch.transport.ReceiveTimeoutTransportException;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Cache implementation of transport actions returning batch of shards related data in the response. It'll
 *
 * @param <T> Response type of transport action.
 * @param <V> Data type of shard level response.
 */
public class ShardBatchCache<T extends BaseNodeResponse, V extends BaseShardResponse> extends BaseShardCache<T> {
    private final Map<String, NodeEntry<V>> cache = new HashMap<>();
    private final Map<ShardId, Integer> shardIdKey = new HashMap<>();
    private final AtomicInteger shardIdIndex = new AtomicInteger();
    private final int batchSize;
    private final Class<V> shardResponseClass;
    private final BiFunction<DiscoveryNode, Map<ShardId, V>, T> responseConstructor;
    private final Map<Integer, ShardId> shardIdReverseKey = new HashMap<>();
    private final Function<T, Map<ShardId, V>> shardsBatchDataGetter;
    private final Supplier<V> emptyResponseBuilder;
    private final Set<ShardId> failedShards;
    private final Consumer<ShardId> handleFailedShard;

    public ShardBatchCache(
        Logger logger,
        String type,
        Map<ShardId, ShardAttributes> shardToCustomDataPath,
        String logKey,
        Class<V> clazz,
        BiFunction<DiscoveryNode, Map<ShardId, V>, T> responseGetter,
        Function<T, Map<ShardId, V>> shardsBatchDataGetter,
        Supplier<V> emptyResponseBuilder,
        Consumer<ShardId> handleFailedShard
    ) {
        super(logger, logKey, type);
        this.batchSize = shardToCustomDataPath.size();
        fillShardIdKeys(shardToCustomDataPath.keySet());
        this.shardResponseClass = clazz;
        this.responseConstructor = responseGetter;
        this.shardsBatchDataGetter = shardsBatchDataGetter;
        this.emptyResponseBuilder = emptyResponseBuilder;
        failedShards = new HashSet<>();
        this.handleFailedShard = handleFailedShard;
    }

    @Override
    public Map<String, ? extends BaseNodeEntry> getCache() {
        return cache;
    }

    @Override
    public void deleteData(ShardId shardId) {
        if (shardIdKey.containsKey(shardId)) {
            Integer shardIdIndex = shardIdKey.remove(shardId);
            for (String nodeId : cache.keySet()) {
                cache.get(nodeId).clearShard(shardIdIndex);
            }
        }
    }

    @Override
    public Map<DiscoveryNode, T> getCacheData(DiscoveryNodes nodes, Set<String> failedNodes) {
        refreshReverseIdMap();
        return super.getCacheData(nodes, failedNodes);
    }

    /**
     * Build a reverse map to get shardId from the array index, this will be used to construct the response which
     * PrimaryShardBatchAllocator or ReplicaShardBatchAllocator are looking for.
     */
    private void refreshReverseIdMap() {
        shardIdReverseKey.clear();
        for (ShardId shardId : shardIdKey.keySet()) {
            shardIdReverseKey.putIfAbsent(shardIdKey.get(shardId), shardId);
        }
    }

    @Override
    public void initData(DiscoveryNode node) {
        cache.put(node.getId(), new NodeEntry<>(node.getId(), shardResponseClass, batchSize));
    }

    /**
     * Put the response received from data nodes into the cache.
     * Get shard level data from batch, then filter out if any shards received failures.
     * After that complete storing the data at node level and mark fetching as done.
     * @param node     node from which we got the response.
     * @param response shard metadata coming from node.
     */
    @Override
    public void putData(DiscoveryNode node, T response) {
        NodeEntry<V> nodeEntry = cache.get(node.getId());
        Map<ShardId, V> batchResponse = shardsBatchDataGetter.apply(response);
        failedShards.addAll(filterFailedShards(batchResponse));
        nodeEntry.doneFetching(batchResponse, shardIdKey);
    }

    /**
     * Return the shard for which we got unhandled exceptions.
     *
     * @param batchResponse response from one node for the batch.
     * @return List of failed shards.
     */
    private List<ShardId> filterFailedShards(Map<ShardId, V> batchResponse) {
        logger.trace("filtering failed shards");
        List<ShardId> failedShards = new ArrayList<>();
        for (Iterator<ShardId> it = batchResponse.keySet().iterator(); it.hasNext();) {
            ShardId shardId = it.next();
            if (batchResponse.get(shardId) != null) {
                if (batchResponse.get(shardId).getException() != null) {
                    // handle per shard level exceptions, process other shards, only throw out this shard from
                    // the batch
                    Exception shardException = batchResponse.get(shardId).getException();
                    // if the request got rejected or timed out, we need to try it again next time...
                    if (shardException instanceof OpenSearchRejectedExecutionException
                        || shardException instanceof ReceiveTimeoutTransportException
                        || shardException instanceof OpenSearchTimeoutException) {
                        logger.trace("got unhandled retryable exception for shard {} {}", shardId.toString(), shardException.toString());
                        failedShards.add(shardId);
                        handleFailedShard.accept(shardId);
                        // remove this failed entry. So, while storing the data, we don't need to re-process it.
                        it.remove();
                    }
                }
            }
        }
        return failedShards;
    }

    @Override
    public T getData(DiscoveryNode node) {
        return this.responseConstructor.apply(node, getBatchData(cache.get(node.getId())));
    }

    private HashMap<ShardId, V> getBatchData(NodeEntry<V> nodeEntry) {
        V[] nodeShardEntries = nodeEntry.getData();
        boolean[] emptyResponses = nodeEntry.getEmptyShardResponse();
        HashMap<ShardId, V> shardData = new HashMap<>();
        for (Integer shardIdIndex : shardIdKey.values()) {
            if (emptyResponses[shardIdIndex]) {
                shardData.put(shardIdReverseKey.get(shardIdIndex), emptyResponseBuilder.get());
            } else if (nodeShardEntries[shardIdIndex] != null) {
                // ignore null responses here
                shardData.put(shardIdReverseKey.get(shardIdIndex), nodeShardEntries[shardIdIndex]);
            }
        }
        return shardData;
    }

    private void fillShardIdKeys(Set<ShardId> shardIds) {
        for (ShardId shardId : shardIds) {
            this.shardIdKey.putIfAbsent(shardId, shardIdIndex.getAndIncrement());
        }
        this.shardIdKey.keySet().removeIf(shardId -> {
            if (!shardIds.contains(shardId)) {
                deleteData(shardId);
                return true;
            } else {
                return false;
            }
        });
    }

    /**
     * A node entry, holding the state of the fetched data for a specific shard
     * for a giving node.
     */
    static class NodeEntry<V extends BaseShardResponse> extends BaseNodeEntry {
        @Nullable
        private final V[] shardData;
        private final boolean[] emptyShardResponse;

        NodeEntry(String nodeId, Class<V> clazz, int batchSize) {
            super(nodeId);
            this.shardData = (V[]) Array.newInstance(clazz, batchSize);
            this.emptyShardResponse = new boolean[batchSize];
        }

        void doneFetching(Map<ShardId, V> shardDataFromNode, Map<ShardId, Integer> shardIdKey) {
            fillShardData(shardDataFromNode, shardIdKey);
            super.doneFetching();
        }

        void clearShard(Integer shardIdIndex) {
            this.shardData[shardIdIndex] = null;
        }

        V[] getData() {
            return this.shardData;
        }

        boolean[] getEmptyShardResponse() {
            return emptyShardResponse;
        }

        private void fillShardData(Map<ShardId, V> shardDataFromNode, Map<ShardId, Integer> shardIdKey) {
            for (ShardId shardId : shardDataFromNode.keySet()) {
                if (shardDataFromNode.get(shardId) != null) {
                    if (shardDataFromNode.get(shardId).isEmpty()) {
                        this.emptyShardResponse[shardIdKey.get(shardId)] = true;
                        this.shardData[shardIdKey.get(shardId)] = null;
                    } else if (shardDataFromNode.get(shardId).getException() == null) {
                        this.shardData[shardIdKey.get(shardId)] = shardDataFromNode.get(shardId);
                    }
                    // if exception is not null, we got unhandled failure for the shard which needs to be ignored
                }
            }
        }
    }

}
