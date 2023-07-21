/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RerouteService;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.opensearch.cluster.routing.allocation.FailedShard;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.common.Priority;
import org.opensearch.common.UUIDs;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.set.Sets;
import org.opensearch.common.lease.Releasables;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadata;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadataBatch;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Allocator for the gateway
 *
 * @opensearch.internal
 */
public class GatewayAllocator implements ExistingShardsAllocator {

    public static final String ALLOCATOR_NAME = "gateway_allocator";

    private static final Logger logger = LogManager.getLogger(GatewayAllocator.class);
    private static final long MAX_BATCH_SIZE = 2000; // will change it to a dynamic setting later

    private final RerouteService rerouteService;

    private final PrimaryShardAllocator primaryShardAllocator;
    private final ReplicaShardAllocator replicaShardAllocator;

    private final PrimaryShardBatchAllocator primaryBatchShardAllocator;
    private final ReplicaShardAllocator replicaBatchShardAllocator;
    private final TransportNodesListGatewayStartedShardsBatch batchStartedAction;
    private final TransportNodesListShardStoreMetadataBatch batchStoreAction;

    private final ConcurrentMap<
        ShardId,
        AsyncShardFetch<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards>> asyncFetchStarted = ConcurrentCollections
        .newConcurrentMap();
    private final ConcurrentMap<ShardId, AsyncShardFetch<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata>> asyncFetchStore =
        ConcurrentCollections.newConcurrentMap();
    private Set<String> lastSeenEphemeralIds = Collections.emptySet();

    private final ConcurrentMap<ShardsBatch, AsyncBatchShardFetch<TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShardsBatch>> asyncBatchFetchStarted = ConcurrentCollections.newConcurrentMap();
    private final ConcurrentMap<ShardsBatch, AsyncBatchShardFetch<TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadataBatch>> asyncBatchFetchStore = ConcurrentCollections.newConcurrentMap();

    @Inject
    public GatewayAllocator(
        RerouteService rerouteService,
        TransportNodesListGatewayStartedShards startedAction,
        TransportNodesListShardStoreMetadata storeAction,
        TransportNodesListGatewayStartedShardsBatch batchStartedAction,
        TransportNodesListShardStoreMetadataBatch batchStoreAction
    ) {
        this.rerouteService = rerouteService;
        this.primaryShardAllocator = new InternalPrimaryShardAllocator(startedAction);
        this.replicaShardAllocator = new InternalReplicaShardAllocator(storeAction);
        this.batchStartedAction = batchStartedAction;
        this.primaryBatchShardAllocator = new InternalPrimaryBatchShardAllocator(batchStartedAction);
        this.batchStoreAction = batchStoreAction;
        this.replicaBatchShardAllocator = new InternalReplicaBatchShardAllocator(batchStoreAction);
    }

    @Override
    public void cleanCaches() {
        Releasables.close(asyncFetchStarted.values());
        asyncFetchStarted.clear();
        Releasables.close(asyncFetchStore.values());
        asyncFetchStore.clear();
    }

    // for tests
    protected GatewayAllocator() {
        this.rerouteService = null;
        this.primaryShardAllocator = null;
        this.replicaShardAllocator = null;
        this.batchStartedAction = null;
        this.primaryBatchShardAllocator = null;
        this.batchStoreAction = null;
        this.replicaBatchShardAllocator = null;
    }

    @Override
    public int getNumberOfInFlightFetches() {
        int count = 0;
        for (AsyncShardFetch<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetch : asyncFetchStarted.values()) {
            count += fetch.getNumberOfInFlightFetches();
        }
        for (AsyncShardFetch<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> fetch : asyncFetchStore.values()) {
            count += fetch.getNumberOfInFlightFetches();
        }
        return count;
    }

    @Override
    public void applyStartedShards(final List<ShardRouting> startedShards, final RoutingAllocation allocation) {
        for (ShardRouting startedShard : startedShards) {
            Releasables.close(asyncFetchStarted.remove(startedShard.shardId()));
            Releasables.close(asyncFetchStore.remove(startedShard.shardId()));
        }

        // ToDo: add new map clearance logic here
    }

    @Override
    public void applyFailedShards(final List<FailedShard> failedShards, final RoutingAllocation allocation) {
        for (FailedShard failedShard : failedShards) {
            Releasables.close(asyncFetchStarted.remove(failedShard.getRoutingEntry().shardId()));
            Releasables.close(asyncFetchStore.remove(failedShard.getRoutingEntry().shardId()));
        }
    }

    @Override
    public void beforeAllocation(final RoutingAllocation allocation) {
        assert primaryShardAllocator != null;
        assert replicaShardAllocator != null;
        ensureAsyncFetchStorePrimaryRecency(allocation);
    }

    @Override
    public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {
        assert replicaShardAllocator != null;
        if (allocation.routingNodes().hasInactiveShards()) {
            // cancel existing recoveries if we have a better match
            replicaShardAllocator.processExistingRecoveries(allocation);
        }
    }

    @Override
    public void allocateUnassigned(
        ShardRouting shardRouting,
        final RoutingAllocation allocation,
        UnassignedAllocationHandler unassignedAllocationHandler
    ) {
        assert primaryShardAllocator != null;
        assert replicaShardAllocator != null;
        innerAllocatedUnassigned(allocation, primaryShardAllocator, replicaShardAllocator, shardRouting, unassignedAllocationHandler);
    }

    @Override
    public void allocateUnassignedBatch(final RoutingAllocation allocation, boolean primary) {
        // create batches for unassigned shards
        createBatches(allocation, primary);

        assert primaryBatchShardAllocator != null;
        assert replicaBatchShardAllocator != null;
        if (primary) {
            asyncBatchFetchStarted.keySet().forEach(batch -> primaryBatchShardAllocator.allocateUnassignedBatch(batch.getBatchId(), batch.getBatchedShards(), allocation));
        } else {
            asyncBatchFetchStore.keySet().forEach(batch -> replicaBatchShardAllocator.allocateUnassignedBatch(batch.getBatchId(), batch.getBatchedShards(), allocation));
        }
    }

    private void createBatches(RoutingAllocation allocation, boolean primary) {
        RoutingNodes.UnassignedShards unassigned = allocation.routingNodes().unassigned();
        // fetch all current batched shards
        Set<ShardRouting> currentBatchedShards;
        if (primary) {
            currentBatchedShards = asyncBatchFetchStarted.keySet().stream().flatMap(shardsBatch -> shardsBatch.getBatchedShards().stream()).collect(Collectors.toSet());
        } else {
            currentBatchedShards = asyncBatchFetchStore.keySet().stream().flatMap(shardsBatch -> shardsBatch.getBatchedShards().stream()).collect(Collectors.toSet());
        }
        Set<ShardRouting> shardsToBatch = Sets.newHashSet();
        // add all unassigned shards to the batch if they are not already in a batch
        unassigned.forEach(shardRouting -> {
            if ((currentBatchedShards.contains(shardRouting) == false) && (shardRouting.primary() == primary)) {
                assert shardRouting.unassigned();
                shardsToBatch.add(shardRouting);
            }
        });
        Iterator<ShardRouting> iterator = shardsToBatch.iterator();
        long batchSize = MAX_BATCH_SIZE;
        Map<ShardRouting, String> addToCurrentBatch = new HashMap<>();
        while (iterator.hasNext()) {
            ShardRouting currentShard = iterator.next();
            if (batchSize > 0) {
                addToCurrentBatch.put(currentShard, IndexMetadata.INDEX_DATA_PATH_SETTING.get(allocation.metadata().index(currentShard.index()).getSettings()));
                batchSize--;
                iterator.remove();
            }
            // add to batch if batch size full or last shard in unassigned list
            if (batchSize == 0 || iterator.hasNext() == false) {
                String batchUUId = UUIDs.base64UUID();
                ShardsBatch shardsBatch = new ShardsBatch(batchUUId, addToCurrentBatch);
                Map<ShardId, String> shardIdsMap = addToCurrentBatch.entrySet().stream().collect(Collectors.toMap(
                    entry -> entry.getKey().shardId(),
                    Map.Entry::getValue
                ));
                if (primary) {
                    asyncBatchFetchStarted.computeIfAbsent(
                        shardsBatch,
                        batch -> new InternalBatchAsyncFetch<>(
                            logger,
                            "batch_shards_started",
                            shardIdsMap,
                            this.batchStartedAction,
                            batch.getBatchId()
                        ));
                } else {
                    asyncBatchFetchStore.computeIfAbsent(
                        shardsBatch,
                        batch -> new InternalBatchAsyncFetch<>(
                            logger,
                            "batch_shards_store",
                            shardIdsMap,
                            this.batchStoreAction,
                            batch.getBatchId()
                        ));
                }
                addToCurrentBatch.clear();
                batchSize = MAX_BATCH_SIZE;
            }
        }
    }

    // allow for testing infra to change shard allocators implementation
    protected static void innerAllocatedUnassigned(
        RoutingAllocation allocation,
        PrimaryShardAllocator primaryShardAllocator,
        ReplicaShardAllocator replicaShardAllocator,
        ShardRouting shardRouting,
        ExistingShardsAllocator.UnassignedAllocationHandler unassignedAllocationHandler
    ) {
        assert shardRouting.unassigned();
        if (shardRouting.primary()) {
            primaryShardAllocator.allocateUnassigned(shardRouting, allocation, unassignedAllocationHandler);
        } else {
            replicaShardAllocator.allocateUnassigned(shardRouting, allocation, unassignedAllocationHandler);
        }
    }

    @Override
    public AllocateUnassignedDecision explainUnassignedShardAllocation(ShardRouting unassignedShard, RoutingAllocation routingAllocation) {
        assert unassignedShard.unassigned();
        assert routingAllocation.debugDecision();
        if (unassignedShard.primary()) {
            assert primaryShardAllocator != null;
            return primaryShardAllocator.makeAllocationDecision(unassignedShard, routingAllocation, logger);
        } else {
            assert replicaShardAllocator != null;
            return replicaShardAllocator.makeAllocationDecision(unassignedShard, routingAllocation, logger);
        }
    }

    /**
     * Clear the fetched data for the primary to ensure we do not cancel recoveries based on excessively stale data.
     */
    private void ensureAsyncFetchStorePrimaryRecency(RoutingAllocation allocation) {
        DiscoveryNodes nodes = allocation.nodes();
        if (hasNewNodes(nodes)) {
            final Set<String> newEphemeralIds = StreamSupport.stream(Spliterators.spliterator(nodes.getDataNodes().entrySet(), 0), false)
                .map(node -> node.getValue().getEphemeralId())
                .collect(Collectors.toSet());
            // Invalidate the cache if a data node has been added to the cluster. This ensures that we do not cancel a recovery if a node
            // drops out, we fetch the shard data, then some indexing happens and then the node rejoins the cluster again. There are other
            // ways we could decide to cancel a recovery based on stale data (e.g. changing allocation filters or a primary failure) but
            // making the wrong decision here is not catastrophic so we only need to cover the common case.
            logger.trace(
                () -> new ParameterizedMessage(
                    "new nodes {} found, clearing primary async-fetch-store cache",
                    Sets.difference(newEphemeralIds, lastSeenEphemeralIds)
                )
            );
            asyncFetchStore.values().forEach(fetch -> clearCacheForPrimary(fetch, allocation));
            // recalc to also (lazily) clear out old nodes.
            this.lastSeenEphemeralIds = newEphemeralIds;
        }
    }

    private static void clearCacheForPrimary(
        AsyncShardFetch<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> fetch,
        RoutingAllocation allocation
    ) {
        ShardRouting primary = allocation.routingNodes().activePrimary(fetch.shardId);
        if (primary != null) {
            fetch.clearCacheForNode(primary.currentNodeId());
        }
    }

    private boolean hasNewNodes(DiscoveryNodes nodes) {
        for (final DiscoveryNode node : nodes.getDataNodes().values()) {
            if (lastSeenEphemeralIds.contains(node.getEphemeralId()) == false) {
                return true;
            }
        }
        return false;
    }

    class InternalAsyncFetch<T extends BaseNodeResponse> extends AsyncShardFetch<T> {

        InternalAsyncFetch(
            Logger logger,
            String type,
            ShardId shardId,
            String customDataPath,
            Lister<? extends BaseNodesResponse<T>, T> action
        ) {
            super(logger, type, shardId, customDataPath, action);
        }

        @Override
        protected void reroute(ShardId shardId, String reason) {
            logger.trace("{} scheduling reroute for {}", shardId, reason);
            assert rerouteService != null;
            rerouteService.reroute(
                "async_shard_fetch",
                Priority.HIGH,
                ActionListener.wrap(
                    r -> logger.trace("{} scheduled reroute completed for {}", shardId, reason),
                    e -> logger.debug(new ParameterizedMessage("{} scheduled reroute failed for {}", shardId, reason), e)
                )
            );
        }
    }

    class InternalBatchAsyncFetch<T extends BaseNodeResponse> extends AsyncBatchShardFetch<T> {

        InternalBatchAsyncFetch(Logger logger,
                                String type,
                                Map<ShardId, String> map,
                                AsyncBatchShardFetch.Lister<? extends BaseNodesResponse<T>, T> action,
                                String batchUUId
        ) {
            super(logger, type, map, action, batchUUId);
        }

        @Override
        protected void reroute(String batchUUId, String reason) {
            logger.trace("{} scheduling reroute for {}", batchUUId, reason);
            assert rerouteService != null;
            rerouteService.reroute(
                "async_shard_fetch",
                Priority.HIGH,
                ActionListener.wrap(
                    r -> logger.trace("{} scheduled reroute completed for {}", batchUUId, reason),
                    e -> logger.debug(new ParameterizedMessage("{} scheduled reroute failed for {}", batchUUId, reason), e)
                )
            );
        }
    }

    class InternalPrimaryShardAllocator extends PrimaryShardAllocator {

        private final TransportNodesListGatewayStartedShards startedAction;

        InternalPrimaryShardAllocator(TransportNodesListGatewayStartedShards startedAction) {
            this.startedAction = startedAction;
        }

        @Override
        protected AsyncShardFetch.FetchResult<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetchData(
            ShardRouting shard,
            RoutingAllocation allocation
        ) {
            AsyncShardFetch<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetch = asyncFetchStarted.computeIfAbsent(
                shard.shardId(),
                shardId -> new InternalAsyncFetch<>(
                    logger,
                    "shard_started",
                    shardId,
                    IndexMetadata.INDEX_DATA_PATH_SETTING.get(allocation.metadata().index(shard.index()).getSettings()),
                    startedAction
                )
            );
            AsyncShardFetch.FetchResult<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> shardState = fetch.fetchData(
                allocation.nodes(),
                allocation.getIgnoreNodes(shard.shardId())
            );

            if (shardState.hasData()) {
                shardState.processAllocation(allocation);
            }
            return shardState;
        }
    }

    class InternalPrimaryBatchShardAllocator extends PrimaryShardBatchAllocator {
        private final TransportNodesListGatewayStartedShardsBatch startedAction;

        InternalPrimaryBatchShardAllocator(TransportNodesListGatewayStartedShardsBatch startedAction) {
            this.startedAction = startedAction;
        }

        @Override
        protected AsyncBatchShardFetch.FetchResult<TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShardsBatch> fetchData(String batchId, Set<ShardRouting> inEligibleShards, RoutingAllocation allocation) {
            ShardsBatch shardsBatchKey = new ShardsBatch(batchId);
            AsyncBatchShardFetch<TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShardsBatch> fetch =
                asyncBatchFetchStarted.get(new ShardsBatch(batchId));
            Map<ShardId, Set<String>> shardToIgnoreNodes = new HashMap<>();
            // remove in-eligible shards which allocator is not responsible for
            fetch.shardsToCustomDataPathMap.keySet().removeAll(inEligibleShards.stream().map(ShardRouting::shardId).collect(Collectors.toSet()));
            for(ShardId shardId : asyncBatchFetchStarted.get(shardsBatchKey).shardsToCustomDataPathMap.keySet()){
                shardToIgnoreNodes.put(shardId, allocation.getIgnoreNodes(shardId));
            }
            AsyncBatchShardFetch.FetchResult<TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShardsBatch> shardBatchState = fetch.fetchData(
                allocation.nodes(),
                shardToIgnoreNodes
            );

            if (shardBatchState.hasData()) {
                shardBatchState.processAllocation(allocation);
            }
            return shardBatchState;
        }

    }

    class InternalReplicaShardAllocator extends ReplicaShardAllocator {

        private final TransportNodesListShardStoreMetadata storeAction;

        InternalReplicaShardAllocator(TransportNodesListShardStoreMetadata storeAction) {
            this.storeAction = storeAction;
        }

        @Override
        protected AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> fetchData(
            ShardRouting shard,
            RoutingAllocation allocation
        ) {
            AsyncShardFetch<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> fetch = asyncFetchStore.computeIfAbsent(
                shard.shardId(),
                shardId -> new InternalAsyncFetch<>(
                    logger,
                    "shard_store",
                    shard.shardId(),
                    IndexMetadata.INDEX_DATA_PATH_SETTING.get(allocation.metadata().index(shard.index()).getSettings()),
                    storeAction
                )
            );
            AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> shardStores = fetch.fetchData(
                allocation.nodes(),
                allocation.getIgnoreNodes(shard.shardId())
            );
            if (shardStores.hasData()) {
                shardStores.processAllocation(allocation);
            }
            return shardStores;
        }

        @Override
        protected boolean hasInitiatedFetching(ShardRouting shard) {
            return asyncFetchStore.get(shard.shardId()) != null;
        }
    }

    class InternalReplicaBatchShardAllocator extends ReplicaShardAllocator {

        private final TransportNodesListShardStoreMetadataBatch storeAction;

        InternalReplicaBatchShardAllocator(TransportNodesListShardStoreMetadataBatch storeAction) {
            this.storeAction = storeAction;
        }

        @Override
        protected AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> fetchData(ShardRouting shard, RoutingAllocation allocation) {
            return null;
        }

        @Override
        protected boolean hasInitiatedFetching(ShardRouting shard) {
            return false;
        }
    }


    private static class ShardsBatch {
        private final String batchId;

        public Map<ShardRouting, String> getShardsToCustomDataPathMap() {
            return shardsToCustomDataPathMap;
        }

        private Map<ShardRouting, String> shardsToCustomDataPathMap;

        private ShardsBatch(String uuid, Map<ShardRouting, String> shardsToCustomDataPathMap) {
            this.batchId = uuid;
            this.shardsToCustomDataPathMap = shardsToCustomDataPathMap;
        }

        private ShardsBatch(String batchId) {
            this.batchId = batchId;
        }

        void removeFromBatch(ShardRouting shard) {
            shardsToCustomDataPathMap.remove(shard);
        }

        Set<ShardRouting> getBatchedShards() {
            return shardsToCustomDataPathMap.keySet();
        }

        public String getBatchId() {
            return batchId;
        }

        @Override
        public boolean equals(Object shardsBatch) {
            if (shardsBatch == this) {
                return true;
            }
            if (shardsBatch == null || shardsBatch.getClass() != getClass()) {
                return false;
            }
            ShardsBatch batch = (ShardsBatch) shardsBatch;
            return batchId.equals(batch.getBatchId());
        }

        @Override
        public int hashCode() {
            return Objects.hash(batchId);
        }

        @Override
        public String toString() {
            return "batchId: " + batchId;
        }
    }


}
