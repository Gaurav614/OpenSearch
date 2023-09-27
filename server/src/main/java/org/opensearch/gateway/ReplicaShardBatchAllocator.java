/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.NodeAllocationResult;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.common.collect.Tuple;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.gateway.AsyncShardFetch.FetchResult;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.store.StoreFilesMetadata;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadata;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadataBatch;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadataBatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.cluster.routing.UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING;

public abstract class ReplicaShardBatchAllocator extends ReplicaShardAllocator {

    /**
     * Process existing recoveries of replicas and see if we need to cancel them if we find a better
     * match. Today, a better match is one that can perform a no-op recovery while the previous recovery
     * has to copy segment files.
     */
    public void processExistingRecoveries(RoutingAllocation allocation, List<Set<ShardRouting>> shardBatches) {
        Metadata metadata = allocation.metadata();
        RoutingNodes routingNodes = allocation.routingNodes();
        List<Runnable> shardCancellationActions = new ArrayList<>();
        for (Set<ShardRouting> shardBatch : shardBatches) {
            Set<ShardRouting> eligibleFetchShards = new HashSet<>();
            Set<ShardRouting> ineligibleShards = new HashSet<>();
            boolean shardMatched;
            for (ShardRouting shard : shardBatch) {
                shardMatched = false;
                for (RoutingNode routingNode : routingNodes) {
                    if (routingNode.getByShardId(shard.shardId()) != null) {
                        ShardRouting shardFromRoutingNode = routingNode.getByShardId(shard.shardId());
                        if (!shardFromRoutingNode.primary()){
                            shardMatched = true;
                            if (shardFromRoutingNode.primary()) {
//                                ineligibleShards.add(shard);
                                continue;
                            }
                            if (shardFromRoutingNode.initializing() == false) {
                                continue;
                            }
                            if (shardFromRoutingNode.relocatingNodeId() != null) {
                                continue;
                            }

                            // if we are allocating a replica because of index creation, no need to go and find a copy, there isn't one...
                            if (shardFromRoutingNode.unassignedInfo() != null && shardFromRoutingNode.unassignedInfo()
                                .getReason() == UnassignedInfo.Reason.INDEX_CREATED) {
                                continue;
                            }
                            eligibleFetchShards.add(shardFromRoutingNode);
                        }
                    }
                    if (shardMatched){
                        break;
                    }
                }
            }
            AsyncShardFetch.FetchResult <NodeStoreFilesMetadataBatch> shardState = fetchData(eligibleFetchShards, ineligibleShards, allocation);
            if (!shardState.hasData()) {
                logger.trace("{}: fetching new stores for initializing shard batch", eligibleFetchShards);
                continue; // still fetching
            }
            for (ShardRouting shard : eligibleFetchShards) {
                ShardRouting primaryShard = allocation.routingNodes().activePrimary(shard.shardId());
                assert primaryShard != null : "the replica shard can be allocated on at least one node, so there must be an active primary";
                assert primaryShard.currentNodeId() != null;
                final DiscoveryNode primaryNode = allocation.nodes().get(primaryShard.currentNodeId());
                NodeShardStores nodeShardStores = getNodeShardStores(shard, shardState);
                final StoreFilesMetadata primaryStore = findStore(primaryNode, nodeShardStores);
                if (primaryStore == null) {
                    // if we can't find the primary data, it is probably because the primary shard is corrupted (and listing failed)
                    // just let the recovery find it out, no need to do anything about it for the initializing shard
                    logger.trace("{}: no primary shard store found or allocated, letting actual allocation figure it out", shard);
                    continue;
                }
                ReplicaShardAllocator.MatchingNodes matchingNodes = findMatchingNodes(shard, allocation, true, primaryNode, primaryStore, nodeShardStores, false);
                if (matchingNodes.getNodeWithHighestMatch() != null) {
                    DiscoveryNode currentNode = allocation.nodes().get(shard.currentNodeId());
                    DiscoveryNode nodeWithHighestMatch = matchingNodes.getNodeWithHighestMatch();
                    // current node will not be in matchingNodes as it is filtered away by SameShardAllocationDecider
                    if (currentNode.equals(nodeWithHighestMatch) == false
                        && matchingNodes.canPerformNoopRecovery(nodeWithHighestMatch)
                        && canPerformOperationBasedRecovery(primaryStore, nodeShardStores, currentNode) == false) {
                        // we found a better match that can perform noop recovery, cancel the existing allocation.
                        logger.debug(
                            "cancelling allocation of replica on [{}], can perform a noop recovery on node [{}]",
                            currentNode,
                            nodeWithHighestMatch
                        );
                        final Set<String> failedNodeIds = shard.unassignedInfo() == null
                            ? Collections.emptySet()
                            : shard.unassignedInfo().getFailedNodeIds();
                        UnassignedInfo unassignedInfo = new UnassignedInfo(
                            UnassignedInfo.Reason.REALLOCATED_REPLICA,
                            "existing allocation of replica to ["
                                + currentNode
                                + "] cancelled, can perform a noop recovery on ["
                                + nodeWithHighestMatch
                                + "]",
                            null,
                            0,
                            allocation.getCurrentNanoTime(),
                            System.currentTimeMillis(),
                            false,
                            UnassignedInfo.AllocationStatus.NO_ATTEMPT,
                            failedNodeIds
                        );
                        // don't cancel shard in the loop as it will cause a ConcurrentModificationException
                        shardCancellationActions.add(
                            () -> routingNodes.failShard(
                                logger,
                                shard,
                                unassignedInfo,
                                metadata.getIndexSafe(shard.index()),
                                allocation.changes()
                            )
                        );
                    }
                }
            }
        }
        for (Runnable action : shardCancellationActions) {
            action.run();
        }
    }


    abstract protected FetchResult<NodeStoreFilesMetadataBatch> fetchData(Set<ShardRouting> shardEligibleForFetch,
                                                                          Set<ShardRouting> inEligibleShards,
                                                                          RoutingAllocation allocation);

    @Override
    protected FetchResult<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> fetchData(ShardRouting shard, RoutingAllocation allocation) {
        return null;
    }

    @Override
    public AllocateUnassignedDecision makeAllocationDecision(ShardRouting unassignedShard, RoutingAllocation allocation, Logger logger) {
        return makeAllocationDecision(new HashSet<>(Collections.singletonList(unassignedShard)),
            allocation, logger).get(unassignedShard);
    }

    @Override
    public HashMap<ShardRouting, AllocateUnassignedDecision> makeAllocationDecision(Set<ShardRouting> shards, RoutingAllocation allocation, Logger logger) {
        HashMap<ShardRouting, AllocateUnassignedDecision> shardAllocationDecisions = new HashMap<>();
        final boolean explain = allocation.debugDecision();
        final RoutingNodes routingNodes = allocation.routingNodes();
        Set<ShardRouting> shardsEligibleForFetch = new HashSet<>();
        Set<ShardRouting> shardsNotEligibleForFetch = new HashSet<>();
        HashMap<ShardRouting, Tuple<Decision, Map<String, NodeAllocationResult>>> nodeAllocationDecisions = new HashMap<>();
        for (ShardRouting shard : shards) {
            if (!isResponsibleFor(shard)) {
                // this allocator n is not responsible for allocating this shard
                shardsNotEligibleForFetch.add(shard);
                shardAllocationDecisions.put(shard, AllocateUnassignedDecision.NOT_TAKEN);
                continue;
            }

            Tuple<Decision, Map<String, NodeAllocationResult>> result = canBeAllocatedToAtLeastOneNode(shard, allocation);
            Decision allocationDecision = result.v1();
            if (allocationDecision.type() != Decision.Type.YES && (!explain || !hasInitiatedFetching(shard))) {
                // only return early if we are not in explain mode, or we are in explain mode but we have not
                // yet attempted to fetch any shard data
                logger.trace("{}: ignoring allocation, can't be allocated on any node", shard);
                shardAllocationDecisions.put(shard,
                    AllocateUnassignedDecision.no(UnassignedInfo.AllocationStatus.fromDecision(allocationDecision.type()),
                        result.v2() != null ? new ArrayList<>(result.v2().values()) : null));
                continue;
            }
            // storing the nodeDecisions in nodeAllocationDecisions if the decision is not YES
            // so that we don't have to compute the decisions again
            nodeAllocationDecisions.put(shard, result);

            shardsEligibleForFetch.add(shard);
        }

        // Do not call fetchData if there are no eligible shards
        if (shardsEligibleForFetch.size() == 0) {
            return shardAllocationDecisions;
        }
        // only fetch data for eligible shards
        final FetchResult<NodeStoreFilesMetadataBatch> shardsState = fetchData(shardsEligibleForFetch, shardsNotEligibleForFetch, allocation);

        for (ShardRouting unassignedShard : shardsEligibleForFetch) {
            if (!shardsState.hasData()) {
                logger.trace("{}: ignoring allocation, still fetching shard stores", unassignedShard);
                allocation.setHasPendingAsyncFetch();
                List<NodeAllocationResult> nodeDecisions = null;
                if (explain) {
                    nodeDecisions = buildDecisionsForAllNodes(unassignedShard, allocation);
                }
                shardAllocationDecisions.put(unassignedShard,
                    AllocateUnassignedDecision.no(UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA, nodeDecisions));
                continue;
            }
            Tuple<Decision, Map<String, NodeAllocationResult>> result = nodeAllocationDecisions.get(unassignedShard);
            shardAllocationDecisions.put(unassignedShard, getAllocationDecision(unassignedShard, allocation, getNodeShardStores(unassignedShard, shardsState), result, logger));
        }
        return shardAllocationDecisions;
    }

    private NodeShardStores getNodeShardStores(ShardRouting unassignedShard, FetchResult<NodeStoreFilesMetadataBatch> data) {
        NodeShardStores nodeShardStores = new NodeShardStores();
        if (data.getData() != null) {
            nodeShardStores.getNodeShardStores().putAll(
                data.getData().entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> new NodeShardStore(entry.getValue().getNodeStoreFilesMetadataBatch().get(unassignedShard.shardId()).storeFilesMetadata()))));
        }
        return nodeShardStores;
    }
}
