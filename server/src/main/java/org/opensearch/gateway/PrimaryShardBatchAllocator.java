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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.NodeAllocationResult;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.env.ShardLockObtainFailedException;
import org.opensearch.gateway.AsyncShardFetch.FetchResult;
import org.opensearch.gateway.TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShardsBatch;
import org.opensearch.gateway.TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShards;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * PrimaryShardBatchAllocator is similar to {@link org.opensearch.gateway.PrimaryShardAllocator} only difference is
 * that it can allocate multiple unassigned primary shards wherein PrimaryShardAllocator can only allocate single
 * unassigned shard.
 * The primary shard batch allocator allocates multiple unassigned primary shards to nodes that hold
 * valid copies of the unassigned primaries.  It does this by iterating over all unassigned
 * primary shards in the routing table and fetching shard metadata from each node in the cluster
 * that holds a copy of the shard.  The shard metadata from each node is compared against the
 * set of valid allocation IDs and for all valid shard copies (if any), the primary shard batch allocator
 * executes the allocation deciders to chose a copy to assign the primary shard to.
 * <p>
 * Note that the PrimaryShardBatchAllocator does *not* allocate primaries on index creation
 * (see {@link org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator}),
 * nor does it allocate primaries when a primary shard failed and there is a valid replica
 * copy that can immediately be promoted to primary, as this takes place in {@link RoutingNodes#failShard}.
 *
 * @opensearch.internal
 */
public abstract class PrimaryShardBatchAllocator extends PrimaryShardAllocator {

    abstract protected FetchResult<NodeGatewayStartedShardsBatch> fetchData(Set<ShardRouting> shardsEligibleForFetch,
                                                                            Set<ShardRouting> inEligibleShards,
                                                                            RoutingAllocation allocation);

    protected FetchResult<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetchData(ShardRouting shard, RoutingAllocation allocation){
        return null;
    }

    @Override
    public AllocateUnassignedDecision makeAllocationDecision(ShardRouting unassignedShard,
                                                             RoutingAllocation allocation,
                                                             Logger logger) {

        return makeAllocationDecision(new HashSet<>(Collections.singletonList(unassignedShard)),
            allocation, logger).get(unassignedShard);
    }

    /**
     * Build allocation decisions for all the shards present in the batch identified by batchId.
     *
     * @param shards     set of shards given for allocation
     * @param allocation current allocation of all the shards
     * @param logger     logger used for logging
     * @return shard to allocation decision map
     */
    @Override
    public HashMap<ShardRouting, AllocateUnassignedDecision> makeAllocationDecision(Set<ShardRouting> shards,
                                                                                    RoutingAllocation allocation,
                                                                                    Logger logger) {
        HashMap<ShardRouting, AllocateUnassignedDecision> shardAllocationDecisions = new HashMap<>();
        final boolean explain = allocation.debugDecision();
        Set<ShardRouting> shardsEligibleForFetch = new HashSet<>();
        Set<ShardRouting> shardsNotEligibleForFetch = new HashSet<>();
        // identify ineligible shards
        for (ShardRouting shard : shards) {
            ShardRouting matchingShard = null;
            for (RoutingNode node: allocation.routingNodes()) {
                matchingShard = node.getByShardId(shard.shardId());
                if (matchingShard != null && matchingShard.primary() == shard.primary()) {
                    // we have a matching shard on this node, so this is a valid copy
                    break;
                }
            }
            if (matchingShard == null) {
                matchingShard = shard;
            }
            AllocateUnassignedDecision decision = skipSnapshotRestore(matchingShard, allocation);
            if (decision != null) {
                shardsNotEligibleForFetch.add(shard);
                shardAllocationDecisions.put(shard, decision);
            } else {
                shardsEligibleForFetch.add(shard);
            }
        }
        // Do not call fetchData if there are no eligible shards
        if (shardsEligibleForFetch.size() == 0) {
            return shardAllocationDecisions;
        }
        // only fetch data for eligible shards
        final FetchResult<NodeGatewayStartedShardsBatch> shardsState = fetchData(shardsEligibleForFetch, shardsNotEligibleForFetch, allocation);
        // Note : shardsState contain the Data, there key is DiscoveryNode but value is Map<ShardId,
        // NodeGatewayStartedShardsBatch> so to get one shard level data (from all the nodes), we'll traverse the map
        // and construct the nodeShardState along the way before making any allocation decision. As metadata for a
        // particular shard is needed from all the discovery nodes.

        // process the received data
        for (ShardRouting unassignedShard : shardsEligibleForFetch) {
            if (shardsState.hasData() == false) {
                // if fetching is not done, add that no decision in the resultant map
                allocation.setHasPendingAsyncFetch();
                List<NodeAllocationResult> nodeDecisions = null;
                if (explain) {
                    nodeDecisions = buildDecisionsForAllNodes(unassignedShard, allocation);
                }
                shardAllocationDecisions.put(unassignedShard,
                    AllocateUnassignedDecision.no(AllocationStatus.FETCHING_SHARD_DATA,
                        nodeDecisions));
            } else {

                NodeShardStates nodeShardStates = getNodeShardStates(unassignedShard, shardsState);
                // get allocation decision for this shard
                shardAllocationDecisions.put(unassignedShard, getAllocationDecision(unassignedShard, allocation,
                    nodeShardStates, logger));
            }
        }
        return shardAllocationDecisions;
    }

    private static NodeShardStates getNodeShardStates(ShardRouting unassignedShard, FetchResult<NodeGatewayStartedShardsBatch> shardsState) {
        NodeShardStates nodeShardStates = new NodeShardStates(new Comparator<INodeShardState>() {
            @Override
            public int compare(INodeShardState o1, INodeShardState o2) {
                return 1;
            }
        });
        Map<DiscoveryNode, NodeGatewayStartedShardsBatch> nodeResponses = shardsState.getData();

        // build data for a shard from all the nodes
        nodeResponses.forEach((node, nodeGatewayStartedShardsBatch) -> {
            nodeShardStates.add(new NodeShardState(nodeGatewayStartedShardsBatch.getNodeGatewayStartedShardsBatch().get(unassignedShard.shardId()), node), node);
        });
        return nodeShardStates;
    }


    private static class NodeShardState implements INodeShardState {
        NodeGatewayStartedShards shardState;
        DiscoveryNode node;

        public NodeShardState(NodeGatewayStartedShards shardState, DiscoveryNode node) {
            this.shardState = shardState;
            this.node = node;
        }

        @Override
        public BaseNodeGatewayStartedShards getShardState() {
            return this.shardState;
        }

        @Override
        public DiscoveryNode getNode() {
            return this.node;
        }
    }
}
