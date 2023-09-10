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
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.AllocationDecision;
import org.opensearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.opensearch.cluster.routing.allocation.NodeAllocationResult;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.Decision;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * An abstract class that implements basic functionality for allocating
 * shards to nodes based on shard copies that already exist in the cluster.
 * <p>
 * Individual implementations of this class are responsible for providing
 * the logic to determine to which nodes (if any) those shards are allocated.
 *
 * @opensearch.internal
 */
public abstract class BaseGatewayShardAllocator {

    protected final Logger logger = LogManager.getLogger(this.getClass());

    /**
     * Allocate an unassigned shard to nodes (if any) where valid copies of the shard already exist.
     * It is up to the individual implementations of {@link #makeAllocationDecision(ShardRouting, RoutingAllocation, Logger)}
     * to make decisions on assigning shards to nodes.
     *
     * @param shardRouting                the shard to allocate
     * @param allocation                  the allocation state container object
     * @param unassignedAllocationHandler handles the allocation of the current shard
     */
    public void allocateUnassigned(
        ShardRouting shardRouting,
        RoutingAllocation allocation,
        ExistingShardsAllocator.UnassignedAllocationHandler unassignedAllocationHandler
    ) {
        final AllocateUnassignedDecision allocateUnassignedDecision = makeAllocationDecision(shardRouting, allocation, logger);
        executeDecision(shardRouting, allocateUnassignedDecision, allocation, unassignedAllocationHandler);
    }

    public void allocateUnassignedBatch(Set<ShardRouting> shards, RoutingAllocation allocation) {
        // make Allocation Decisions for all shards
        HashMap<ShardRouting, AllocateUnassignedDecision> decisionMap = makeAllocationDecision(shards,
            allocation, logger);
        assert shards.size() == decisionMap.size() : "make allocation decision didn't return allocation decision for " +
            "some shards";
        // get all unassigned shards
        RoutingNodes.UnassignedShards.UnassignedIterator iterator = allocation.routingNodes().unassigned().iterator();

        while (iterator.hasNext()) {
            ShardRouting shard = iterator.next();
            try {
                if (decisionMap.isEmpty() == false) {
                    if (shards.stream().filter(shardRouting -> shardRouting.shardId().equals(shard.shardId()) &&
                        shardRouting.primary() == shard.primary()).count() == 1) {
                        List<ShardRouting> matchedShardRouting =
                            decisionMap.keySet().stream().filter(shardRouting -> shardRouting.shardId().equals(shard.shardId())
                                && shardRouting.primary() == shard.primary()).collect(Collectors.toList());
                        if (matchedShardRouting.size() == 1) {
                            executeDecision(shard,
                                decisionMap.remove(matchedShardRouting.get(0)),
                                allocation,
                                iterator);
                        } else if (matchedShardRouting.size() > 1){
                            // Adding this just to check the behaviour if we ever land up here.
                            throw new IllegalStateException("decision map must have single entry for 1 shard");
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("failed to execute decision for shard {} ", shard, e);
            }
        }
    }

    private void executeDecision(ShardRouting shardRouting,
                                 AllocateUnassignedDecision allocateUnassignedDecision,
                                 RoutingAllocation allocation,
                                 ExistingShardsAllocator.UnassignedAllocationHandler unassignedAllocationHandler) {
        if (allocateUnassignedDecision.isDecisionTaken() == false) {
            // no decision was taken by this allocator
            return;
        }

        if (allocateUnassignedDecision.getAllocationDecision() == AllocationDecision.YES) {
            unassignedAllocationHandler.initialize(
                allocateUnassignedDecision.getTargetNode().getId(),
                allocateUnassignedDecision.getAllocationId(),
                getExpectedShardSize(shardRouting, allocation),
                allocation.changes()
            );
        } else {
            unassignedAllocationHandler.removeAndIgnore(allocateUnassignedDecision.getAllocationStatus(), allocation.changes());
        }
    }


    protected long getExpectedShardSize(ShardRouting shardRouting, RoutingAllocation allocation) {
        if (shardRouting.primary()) {
            if (shardRouting.recoverySource().getType() == RecoverySource.Type.SNAPSHOT) {
                return allocation.snapshotShardSizeInfo().getShardSize(shardRouting, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
            } else {
                return ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE;
            }
        } else {
            return allocation.clusterInfo().getShardSize(shardRouting, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        }
    }

    /**
     * Make a decision on the allocation of an unassigned shard.  This method is used by
     * {@link #allocateUnassigned(ShardRouting, RoutingAllocation, ExistingShardsAllocator.UnassignedAllocationHandler)} to make decisions
     * about whether or not the shard can be allocated by this allocator and if so, to which node it will be allocated.
     *
     * @param unassignedShard the unassigned shard to allocate
     * @param allocation      the current routing state
     * @param logger          the logger
     * @return an {@link AllocateUnassignedDecision} with the final decision of whether to allocate and details of the decision
     */
    public abstract AllocateUnassignedDecision makeAllocationDecision(
        ShardRouting unassignedShard,
        RoutingAllocation allocation,
        Logger logger
    );

    public HashMap<ShardRouting, AllocateUnassignedDecision> makeAllocationDecision(
        Set<ShardRouting> shards,
        RoutingAllocation allocation,
        Logger logger
    ) {
        HashMap<ShardRouting, AllocateUnassignedDecision> allocationDecisions = new HashMap<>();
        for (ShardRouting unassignedShard : shards) {
            allocationDecisions.put(unassignedShard, makeAllocationDecision(unassignedShard, allocation, logger));
        }
        return allocationDecisions;
    }

    /**
     * Builds decisions for all nodes in the cluster, so that the explain API can provide information on
     * allocation decisions for each node, while still waiting to allocate the shard (e.g. due to fetching shard data).
     */
    protected static List<NodeAllocationResult> buildDecisionsForAllNodes(ShardRouting shard, RoutingAllocation allocation) {
        List<NodeAllocationResult> results = new ArrayList<>();
        for (RoutingNode node : allocation.routingNodes()) {
            Decision decision = allocation.deciders().canAllocate(shard, node, allocation);
            results.add(new NodeAllocationResult(node.node(), null, decision));
        }
        return results;
    }
}
