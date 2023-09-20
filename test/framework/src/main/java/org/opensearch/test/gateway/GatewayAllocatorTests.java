/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.gateway.GatewayAllocator;
import org.opensearch.snapshots.SnapshotShardSizeInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class GatewayAllocatorTests extends OpenSearchAllocationTestCase {

    private final Logger logger = LogManager.getLogger(GatewayAllocatorTests.class);

    public void testSingleBatchCreation(){
        TestGatewayAllocator testGatewayAllocator = new TestGatewayAllocator();
        AllocationService allocation = createAllocationService(Settings.EMPTY, testGatewayAllocator);
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(3)
                    .numberOfReplicas(0)
            )
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(RoutingTable.builder().addAsNew(metadata.index("test")).build())
            .build();
        RoutingAllocation testAllocation= new RoutingAllocation(new AllocationDeciders(Collections.emptyList()),
            new RoutingNodes(clusterState, false),
            clusterState,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime());
        // test for primary
        Set<String> batchesToAssign = testGatewayAllocator.callCreateAndUpdateBatches(testAllocation, true);
        assertEquals(1, batchesToAssign.size());

        // test for replicas
       batchesToAssign = testGatewayAllocator.callCreateAndUpdateBatches(testAllocation, false);
       assertEquals(0, batchesToAssign.size());
    }

    public void testTwoBatchCreation(){
        TestGatewayAllocator testGatewayAllocator = new TestGatewayAllocator();
        AllocationService allocation = createAllocationService(Settings.EMPTY, testGatewayAllocator);
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1020)
                    .numberOfReplicas(1)
            )
            .put(
                IndexMetadata.builder("test-2")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1020)
                    .numberOfReplicas(1)
            ).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(RoutingTable.builder().addAsNew(metadata.index("test")).addAsNew(metadata.index("test-2")).build())
            .build();
        RoutingAllocation testAllocation= new RoutingAllocation(new AllocationDeciders(Collections.emptyList()),
            new RoutingNodes(clusterState, false),
            clusterState,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime());

        final Set<String> batchesToAssignForPrimaries = testGatewayAllocator.callCreateAndUpdateBatches(testAllocation, true);
        assertEquals(2, batchesToAssignForPrimaries.size());

        List<GatewayAllocator.ShardsBatch> listOfBatches = new ArrayList<>(testGatewayAllocator.getBatchIdToStartedShardBatch().values());
        assertFalse(listOfBatches.get(0).equals(listOfBatches.get(1)));

        // test for replicas
        final Set<String> batchesToAssignForReplicas = testGatewayAllocator.callCreateAndUpdateBatches(testAllocation, false);
        assertEquals(2, batchesToAssignForReplicas.size());
        listOfBatches = new ArrayList<>(testGatewayAllocator.getBatchIdToStoreShardBatch().values());
        assertFalse(listOfBatches.get(0).equals(listOfBatches.get(1)));
    }

    public void testNonDuplicationOfBatch(){
        TestGatewayAllocator testGatewayAllocator = new TestGatewayAllocator();
        AllocationService allocation = createAllocationService(Settings.EMPTY, testGatewayAllocator);
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(3)
                    .numberOfReplicas(1)
            )
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(RoutingTable.builder().addAsNew(metadata.index("test")).build())
            .build();
        RoutingAllocation testAllocation= new RoutingAllocation(new AllocationDeciders(Collections.emptyList()),
            new RoutingNodes(clusterState, false),
            clusterState,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime());
        Set<String> batchesToAssignPrimaries = testGatewayAllocator.callCreateAndUpdateBatches(testAllocation, true);
        Set<String> batchesToAssignReplicas = testGatewayAllocator.callCreateAndUpdateBatches(testAllocation, false);
        assertEquals(1, batchesToAssignPrimaries.size());
        assertEquals(1, batchesToAssignReplicas.size());

        // again try to create batch and verify no new batch is created since shard is already batched and no new unassigned shard
        assertEquals(batchesToAssignPrimaries, testGatewayAllocator.callCreateAndUpdateBatches(testAllocation, true));
        assertEquals(batchesToAssignReplicas, testGatewayAllocator.callCreateAndUpdateBatches(testAllocation, false));
    }

    public void testCorrectnessOfBatch(){
        /*
         1. Test only unassigned Shards are in batches
         2. Test correct shards in a batch for a index
         3. Test batchIdToStartedShardBatch and batchIdToStoreShardBatch maps has correct set shards batched in GatewayAllocator
         */

        TestGatewayAllocator testGatewayAllocator = new TestGatewayAllocator();
        AllocationService allocation = createAllocationService(Settings.EMPTY, testGatewayAllocator);
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1020)
                    .numberOfReplicas(1)
            )
            .put(
                IndexMetadata.builder("test-2")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1020)
                    .numberOfReplicas(1)
            ).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(RoutingTable.builder().addAsNew(metadata.index("test")).addAsNew(metadata.index("test-2")).build())
            .build();
        RoutingAllocation testAllocation= new RoutingAllocation(new AllocationDeciders(Collections.emptyList()),
            new RoutingNodes(clusterState, false),
            clusterState,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime());

        Set<String> batchesToAssign = testGatewayAllocator.callCreateAndUpdateBatches(testAllocation, true);
        assertEquals(2, batchesToAssign.size());

        Map<String, GatewayAllocator.ShardsBatch> batchIdToStartedShardBatch = testGatewayAllocator.getBatchIdToStartedShardBatch();
        assertEquals(batchIdToStartedShardBatch.keySet(), batchesToAssign);

        Set<ShardId> test = clusterState.routingTable().index("test").getShards().values().stream().map(IndexShardRoutingTable::getShardId).collect(Collectors.toSet());
        Set<ShardId> test2 = clusterState.routingTable().index("test-2").getShards().values().stream().map(IndexShardRoutingTable::getShardId).collect(Collectors.toSet());
        test.addAll(test2);
        Set<ShardId> collect = batchIdToStartedShardBatch.values().stream().map(GatewayAllocator.ShardsBatch::getBatchedShards).
            flatMap(Set::stream).collect(Collectors.toSet());
        assertEquals(collect, test);

        // now for replicas
        batchesToAssign = testGatewayAllocator.callCreateAndUpdateBatches(testAllocation, false);
        assertEquals(2, batchesToAssign.size());
        Map<String, GatewayAllocator.ShardsBatch> batchIdToStoreShardBatch = testGatewayAllocator.getBatchIdToStoreShardBatch();
        assertEquals(batchIdToStoreShardBatch.keySet(), batchesToAssign);
        collect = batchIdToStoreShardBatch.values().stream().map(GatewayAllocator.ShardsBatch::getBatchedShards).
            flatMap(Set::stream).collect(Collectors.toSet());
        assertEquals(collect, test);

        Set<ShardRouting> shardRoutings = batchIdToStartedShardBatch.values().stream().map(GatewayAllocator.ShardsBatch::getBatchedShardRoutings).
            flatMap(Set::stream).collect(Collectors.toSet());
        shardRoutings.forEach(shardRouting -> assertTrue(shardRouting.unassigned() && shardRouting.primary()==true));

        Set<ShardRouting> replicasShardRoutings = batchIdToStoreShardBatch.values().stream().map(GatewayAllocator.ShardsBatch::getBatchedShardRoutings).
            flatMap(Set::stream).collect(Collectors.toSet());

        replicasShardRoutings.forEach(shardRouting -> assertTrue(shardRouting.unassigned() && shardRouting.primary()==false));
    }
}
