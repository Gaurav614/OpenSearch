/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.discovery.Discovery;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.seqno.RetentionLease;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadata;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadataBatch;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadataBatch;
import org.opensearch.snapshots.SnapshotShardSizeInfo;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.unmodifiableMap;
import static org.hamcrest.Matchers.equalTo;

public class ReplicaShardBatchAllocatorTest extends OpenSearchAllocationTestCase {
    private static final org.apache.lucene.util.Version MIN_SUPPORTED_LUCENE_VERSION = org.opensearch.Version.CURRENT
        .minimumIndexCompatibilityVersion().luceneVersion;
    private final ShardId shardId = new ShardId("test", "_na_", 0);
    private final DiscoveryNode node1 = newNode("node1");
    private final DiscoveryNode node2 = newNode("node2");
    private final DiscoveryNode node3 = newNode("node3");

    private TestBatchAllocator testBatchAllocator;

    @Before
    public void buildTestAllocator() {
        this.testBatchAllocator = new TestBatchAllocator();
    }

    private void allocateAllUnassigned(final RoutingAllocation allocation) {
        final RoutingNodes.UnassignedShards.UnassignedIterator iterator = allocation.routingNodes().unassigned().iterator();
        while (iterator.hasNext()) {
            testBatchAllocator.allocateUnassigned(iterator.next(), allocation, iterator);
        }
    }

    private void allocateAllUnassignedBatch(final RoutingAllocation allocation) {
        final RoutingNodes.UnassignedShards.UnassignedIterator iterator = allocation.routingNodes().unassigned().iterator();
        Set<ShardRouting> shardToBatch = new HashSet<>();
        while (iterator.hasNext()) {
            shardToBatch.add(iterator.next());
        }
        testBatchAllocator.allocateUnassignedBatch(shardToBatch, allocation);
    }

    /**
     * Verifies that when we are still fetching data in an async manner, the replica shard moves to ignore unassigned.
     */
    public void testNoAsyncFetchData() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        testBatchAllocator.clean();
        allocateAllUnassignedBatch(allocation);
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
    }

    /**
     * Verifies that on index creation, we don't go and fetch data, but keep the replica shard unassigned to let
     * the shard allocator to allocate it. There isn't a copy around to find anyhow.
     */
    public void testNoAsyncFetchOnIndexCreation() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(
            yesAllocationDeciders(),
            Settings.EMPTY,
            UnassignedInfo.Reason.INDEX_CREATED
        );
        testBatchAllocator.clean();
        allocateAllUnassignedBatch(allocation);
        assertThat(testBatchAllocator.getFetchDataCalledAndClean(), equalTo(false));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(1));
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.UNASSIGNED).get(0).shardId(), equalTo(shardId));
    }

    /**
     * Verifies that for anything but index creation, fetch data ends up being called, since we need to go and try
     * and find a better copy for the shard.
     */
    public void testAsyncFetchOnAnythingButIndexCreation() {
        UnassignedInfo.Reason reason = RandomPicks.randomFrom(
            random(),
            EnumSet.complementOf(EnumSet.of(UnassignedInfo.Reason.INDEX_CREATED))
        );
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders(), Settings.EMPTY, reason);
        testBatchAllocator.clean();
        allocateAllUnassignedBatch(allocation);
        assertThat("failed with reason " + reason, testBatchAllocator.getFetchDataCalledAndClean(), equalTo(true));
    }

    /**
     * Verifies that when there is a full match (syncId and files) we allocate it to matching node.
     */
    public void testSimpleFullMatchAllocation() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        DiscoveryNode nodeToMatch = randomBoolean() ? node2 : node3;
        testBatchAllocator.addData(node1, "MATCH", null, new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
            .addData(nodeToMatch, "MATCH", null, new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassignedBatch(allocation);
        assertThat(allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(
            allocation.routingNodes().shardsWithState(ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo(nodeToMatch.getId())
        );
    }

    public void testProcessExistingRecoveries() {
    }

    public void testMakeAllocationDecision() {
    }

    public void testHasInitiatedFetching() {
    }

    private RoutingAllocation onePrimaryOnNode1And1Replica(AllocationDeciders deciders) {
        return onePrimaryOnNode1And1Replica(deciders, Settings.EMPTY, UnassignedInfo.Reason.CLUSTER_RECOVERED);
    }

    private RoutingAllocation onePrimaryOnNode1And1Replica(AllocationDeciders deciders, Settings settings, UnassignedInfo.Reason reason) {
        ShardRouting primaryShard = TestShardRouting.newShardRouting(shardId, node1.getId(), true, ShardRoutingState.STARTED);
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(shardId.getIndexName())
            .settings(settings(Version.CURRENT).put(settings))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .putInSyncAllocationIds(0, Sets.newHashSet(primaryShard.allocationId().getId()));
        Metadata metadata = Metadata.builder().put(indexMetadata).build();
        // mark shard as delayed if reason is NODE_LEFT
        boolean delayed = reason == UnassignedInfo.Reason.NODE_LEFT
            && UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.get(settings).nanos() > 0;
        int failedAllocations = reason == UnassignedInfo.Reason.ALLOCATION_FAILED ? 1 : 0;
        RoutingTable routingTable = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(shardId.getIndex())
                    .addIndexShard(
                        new IndexShardRoutingTable.Builder(shardId).addShard(primaryShard)
                            .addShard(
                                ShardRouting.newUnassigned(
                                    shardId,
                                    false,
                                    RecoverySource.PeerRecoverySource.INSTANCE,
                                    new UnassignedInfo(
                                        reason,
                                        null,
                                        null,
                                        failedAllocations,
                                        System.nanoTime(),
                                        System.currentTimeMillis(),
                                        delayed,
                                        UnassignedInfo.AllocationStatus.NO_ATTEMPT,
                                        Collections.emptySet()
                                    )
                                )
                            )
                            .build()
                    )
            )
            .build();
        ClusterState state = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3))
            .build();
        return new RoutingAllocation(
            deciders,
            new RoutingNodes(state, false),
            state,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );
    }

    private RoutingAllocation onePrimaryOnNode1And1ReplicaRecovering(AllocationDeciders deciders, UnassignedInfo unassignedInfo) {
        ShardRouting primaryShard = TestShardRouting.newShardRouting(shardId, node1.getId(), true, ShardRoutingState.STARTED);
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(shardId.getIndexName())
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
                    .putInSyncAllocationIds(0, Sets.newHashSet(primaryShard.allocationId().getId()))
            )
            .build();
        RoutingTable routingTable = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(shardId.getIndex())
                    .addIndexShard(
                        new IndexShardRoutingTable.Builder(shardId).addShard(primaryShard)
                            .addShard(
                                TestShardRouting.newShardRouting(
                                    shardId,
                                    node2.getId(),
                                    null,
                                    false,
                                    ShardRoutingState.INITIALIZING,
                                    unassignedInfo
                                )
                            )
                            .build()
                    )
            )
            .build();
        ClusterState state = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3))
            .build();
        return new RoutingAllocation(
            deciders,
            new RoutingNodes(state, false),
            state,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );
    }

    private RoutingAllocation onePrimaryOnNode1And1ReplicaRecovering(AllocationDeciders deciders) {
        return onePrimaryOnNode1And1ReplicaRecovering(deciders, new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, null));
    }

    static String randomSyncId() {
        return randomFrom("MATCH", "NOT_MATCH", null);
    }

    class TestBatchAllocator extends ReplicaShardBatchAllocator {
        private Map<DiscoveryNode, TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadata> data = null;
        private AtomicBoolean fetchDataCalled = new AtomicBoolean(false);

        public void clean() {
            data = null;
        }

        public boolean getFetchDataCalledAndClean() {
            return fetchDataCalled.getAndSet(false);
        }

        public TestBatchAllocator addData(DiscoveryNode node, String syncId, @Nullable Exception storeFileFetchException, StoreFileMetadata ...files) {
            return addData(node, Collections.emptyList(), syncId, storeFileFetchException, files);
        }

        public TestBatchAllocator addData(
            DiscoveryNode node,
            List<RetentionLease> peerRecoveryRetentionLeases,
            String syncId,
            @Nullable Exception storeFileFetchException,
            StoreFileMetadata ...files) {
            if (data == null) {
                data = new HashMap<>();
            }
            Map<String, StoreFileMetadata> filesAsMap = new HashMap<>();
            for (StoreFileMetadata file : files) {
                filesAsMap.put(file.name(), file);
            }
            Map<String, String> commitData = new HashMap<>();
            if (syncId != null) {
                commitData.put(Engine.SYNC_COMMIT_ID, syncId);
            }
            data.put(
                node,
                new TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadata(
                    new TransportNodesListShardStoreMetadataBatch.StoreFilesMetadata(
                        shardId,
                        new Store.MetadataSnapshot(unmodifiableMap(filesAsMap), unmodifiableMap(commitData), randomInt()),
                        peerRecoveryRetentionLeases
                    ),
                    storeFileFetchException
                )
            );
            return this;
        }

        @Override
        protected AsyncBatchShardFetch.FetchResult<NodeStoreFilesMetadataBatch> fetchData(Set<ShardRouting> shardEligibleForFetch, Set<ShardRouting> inEligibleShards, RoutingAllocation allocation) {
            fetchDataCalled.set(true);
            Map<DiscoveryNode, NodeStoreFilesMetadataBatch> tData = null;
            if (data != null) {
                tData = new HashMap<>();
                for (Map.Entry<DiscoveryNode, TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadata> entry : data.entrySet()) {
                    Map<ShardId, TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadata> shardData = Map.of(
                        shardId,
                        entry.getValue()
                    );
                    tData.put(
                        entry.getKey(),
                        new TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadataBatch(entry.getKey(), shardData)
                    );
                }
            }
            return new AsyncBatchShardFetch.FetchResult<>(tData, Collections.<ShardId, Set<String>>emptyMap());
        }

        @Override
        protected boolean hasInitiatedFetching(ShardRouting shard) {
            return fetchDataCalled.get();
        }
    }
}
