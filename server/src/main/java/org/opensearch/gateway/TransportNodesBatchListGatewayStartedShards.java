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

import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionType;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.gateway.TransportNodesGatewayStartedShardHelper.NodeGatewayStartedShardInfo;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.*;

/**
 * This transport action is used to fetch  all unassigned shard version from each node during primary allocation in {@link GatewayAllocator}.
 * We use this to find out which node holds the latest shard version and which of them used to be a primary in order to allocate
 * shards after node or cluster restarts.
 *
 * @opensearch.internal
 */
public class TransportNodesBatchListGatewayStartedShards extends TransportNodesAction<
    TransportNodesBatchListGatewayStartedShards.Request,
    TransportNodesBatchListGatewayStartedShards.NodesGatewayStartedShardsBatch,
    TransportNodesBatchListGatewayStartedShards.NodeRequest,
    TransportNodesBatchListGatewayStartedShards.NodeGatewayStartedShardsBatch>
    implements
        AsyncShardsFetchPerNode.Lister<
            TransportNodesBatchListGatewayStartedShards.NodesGatewayStartedShardsBatch,
            TransportNodesBatchListGatewayStartedShards.NodeGatewayStartedShardsBatch> {

    public static final String ACTION_NAME = "internal:gateway/local/bulk_started_shards";
    public static final ActionType<NodesGatewayStartedShardsBatch> TYPE = new ActionType<>(ACTION_NAME, NodesGatewayStartedShardsBatch::new);

    private final Settings settings;
    private final NodeEnvironment nodeEnv;
    private final IndicesService indicesService;
    private final NamedXContentRegistry namedXContentRegistry;

    @Inject
    public TransportNodesBatchListGatewayStartedShards(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        NodeEnvironment env,
        IndicesService indicesService,
        NamedXContentRegistry namedXContentRegistry
    ) {
        super(
            ACTION_NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            Request::new,
            NodeRequest::new,
            ThreadPool.Names.FETCH_SHARD_STARTED,
            NodeGatewayStartedShardsBatch.class
        );
        this.settings = settings;
        this.nodeEnv = env;
        this.indicesService = indicesService;
        this.namedXContentRegistry = namedXContentRegistry;
    }

    @Override
    public void list(
        DiscoveryNode[] nodes,
        Map<ShardId, String> shardIdsWithCustomDataPath,
        ActionListener<NodesGatewayStartedShardsBatch> listener
    ) {
        execute(new Request(nodes, shardIdsWithCustomDataPath), listener);
    }

    @Override
    protected NodeRequest newNodeRequest(Request request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeGatewayStartedShardsBatch newNodeResponse(StreamInput in) throws IOException {
        return new NodeGatewayStartedShardsBatch(in);
    }

    @Override
    protected NodesGatewayStartedShardsBatch newResponse(
        Request request,
        List<NodeGatewayStartedShardsBatch> responses,
        List<FailedNodeException> failures
    ) {
        return new NodesGatewayStartedShardsBatch(clusterService.getClusterName(), responses, failures);
    }

    /**
     * This function is similar to nodeoperation method of {@link TransportNodesListGatewayStartedShards} we loop over
     * the shards here to fetch the shard result in bulk.
     *
     * @param request
     * @return NodeGatewayStartedShardsBatch
     */
    @Override
    protected NodeGatewayStartedShardsBatch nodeOperation(NodeRequest request) {
        Map<ShardId, NodeGatewayStartedShardInfo> shardsOnNode = new HashMap<>();
        for (Map.Entry<ShardId, String> shardToCustomDataPathEntry : request.shardIdsWithCustomDataPath.entrySet()) {
            final ShardId shardId = shardToCustomDataPathEntry.getKey();
            try {
                final NodeGatewayStartedShardInfo nodeGatewayStartedShardInfo = TransportNodesGatewayStartedShardHelper
                    .getShardInfoOnLocalNode(
                        logger,
                        shardId,
                        namedXContentRegistry,
                        nodeEnv,
                        indicesService,
                        shardToCustomDataPathEntry.getValue(),
                        settings,
                        clusterService
                    );
                shardsOnNode.put(shardId, nodeGatewayStartedShardInfo);
            } catch (Exception e) {
                Exception shardInfoFetchException = new OpenSearchException("failed to load started shards", e);
                shardsOnNode.put(shardId, new NodeGatewayStartedShardInfo(null, false, null, shardInfoFetchException));
            }
        }
        return new NodeGatewayStartedShardsBatch(clusterService.localNode(), shardsOnNode);
    }

    /**
     * This is used in constructing the request for making the transport request to set of other node.
     * Refer {@link TransportNodesAction} class start method.
     *
     * @opensearch.internal
     */
    public static class Request extends BaseNodesRequest<Request> {
        private final Map<ShardId, String> shardIdsWithCustomDataPath;

        public Request(StreamInput in) throws IOException {
            super(in);
            shardIdsWithCustomDataPath = in.readMap(ShardId::new, StreamInput::readString);
        }

        public Request(DiscoveryNode[] nodes, Map<ShardId, String> shardIdStringMap) {
            super(nodes);
            this.shardIdsWithCustomDataPath = Objects.requireNonNull(shardIdStringMap);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(shardIdsWithCustomDataPath, (o, k) -> k.writeTo(o), StreamOutput::writeString);
        }

        public Map<ShardId, String> getShardIdsMap() {
            return shardIdsWithCustomDataPath;
        }
    }

    /**
     * Responses received from set of other nodes is clubbed into this class and sent back to the caller
     * of this transport request. Refer {@link TransportNodesAction}
     *
     * @opensearch.internal
     */
    public static class NodesGatewayStartedShardsBatch extends BaseNodesResponse<NodeGatewayStartedShardsBatch> {

        public NodesGatewayStartedShardsBatch(StreamInput in) throws IOException {
            super(in);
        }

        public NodesGatewayStartedShardsBatch(
            ClusterName clusterName,
            List<NodeGatewayStartedShardsBatch> nodes,
            List<FailedNodeException> failures
        ) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeGatewayStartedShardsBatch> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(NodeGatewayStartedShardsBatch::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeGatewayStartedShardsBatch> nodes) throws IOException {
            out.writeList(nodes);
        }
    }

    /**
     * NodeRequest class is for deserializing the  request received by this node from other node for this transport action.
     * This is used in {@link TransportNodesAction}
     *
     * @opensearch.internal
     */
    public static class NodeRequest extends TransportRequest {
        private final Map<ShardId, String> shardIdsWithCustomDataPath;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            shardIdsWithCustomDataPath = in.readMap(ShardId::new, StreamInput::readString);
        }

        public NodeRequest(Request request) {
            this.shardIdsWithCustomDataPath = Objects.requireNonNull(request.getShardIdsMap());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(shardIdsWithCustomDataPath, (o, k) -> k.writeTo(o), StreamOutput::writeString);
        }

    }

    /**
     * This is the response from a single node, this is used in {@link NodesGatewayStartedShardsBatch} for creating
     * node to its response mapping for this transport request.
     * Refer {@link TransportNodesAction} start method
     *
     * @opensearch.internal
     */
    public static class NodeGatewayStartedShardsBatch extends BaseNodeResponse {
        private final Map<ShardId, NodeGatewayStartedShardInfo> nodeGatewayStartedShardsBatch;

        public Map<ShardId, NodeGatewayStartedShardInfo> getNodeGatewayStartedShardsBatch() {
            return nodeGatewayStartedShardsBatch;
        }

        public NodeGatewayStartedShardsBatch(StreamInput in) throws IOException {
            super(in);
            this.nodeGatewayStartedShardsBatch = in.readMap(ShardId::new, NodeGatewayStartedShardInfo::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(nodeGatewayStartedShardsBatch, (o, k) -> k.writeTo(o), (o, v) -> v.writeTo(o));
        }

        public NodeGatewayStartedShardsBatch(DiscoveryNode node, Map<ShardId, NodeGatewayStartedShardInfo> nodeGatewayStartedShardsBatch) {
            super(node);
            this.nodeGatewayStartedShardsBatch = nodeGatewayStartedShardsBatch;
        }
    }
}
