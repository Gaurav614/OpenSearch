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
import org.opensearch.common.Nullable;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.gateway.TransportNodesGatewayStartedShardHelper.NodeGatewayStartedShardInfo;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * This transport action is used to fetch the shard version from each node during primary allocation in {@link GatewayAllocator}.
 * We use this to find out which node holds the latest shard version and which of them used to be a primary in order to allocate
 * shards after node or cluster restarts.
 *
 * @opensearch.internal
 */
public class TransportNodesListGatewayStartedShards extends TransportNodesAction<
    TransportNodesListGatewayStartedShards.Request,
    TransportNodesListGatewayStartedShards.NodesGatewayStartedShards,
    TransportNodesListGatewayStartedShards.NodeRequest,
    TransportNodesListGatewayStartedShards.NodeGatewayStartedShards>
    implements
        AsyncShardFetch.Lister<
            TransportNodesListGatewayStartedShards.NodesGatewayStartedShards,
            TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> {

    public static final String ACTION_NAME = "internal:gateway/local/started_shards";
    public static final ActionType<NodesGatewayStartedShards> TYPE = new ActionType<>(ACTION_NAME, NodesGatewayStartedShards::new);

    private final Settings settings;
    private final NodeEnvironment nodeEnv;
    private final IndicesService indicesService;
    private final NamedXContentRegistry namedXContentRegistry;

    @Inject
    public TransportNodesListGatewayStartedShards(
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
            NodeGatewayStartedShards.class
        );
        this.settings = settings;
        this.nodeEnv = env;
        this.indicesService = indicesService;
        this.namedXContentRegistry = namedXContentRegistry;
    }

    @Override
    public void list(ShardId shardId, String customDataPath, DiscoveryNode[] nodes, ActionListener<NodesGatewayStartedShards> listener) {
        execute(new Request(shardId, customDataPath, nodes), listener);
    }

    @Override
    protected NodeRequest newNodeRequest(Request request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeGatewayStartedShards newNodeResponse(StreamInput in) throws IOException {
        return new NodeGatewayStartedShards(in);
    }

    @Override
    protected NodesGatewayStartedShards newResponse(
        Request request,
        List<NodeGatewayStartedShards> responses,
        List<FailedNodeException> failures
    ) {
        return new NodesGatewayStartedShards(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeGatewayStartedShards nodeOperation(NodeRequest request) {
        try {
            final ShardId shardId = request.getShardId();
            final NodeGatewayStartedShardInfo nodeGatewayStartedShardInfo = TransportNodesGatewayStartedShardHelper.getShardInfoOnLocalNode(
                logger,
                shardId,
                namedXContentRegistry,
                nodeEnv,
                indicesService,
                request.getCustomDataPath(),
                settings,
                clusterService
            );
            return new NodeGatewayStartedShards(clusterService.localNode(), nodeGatewayStartedShardInfo);
        } catch (Exception e) {
            throw new OpenSearchException("failed to load started shards", e);
        }
    }

    /**
     * The nodes request.
     *
     * @opensearch.internal
     */
    public static class Request extends BaseNodesRequest<Request> {

        private final ShardId shardId;
        @Nullable
        private final String customDataPath;

        public Request(StreamInput in) throws IOException {
            super(in);
            shardId = new ShardId(in);
            customDataPath = in.readString();
        }

        public Request(ShardId shardId, String customDataPath, DiscoveryNode[] nodes) {
            super(nodes);
            this.shardId = Objects.requireNonNull(shardId);
            this.customDataPath = Objects.requireNonNull(customDataPath);
        }

        public ShardId shardId() {
            return shardId;
        }

        /**
         * Returns the custom data path that is used to look up information for this shard.
         * Returns an empty string if no custom data path is used for this index.
         * Returns null if custom data path information is not available (due to BWC).
         */
        @Nullable
        public String getCustomDataPath() {
            return customDataPath;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeString(customDataPath);
        }
    }

    /**
     *  The nodes response.
     *
     * @opensearch.internal
     */
    public static class NodesGatewayStartedShards extends BaseNodesResponse<NodeGatewayStartedShards> {

        public NodesGatewayStartedShards(StreamInput in) throws IOException {
            super(in);
        }

        public NodesGatewayStartedShards(
            ClusterName clusterName,
            List<NodeGatewayStartedShards> nodes,
            List<FailedNodeException> failures
        ) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeGatewayStartedShards> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(NodeGatewayStartedShards::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeGatewayStartedShards> nodes) throws IOException {
            out.writeList(nodes);
        }
    }

    /**
     * The request.
     *
     * @opensearch.internal
     */
    public static class NodeRequest extends TransportRequest {

        private final ShardId shardId;
        @Nullable
        private final String customDataPath;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            shardId = new ShardId(in);
            customDataPath = in.readString();
        }

        public NodeRequest(Request request) {
            this.shardId = Objects.requireNonNull(request.shardId());
            this.customDataPath = Objects.requireNonNull(request.getCustomDataPath());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            assert customDataPath != null;
            out.writeString(customDataPath);
        }

        public ShardId getShardId() {
            return shardId;
        }

        /**
         * Returns the custom data path that is used to look up information for this shard.
         * Returns an empty string if no custom data path is used for this index.
         * Returns null if custom data path information is not available (due to BWC).
         */
        @Nullable
        public String getCustomDataPath() {
            return customDataPath;
        }
    }

    /**
     * The response.
     *
     * @opensearch.internal
     */
    public static class NodeGatewayStartedShards extends BaseNodeResponse {
        private final NodeGatewayStartedShardInfo nodeGatewayStartedShardInfo;

        public NodeGatewayStartedShards(StreamInput in) throws IOException {
            super(in);
            nodeGatewayStartedShardInfo = new NodeGatewayStartedShardInfo(in);
        }

        public NodeGatewayStartedShards(DiscoveryNode localNode, NodeGatewayStartedShardInfo nodeGatewayStartedShardInfo) {
            super(localNode);
            this.nodeGatewayStartedShardInfo = nodeGatewayStartedShardInfo;
        }

        public NodeGatewayStartedShards(
            DiscoveryNode node,
            String allocationId,
            boolean primary,
            ReplicationCheckpoint replicationCheckpoint
        ) {
            this(node, allocationId, primary, replicationCheckpoint, null);
        }

        public NodeGatewayStartedShards(
            DiscoveryNode node,
            String allocationId,
            boolean primary,
            ReplicationCheckpoint replicationCheckpoint,
            Exception storeException
        ) {
            super(node);
            nodeGatewayStartedShardInfo = new NodeGatewayStartedShardInfo(allocationId, primary, replicationCheckpoint, storeException);

        }

        public String allocationId() {
            return this.nodeGatewayStartedShardInfo.allocationId();
        }

        public boolean primary() {
            return this.nodeGatewayStartedShardInfo.primary();
        }

        public ReplicationCheckpoint replicationCheckpoint() {
            return this.nodeGatewayStartedShardInfo.replicationCheckpoint();
        }

        public Exception storeException() {
            return this.nodeGatewayStartedShardInfo.storeException();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            this.nodeGatewayStartedShardInfo.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            return this.nodeGatewayStartedShardInfo.equals(o);
        }

        @Override
        public int hashCode() {
            return this.nodeGatewayStartedShardInfo.hashCode();
        }

        @Override
        public String toString() {
            return this.nodeGatewayStartedShardInfo.toString();
        }
    }
}
