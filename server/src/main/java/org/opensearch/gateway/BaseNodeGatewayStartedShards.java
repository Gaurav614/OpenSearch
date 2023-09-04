/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

public interface BaseNodeGatewayStartedShards {

    String allocationId();
    boolean primary();
    ReplicationCheckpoint replicationCheckpoint();
    Exception storeException();
}
