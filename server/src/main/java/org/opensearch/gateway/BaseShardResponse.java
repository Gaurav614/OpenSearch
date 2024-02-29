/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;

/**
 * Base response class for shard response.
 *
 * @opensearch.internal
 */
public abstract class BaseShardResponse extends TransportResponse {

    private Exception storeException;

    public BaseShardResponse(Exception storeException) {
        this.storeException = storeException;
    }

    public abstract boolean isEmpty();

    public Exception getException() {
        return storeException;
    }

    public BaseShardResponse(StreamInput in) throws IOException {
        super(in);
        if (in.readBoolean()) {
            storeException = in.readException();
        } else {
            storeException = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (storeException != null) {
            out.writeBoolean(true);
            out.writeException(storeException);
        } else {
            out.writeBoolean(false);
        }
    }
}
