/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.opensearch.core.index.shard.ShardId;

import java.util.HashSet;
import java.util.Set;

public class BatchTestUtil {
    public static Set<ShardId> setUpShards(int numberOfShards) {
        Set<ShardId> shards = new HashSet<>();
        for (int shardNumber = 0; shardNumber < numberOfShards; shardNumber++) {
            ShardId shardId = new ShardId("test", "_na_", shardNumber);
            shards.add(shardId);
        }
        return shards;
    }
}
