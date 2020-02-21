/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.tools.sequence;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.tools.sequence.internal.DistributedSequenceImpl;

/**
 * The single entry point to generate and operate distributed sequences.
 */
public final class DistributedSequenceFactory {

  /**
   * Initializes or retrieves the distributed sequences by executing the following steps:
   *  - Execute a function on all servers reachable by the default {@link Pool} from the {@link ClientCache}.
   *  - Creates a local PROXY region, which is used as the local backing store for the actual region created on the server side.
   *
   * @param sequenceId The name of the distributed sequence.
   * @param regionType The type (REPLICATE or PARTITION) of the region that will be used by the distributed sequence.
   * @param clientCache The fully initialized {@link ClientCache} that will be used by this distributed sequence.
   */
  public static DistributedSequence getSequence(String sequenceId, RegionType regionType, ClientCache clientCache) {
    return new DistributedSequenceImpl(sequenceId, regionType, clientCache);
  }

  /**
   * Initializes or retrieves the distributed sequences by executing the following steps:
   *  - Execute a function on all servers reachable by the specified client {@link Pool}.
   *  - Creates a local PROXY region, which is used as the local backing store for the actual region created on the server side.
   *
   * @param sequenceId The name of the distributed sequence.
   * @param regionType The type (REPLICATE or PARTITION) of the region that will be used by the distributed sequence.
   * @param poolName The name of the pool that will be used to initialize the region on the server side, the pool must exist and be fully initialized.
   * @param clientCache The fully initialized {@link ClientCache} that will be used by this distributed sequence.
   */
  public static DistributedSequence getSequence(String sequenceId, RegionType regionType, String poolName, ClientCache clientCache) {
    return new DistributedSequenceImpl(sequenceId, regionType, poolName, clientCache);
  }
}
