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

import java.util.Objects;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.tools.sequence.internal.DistributedSequenceImpl;

/**
 * The single entry point to generate and compute distributed sequences.
 * There is only one region used to store the sequences, and it's only initialized once, both on
 * the client and server side.
 */
public final class DistributedSequenceFactory {
  private static Region<String, Long> sequencesRegion;

  /**
   * Initializes the distributed sequences by executing the following steps:
   *  - Execute a function on all servers reachable by the default {@link Pool} from the {@link ClientCache}.
   *  - Creates a local PROXY region, which is used as the local backing store for the actual region created on the server side (PARTITION by default) to compute the sequences.
   *
   * @param clientCache The fully initialized {@link ClientCache} that will be used by this distributed sequence.
   * @throws IllegalArgumentException If the {@link ClientCache} doesn't have a DEFAULT pool created.
   */
  public static void initialize(ClientCache clientCache) throws IllegalArgumentException {
    Objects.requireNonNull(clientCache, "ClientCache can not be null");
    if (clientCache.getDefaultPool() == null) throw new IllegalArgumentException("The default pool could not be found.");
    sequencesRegion = DistributedSequenceImpl.initialize(clientCache, RegionType.PARTITION.getDataPolicy(), clientCache.getDefaultPool());
  }

  /**
   * Initializes the distributed sequences by executing the following steps:
   *  - Execute a function on all servers reachable by the default {@link Pool} from the {@link ClientCache}.
   *  - Creates a local PROXY region, which is used as the local backing store for the actual region created on the server side (according to RegionType) to compute the sequences.
   *
   * @param clientCache The fully initialized {@link ClientCache} that will be used by this distributed sequence.
   * @throws IllegalArgumentException If the {@link ClientCache} doesn't have a DEFAULT pool created.
   */
  public static void initialize(ClientCache clientCache, RegionType regionType) throws IllegalArgumentException {
    Objects.requireNonNull(clientCache, "ClientCache can not be null");
    Objects.requireNonNull(regionType, "RegionType can not be null");
    if (clientCache.getDefaultPool() == null) throw new IllegalArgumentException("The default pool could not be found.");
    sequencesRegion = DistributedSequenceImpl.initialize(clientCache, regionType.getDataPolicy(), clientCache.getDefaultPool());
  }

  /**
   * Initializes or retrieves the distributed sequences by executing the following steps:
   *  - Execute a function on all servers reachable by the specified client {@link Pool}.
   *  - Creates a local PROXY region, which is used as the local backing store for the actual region created on the server side (according to RegionType) to compute the sequences.
   *
   * @param clientCache The fully initialized {@link ClientCache} that will be used by this distributed sequence.
   * @param regionType The type (REPLICATE or PARTITION) of the region that will be used by the distributed sequences.
   * @param poolName The name of the pool that will be used to initialize the region on the server side, the pool must exist and be fully initialized.
   * @throws IllegalArgumentException If the {@link ClientCache} doesn't have the specified named {@link Pool} created.
   */
  public static void initialize(ClientCache clientCache, RegionType regionType, String poolName) throws IllegalArgumentException {
    Objects.requireNonNull(clientCache, "ClientCache can not be null");
    Objects.requireNonNull(regionType, "RegionType can not be null");
    if (PoolManager.find(poolName) == null) throw new IllegalArgumentException("The specified pool (" + poolName + ") could not be found.");
    sequencesRegion = DistributedSequenceImpl.initialize(clientCache, regionType.getDataPolicy(), PoolManager.find(poolName));
  }

  /**
   * Constructs a distributed sequence to be used by client applications.
   *
   * @param sequenceId The id of the distributed sequence to build.
   * @return The distributed sequence wrapper.
   */
  public static DistributedSequence getSequence(String sequenceId) {
    return new DistributedSequenceImpl(sequenceId, sequencesRegion);
  }
}
