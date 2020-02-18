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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.tools.sequence.internal.SequenceArgs;
import org.apache.geode.tools.sequence.internal.SequenceFunction;
import org.apache.geode.tools.sequence.internal.SetUpFunction;

/**
 * The single entry point to the geode-sequence-generator tool.
 */
public class GeodeSequence {
  enum RegionType { REPLICATE, PARTITION }
  private static Region<String, Long> sequencesRegion;

  static DataPolicy regionTypeToDataPolicy(RegionType regionType) {
    switch (regionType) {
      case PARTITION: return DataPolicy.PERSISTENT_PARTITION;
      case REPLICATE: return DataPolicy.PERSISTENT_REPLICATE;
      default: throw new IllegalArgumentException("Unknown RegionType : " + regionType);
    }
  }

  /**
   * Initializes the tool by executing the {@link SetUpFunction} on all servers reachable by the
   * specified client {@link Pool}.
   *
   * @param clientCache The fully initialized {@link ClientCache} used to create the region and find the pool.
   * @param poolName The name of the client {@link Pool} used to connect to the Geode cluster. The default one will be used if the pool can not be found.
   * @param regionType The type (REPLICATE or PARTITION) for the backing region used to generate sequences.
   */
  @SuppressWarnings("unchecked")
  public static void initialize(ClientCache clientCache, String poolName, RegionType regionType) {
    Objects.requireNonNull(regionType);
    Objects.requireNonNull(clientCache);

    // Execute the SetUpFunction on the specified pool, or the default one if none was found.
    Pool clientPool = PoolManager.find(poolName);
    if (clientPool == null) clientPool = clientCache.getDefaultPool();
    DataPolicy backingRegionType = regionTypeToDataPolicy(regionType);
    FunctionService
        .onServers(clientPool)
        .setArguments(backingRegionType)
        .execute(SetUpFunction.FUNCTION_ID);

    // Get or create the local region.
    sequencesRegion = clientCache.getRegion(SetUpFunction.SEQUENCES_REGION_ID);
    try {
      if (sequencesRegion == null) {
        sequencesRegion = clientCache
            // Don't store anything locally.
            .<String, Long>createClientRegionFactory(ClientRegionShortcut.PROXY)
            .create(SetUpFunction.SEQUENCES_REGION_ID);
      }
    } catch (RegionExistsException ignore) {
      // Another thread already created the region.
      sequencesRegion = clientCache.getRegion(SetUpFunction.SEQUENCES_REGION_ID);
    }
  }

  /**
   * Returns the next batch of generated sequences.
   *
   * @param clientCache The {@link ClientCache} used to connect to the cluster.
   * @param sequenceId The id of the sequence for which the next batch is requested.
   * @param batchSize The size of the batch requested (how many sequences to generate).
   * @return The list of sequences generated.
   */
  @SuppressWarnings("unchecked")
  public static List<Long> nextBatch(ClientCache clientCache, String sequenceId, Integer batchSize) {
    Objects.requireNonNull(batchSize);
    Objects.requireNonNull(sequenceId);
    Objects.requireNonNull(clientCache);

    List<Long> aggregateResults = new ArrayList<>();
    Collection<List<Long>> allResults = (Collection<List<Long>>) FunctionService
        .onRegion(sequencesRegion)
        .setArguments(new SequenceArgs(sequenceId, batchSize))
        .withFilter(Collections.singleton(sequenceId))
        .execute(SequenceFunction.FUNCTION_ID)
        .getResult();
    allResults.forEach(aggregateResults::addAll);

    return aggregateResults;
  }
}
