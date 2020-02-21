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
package org.apache.geode.tools.sequence.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.tools.sequence.DistributedSequence;
import org.apache.geode.tools.sequence.RegionType;

final public class DistributedSequenceImpl implements DistributedSequence {
  private final String sequenceId;
  private final ClientCache clientCache;
  private final Region<String, Long> sequencesRegion;

  @SuppressWarnings("unchecked")
  private Region<String, Long> initializeRegion(DataPolicy dataPolicy, Pool clientPool) {
    FunctionService
        .onServers(clientPool)
        .setArguments(dataPolicy)
        .execute(SetUpFunction.FUNCTION_ID);

    // Get or create the local region.
    Region<String, Long> region = clientCache.getRegion(SetUpFunction.DISTRIBUTED_SEQUENCES_REGION_ID);
    try {
      if (region == null) {
        region = clientCache
            // Don't store anything locally.
            .<String, Long>createClientRegionFactory(ClientRegionShortcut.PROXY)
            .create(SetUpFunction.DISTRIBUTED_SEQUENCES_REGION_ID);
      }
    } catch (RegionExistsException ignore) {
      // Another thread already created the region.
      region = clientCache.getRegion(SetUpFunction.DISTRIBUTED_SEQUENCES_REGION_ID);
    }

    return region;
  }

  public DistributedSequenceImpl(String sequenceId, RegionType regionType, ClientCache clientCache) {
    Objects.requireNonNull(regionType);
    Objects.requireNonNull(clientCache);
    if (StringUtils.isEmpty(sequenceId)) throw new IllegalArgumentException("SequenceId can not be null nor empty.");
    if (clientCache.getDefaultPool() == null) throw new IllegalArgumentException("The default pool could not be found.");

    this.sequenceId = sequenceId;
    this.clientCache = clientCache;
    this.sequencesRegion = initializeRegion(regionType.getDataPolicy(), clientCache.getDefaultPool());
  }

  public DistributedSequenceImpl(String sequenceId, RegionType regionType, String poolName, ClientCache clientCache) {
    Objects.requireNonNull(regionType);
    Objects.requireNonNull(clientCache);
    if (StringUtils.isEmpty(sequenceId)) throw new IllegalArgumentException("SequenceId can not be null nor empty.");
    if (PoolManager.find(poolName) == null) throw new IllegalArgumentException("The specified pool (" + poolName + ") could not be found.");

    this.sequenceId = sequenceId;
    this.clientCache = clientCache;
    this.sequencesRegion = initializeRegion(regionType.getDataPolicy(), PoolManager.find(poolName));
  }

  @Override
  @SuppressWarnings("unchecked")
  public Long get() {
    return ((List<Long>) FunctionService
        .onRegion(sequencesRegion)
        .setArguments(sequenceId)
        .withFilter(Collections.singleton(sequenceId))
        .execute(GetFunction.FUNCTION_ID)
        .getResult()).get(0);
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Long> nextBatch(Integer batchSize) {
    Objects.requireNonNull(batchSize);

    List<Long> aggregateResults = new ArrayList<>();
    Collection<List<Long>> allResults = (Collection<List<Long>>) FunctionService
        .onRegion(sequencesRegion)
        .setArguments(new NextBatchFunction.Args(sequenceId, batchSize))
        .withFilter(Collections.singleton(sequenceId))
        .execute(NextBatchFunction.FUNCTION_ID)
        .getResult();
    allResults.forEach(aggregateResults::addAll);

    return aggregateResults;
  }
}
