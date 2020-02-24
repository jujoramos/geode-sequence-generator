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

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.tools.sequence.DistributedSequence;

final public class DistributedSequenceImpl implements DistributedSequence {
  private final String sequenceId;
  private final Region<String, Long> sequencesRegion;
  private final static Object LOCK_SYNC = new Object();

  @SuppressWarnings("unchecked")
  public static Region<String, Long> initialize(ClientCache clientCache, DataPolicy dataPolicy, Pool clientPool) {
    Region<String, Long> region = clientCache.getRegion(SetUpFunction.DISTRIBUTED_SEQUENCES_REGION_ID);

    if (region == null) {
      // Prevent the same client initialization multiple times.
      synchronized (LOCK_SYNC) {
        region = clientCache.getRegion(SetUpFunction.DISTRIBUTED_SEQUENCES_REGION_ID);

        if (region == null) {
          // Execute the SetUpFunction.
          FunctionService
              .onServers(clientPool)
              .setArguments(dataPolicy)
              .execute(SetUpFunction.FUNCTION_ID);

          // Create the local region as PROXY.
          region = clientCache
              .<String, Long>createClientRegionFactory(ClientRegionShortcut.PROXY)
              .create(SetUpFunction.DISTRIBUTED_SEQUENCES_REGION_ID);
        }
      }
    }

    return region;
  }

  public DistributedSequenceImpl(String sequenceId, Region<String, Long> sequencesRegion) {
    this.sequenceId = sequenceId;
    this.sequencesRegion = sequencesRegion;
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
