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

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.management.internal.security.ResourcePermissions;
import org.apache.geode.security.ResourcePermission;

/**
 * Function used to get the next batch of unique sequence ids.
 * The actual internal region hosting the sequences is created by {@link SetUpFunction}.
 */
public class SequenceFunction implements Function<SequenceArgs>, Declarable {
  public final static String FUNCTION_ID = "SequenceFunction";
  private final static  Object LOCK_SYNC = new Object();
  private final static String DISTRIBUTED_LOCK_SERVICE_PREFIX = "_sequenceLockService";

  @Override
  public String getId() {
    return FUNCTION_ID;
  }

  @Override
  public boolean isHA() {
    return false;
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean optimizeForWrite() {
    return true;
  }

  @Override
  public Collection<ResourcePermission> getRequiredPermissions(String regionName) {
    return Collections.singletonList(ResourcePermissions.DATA_WRITE);
  }

  @Override
  public Collection<ResourcePermission> getRequiredPermissions(String regionName, Object args) {
    return getRequiredPermissions(regionName);
  }

  /**
   * Obtains (or creates) the {@link DistributedLockService} associated to the sequences.
   *
   * @param cache Geode cache to use when obtaining or crating the {@link DistributedLockService}.
   * @param sequenceId Name of the sequence to use for building the {@link DistributedLockService}.
   * @return the {@link DistributedLockService} to use generating sequences.
   */
  DistributedLockService getDistributedLockService(Cache cache, String sequenceId) {
    DistributedLockService distributedLockService;
    String lockServiceName = sequenceId + DISTRIBUTED_LOCK_SERVICE_PREFIX;
    distributedLockService = DistributedLockService.getServiceNamed(lockServiceName);

    if (distributedLockService == null) {
      synchronized (LOCK_SYNC) {
        distributedLockService = DistributedLockService.getServiceNamed(lockServiceName);
        if (distributedLockService == null) {
            distributedLockService = DistributedLockService.create(lockServiceName, cache.getDistributedSystem());
        }
      }
    }

    return distributedLockService;
  }

  @Override
  public void execute(FunctionContext<SequenceArgs> functionContext) {
    Cache cache = functionContext.getCache();
    String sequenceId = functionContext.getArguments().getId();
    Integer batchSize = functionContext.getArguments().getBatchSize();

    // Should never happen as users must not invoke this function directly
    if (!(functionContext instanceof RegionFunctionContext)) {
      throw new FunctionException("This is a data aware function, and has to be called using FunctionService.onRegion.");
    }

    List<Long> sequences = new ArrayList<>(batchSize);
    RegionFunctionContext regionFunctionContext = (RegionFunctionContext) functionContext;
    DistributedLockService distributedLockService = getDistributedLockService(cache, sequenceId);
    boolean locked = distributedLockService.lock(sequenceId, -1, -1);

    // Proceed only if we got the lock.
    if (locked) {
      try {
        Region<String, Long> sequenceRegion = regionFunctionContext.getDataSet();
        long lastSequence = sequenceRegion.getOrDefault(sequenceId, 0L);
        for (int i = 0; i < batchSize; i++) sequences.add(lastSequence++);
        sequenceRegion.put(sequenceId, lastSequence);
      } finally {
        // Release the lock no matter what.
        distributedLockService.unlock(sequenceId);
      }
    } else {
      throw new FunctionException(String.format("Could no acquire Distributed Lock for sequence %s.", sequenceId));
    }

    functionContext.getResultSender().lastResult(sequences);
  }
}
