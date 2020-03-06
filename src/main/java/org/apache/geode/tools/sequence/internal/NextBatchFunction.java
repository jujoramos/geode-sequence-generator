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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Cache;
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
 */
public class NextBatchFunction extends AbstractFunction implements Function<NextBatchFunction.Args> {
  final static String FUNCTION_ID = "DSequenceNext";

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

  @Override
  public void execute(FunctionContext<Args> functionContext) {
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

  public static class Args implements DataSerializable {
    private String id;
    private Integer batchSize;

    public String getId() {
      return id;
    }

    public Integer getBatchSize() {
      return batchSize;
    }

    @SuppressWarnings("unused")
    public Args() {
    }

    public Args(String id, Integer batchSize) {
      Objects.requireNonNull(id);
      Objects.requireNonNull(batchSize);

      this.id = id;
      this.batchSize = batchSize;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeString(id, out);
      DataSerializer.writeInteger(batchSize, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException {
      this.id = DataSerializer.readString(in);
      this.batchSize = DataSerializer.readInteger(in);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Args that = (Args) o;

      if (!id.equals(that.id)) {
        return false;
      }
      return batchSize.equals(that.batchSize);
    }

    @Override
    public int hashCode() {
      int result = id.hashCode();
      result = 31 * result + batchSize.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return "SequenceArguments{" +
          "name='" + id + '\'' +
          ", rangeSize=" + batchSize +
          '}';
    }
  }
}
