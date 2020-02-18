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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.distributed.DistributedLockService;

@RunWith(JUnitParamsRunner.class)
public class SequenceFunctionTest {
  private SequenceFunction sequenceFunction;
  private ResultSender<Object> resultSender;
  private FunctionContext<SequenceArgs> functionContext;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    resultSender = mock(ResultSender.class);
    sequenceFunction = spy(SequenceFunction.class);
    functionContext = mock(RegionFunctionContext.class);
  }

  private DistributedLockService mockDistributedLockService(boolean returnValue) {
    DistributedLockService mockDistributedLockService = mock(DistributedLockService.class);
    when(mockDistributedLockService.lock(anyString(),anyLong(), anyLong())).thenReturn(returnValue);
    doReturn(mockDistributedLockService).when(sequenceFunction).getDistributedLockService(any(), any());

    return mockDistributedLockService;
  }

  private void mockRegionFunctionContext(String sequenceId, Integer batchSize, Region<Object, Object> region) {
    when(functionContext.getResultSender()).thenReturn(resultSender);
    when(functionContext.getArguments()).thenReturn(new SequenceArgs(sequenceId, batchSize));
    RegionFunctionContext regionFunctionContext = (RegionFunctionContext) functionContext;
    when(regionFunctionContext.getDataSet()).thenReturn(region);
  }

  @Test
  public void executeShouldThrowExceptionWhenContextIsNotInstanceOfRegionFunctionContext() {
    @SuppressWarnings("unchecked")
    FunctionContext<SequenceArgs> mockContext = mock(FunctionContext.class);
    when(mockContext.getArguments()).thenReturn(new SequenceArgs("", 0));

    assertThatThrownBy(() -> sequenceFunction.execute(mockContext))
        .isInstanceOf(FunctionException.class)
        .hasMessage("This is a data aware function, and has to be called using FunctionService.onRegion.");
  }

  @Test
  public void executeShouldThrowExceptionWhenDistributedLockCanNotBeAcquired() {
    mockDistributedLockService(false);
    mockRegionFunctionContext("sequenceId", 10, null);

    assertThatThrownBy(() -> sequenceFunction.execute(functionContext))
        .isInstanceOf(FunctionException.class)
        .hasMessage("Could no acquire Distributed Lock for sequence sequenceId.");
  }

  @Test
  @Parameters({"0, 10", "100, 1", "50000, 100"})
  public void executeShouldReturnExpectedValuesAndReleaseLock(Long lastSequence, Integer batchSize) {
    String seqId = "mySeq";

    List<Long> expectedValues = new ArrayList<>();
    LongStream.iterate(lastSequence, val -> val + 1).limit(batchSize).forEach(expectedValues::add);

    @SuppressWarnings("unchecked")
    Region<Object, Object> region = mock(Region.class);
    when(region.getOrDefault(any(), any())).thenReturn(lastSequence);
    mockRegionFunctionContext(seqId, batchSize, region);
    DistributedLockService distributedLockService = mockDistributedLockService(true);

    sequenceFunction.execute(functionContext);
    verify(distributedLockService).unlock(seqId);
    verify(resultSender).lastResult(expectedValues);
    verify(region).put(seqId, lastSequence +  batchSize);
  }
}
