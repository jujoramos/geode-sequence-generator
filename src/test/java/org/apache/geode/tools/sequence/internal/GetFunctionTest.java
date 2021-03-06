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
public class GetFunctionTest {
  private GetFunction getFunction;
  private ResultSender<Object> resultSender;
  private FunctionContext<String> functionContext;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    getFunction = spy(GetFunction.class);
    resultSender = mock(ResultSender.class);
    functionContext = mock(RegionFunctionContext.class);
  }

  private DistributedLockService mockDistributedLockService(boolean returnValue) {
    DistributedLockService mockDistributedLockService = mock(DistributedLockService.class);
    when(mockDistributedLockService.lock(anyString(),anyLong(), anyLong())).thenReturn(returnValue);
    doReturn(mockDistributedLockService).when(getFunction).getDistributedLockService(any(), any());

    return mockDistributedLockService;
  }

  private void mockRegionFunctionContext(String sequenceId, Region<Object, Object> region) {
    when(functionContext.getArguments()).thenReturn(sequenceId);
    when(functionContext.getResultSender()).thenReturn(resultSender);
    RegionFunctionContext regionFunctionContext = (RegionFunctionContext) functionContext;
    when(regionFunctionContext.getDataSet()).thenReturn(region);
  }

  @Test
  public void executeShouldThrowExceptionWhenContextIsNotInstanceOfRegionFunctionContext() {
    @SuppressWarnings("unchecked")
    FunctionContext<String> mockContext = mock(FunctionContext.class);
    when(mockContext.getArguments()).thenReturn("");

    assertThatThrownBy(() -> getFunction.execute(mockContext))
        .isInstanceOf(FunctionException.class)
        .hasMessage("This is a data aware function, and has to be called using FunctionService.onRegion.");
  }

  @Test
  public void executeShouldThrowExceptionWhenDistributedLockCanNotBeAcquired() {
    mockDistributedLockService(false);
    mockRegionFunctionContext("sequenceId", null);

    assertThatThrownBy(() -> getFunction.execute(functionContext))
        .isInstanceOf(FunctionException.class)
        .hasMessage("Could no acquire Distributed Lock for sequence sequenceId.");
  }

  @Test
  @Parameters({"0", "100", "50000"})
  public void executeShouldReturnExpectedValueAndReleaseLock(Long lastSequence) {
    String seqId = "mySeq";

    @SuppressWarnings("unchecked")
    Region<Object, Object> region = mock(Region.class);
    when(region.getOrDefault(any(), any())).thenReturn(lastSequence);
    mockRegionFunctionContext(seqId, region);
    DistributedLockService distributedLockService = mockDistributedLockService(true);

    getFunction.execute(functionContext);
    verify(distributedLockService).unlock(seqId);
    verify(resultSender).lastResult(lastSequence);
  }
}
