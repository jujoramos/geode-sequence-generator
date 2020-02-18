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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.xmlcache.RegionAttributesCreation;

@RunWith(JUnitParamsRunner.class)
public class SetUpFunctionTest {
  private SetUpFunction setUpFunction;
  private InternalCache internalCache;
  private DiskStoreFactory diskStoreFactory;
  private FunctionContext<DataPolicy> functionContext;
  private InternalCacheForClientAccess internalCacheForClientAccess;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    setUpFunction = spy(SetUpFunction.class);
    internalCache = mock(InternalCache.class);
    functionContext = mock(FunctionContext.class);
    diskStoreFactory = mock(DiskStoreFactory.class);
    internalCacheForClientAccess = mock(InternalCacheForClientAccess.class);

    when(functionContext.getCache()).thenReturn(internalCache);
    when(internalCache.createDiskStoreFactory()).thenReturn(diskStoreFactory);
    when(internalCache.getCacheForProcessingClientRequests()).thenReturn(internalCacheForClientAccess);
  }

  @SuppressWarnings("unused")
  static DataPolicy[] dataPolicies() {
    return new DataPolicy[] {
        DataPolicy.PERSISTENT_REPLICATE, DataPolicy.PERSISTENT_REPLICATE
    };
  }

  @Test
  public void executeShouldThrowExceptionWhenCacheIsNotInstanceOfInternalCache() {
    when(functionContext.getCache()).thenReturn(mock(Cache.class));

    assertThatThrownBy(() -> setUpFunction.execute(functionContext))
        .isInstanceOf(FunctionException.class)
        .hasMessage("The function needs an instance of InternalCache to execute some non-public methods.");
  }

  @Test
  @Parameters(method = "dataPolicies")
  public void executeShouldPropagateExceptionsThrownWhileCreatingTheRegion(DataPolicy dataPolicy) throws IOException, ClassNotFoundException {
    when(functionContext.getArguments()).thenReturn(dataPolicy);
    when(internalCache.findDiskStore(any())).thenReturn(mock(DiskStore.class));

    doThrow(new ClassNotFoundException("Mock Exception")).when(internalCacheForClientAccess).createInternalRegion(any(), any(), any());
    assertThatThrownBy(() -> setUpFunction.execute(functionContext))
        .isInstanceOf(FunctionException.class)
        .hasMessage("Internal error while creating the region")
        .hasCauseInstanceOf(ClassNotFoundException.class);

    doThrow(new IOException("Mock Exception")).when(internalCacheForClientAccess).createInternalRegion(any(), any(), any());
    assertThatThrownBy(() -> setUpFunction.execute(functionContext))
        .isInstanceOf(FunctionException.class)
        .hasMessage("Internal error while creating the region")
        .hasCauseInstanceOf(IOException.class);
  }

  @Test
  @Parameters(method = "dataPolicies")
  public void executeShouldIgnoreRegionExistsException(DataPolicy dataPolicy) throws IOException, ClassNotFoundException {
    when(functionContext.getArguments()).thenReturn(dataPolicy);
    when(internalCache.findDiskStore(any())).thenReturn(mock(DiskStore.class));

    doThrow(new RegionExistsException(mock(Region.class))).when(internalCacheForClientAccess).createInternalRegion(any(), any(), any());
    assertThatCode(() -> setUpFunction.execute(functionContext)).doesNotThrowAnyException();
  }

  @Test
  public void executeShouldNotReCreateDiskStore() throws IOException, ClassNotFoundException {
    when(internalCache.getRegion(any())).thenReturn(null);
    when(functionContext.getArguments()).thenReturn(mock(DataPolicy.class));
    when(internalCache.findDiskStore(any())).thenReturn(mock(DiskStore.class));

    setUpFunction.execute(functionContext);
    verify(diskStoreFactory, never()).create(any());
    verify(internalCacheForClientAccess).createInternalRegion(any(), any(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void executeShouldNotReCreateRegion() throws IOException, ClassNotFoundException {
    when(internalCache.getRegion(any())).thenReturn(mock(Region.class));

    setUpFunction.execute(functionContext);
    verify(internalCache, never()).createVMRegion(any(), any(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  @Parameters(method = "dataPolicies")
  public void executeShouldCreateRegionAndDiskStore(DataPolicy dataPolicy) throws IOException, ClassNotFoundException {
    ArgumentCaptor<String> regionNameCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<InternalRegionArguments> internalArgumentsCaptor = ArgumentCaptor.forClass(InternalRegionArguments.class);
    ArgumentCaptor<RegionAttributesCreation> regionAttributesCaptor = ArgumentCaptor.forClass(RegionAttributesCreation.class);
    DiskStore mockDiskStore = mock(DiskStore.class);
    when(mockDiskStore.getName()).thenReturn(SetUpFunction.SEQUENCES_DISK_STORE_ID);
    when(diskStoreFactory.create(SetUpFunction.SEQUENCES_DISK_STORE_ID)).thenReturn(mockDiskStore);
    when(functionContext.getArguments()).thenReturn(dataPolicy);

    setUpFunction.execute(functionContext);
    verify(diskStoreFactory).create(SetUpFunction.SEQUENCES_DISK_STORE_ID);
    verify(internalCacheForClientAccess).createInternalRegion(regionNameCaptor.capture(), regionAttributesCaptor.capture(), internalArgumentsCaptor.capture());

    // Assert arguments used to create the region.
    assertThat(regionNameCaptor.getValue()).isEqualTo(SetUpFunction.SEQUENCES_REGION_ID);
    InternalRegionArguments internalRegionArguments = internalArgumentsCaptor.getValue();
    assertThat(internalRegionArguments.isInternalRegion()).isTrue();
    RegionAttributesCreation regionAttributes = regionAttributesCaptor.getValue();
    assertThat(regionAttributes.getDataPolicy()).isEqualTo(dataPolicy);
    assertThat(regionAttributes.getKeyConstraint()).isEqualTo(String.class);
    assertThat(regionAttributes.getValueConstraint()).isEqualTo(Long.class);
    assertThat(regionAttributes.getScope()).isEqualTo(Scope.DISTRIBUTED_ACK);
    assertThat(regionAttributes.getDiskStoreName()).isEqualTo(SetUpFunction.SEQUENCES_DISK_STORE_ID);
  }
}
