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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.stream.IntStream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@RunWith(JUnitParamsRunner.class)
public class SetUpFunctionIntegrationTest {
  private SetUpFunction setUpFunction;

  @Rule
  public TestName testName = new TestName();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ServerStarterRule serverStarterRule = new ServerStarterRule().withAutoStart();

  @Before
  public void setUp() {
    setUpFunction = spy(SetUpFunction.class);
    FunctionService.registerFunction(setUpFunction);
  }

  @SuppressWarnings("unused")
  static DataPolicy[] dataPolicies() {
    return new DataPolicy[] {
        DataPolicy.PERSISTENT_REPLICATE, DataPolicy.PERSISTENT_PARTITION
    };
  }

  @Test
  @SuppressWarnings("unchecked")
  @Parameters(method = "dataPolicies")
  public void executeShouldCreateRegionAndDiskStore(DataPolicy dataPolicy) {
    FunctionService
        .onMember(serverStarterRule.getCache().getMyId())
        .setArguments(dataPolicy)
        .execute(setUpFunction);

    // Assert Disk Store
    DiskStore diskStore = serverStarterRule.getCache().findDiskStore(SetUpFunction.SEQUENCES_DISK_STORE_ID);
    assertThat(diskStore).isNotNull();
    assertThat(diskStore.getName()).isEqualTo(SetUpFunction.SEQUENCES_DISK_STORE_ID);

    // Assert Region Attributes
    Region<?, ?> sequencesRegion = serverStarterRule.getCache().getRegion(SetUpFunction.SEQUENCES_REGION_ID);
    assertThat(sequencesRegion).isNotNull();
    RegionAttributes<?, ?> regionAttributes = sequencesRegion.getAttributes();
    assertThat(regionAttributes.getDataPolicy()).isEqualTo(dataPolicy);
    assertThat(regionAttributes.getKeyConstraint()).isEqualTo(String.class);
    assertThat(regionAttributes.getValueConstraint()).isEqualTo(Long.class);
    assertThat(regionAttributes.getScope()).isEqualTo(Scope.DISTRIBUTED_ACK);
    assertThat(regionAttributes.isDiskSynchronous()).isTrue();
    assertThat(regionAttributes.getDiskStoreName()).isEqualTo(SetUpFunction.SEQUENCES_DISK_STORE_ID);
  }

  @Test
  @SuppressWarnings("unchecked")
  @Parameters(method = "dataPolicies")
  public void executeShouldDoNothingIfRegionAndDiskStoreAlreadyExist(DataPolicy dataPolicy) {
    FunctionService
        .onMember(serverStarterRule.getCache().getMyId())
        .setArguments(dataPolicy)
        .execute(setUpFunction);

    assertThat(serverStarterRule.getCache().getRegion(SetUpFunction.SEQUENCES_REGION_ID)).isNotNull();
    assertThat(serverStarterRule.getCache().findDiskStore(SetUpFunction.SEQUENCES_DISK_STORE_ID)).isNotNull();

    // Execute the function again, it shouldn't re-create already created stuff.
    IntStream.range(0, 10).forEach(i -> FunctionService.onMember(serverStarterRule.getCache().getMyId()).setArguments(dataPolicy).execute(setUpFunction));

    verify(setUpFunction, times(1)).getOrCreateDiskStore(any());
    verify(setUpFunction, times(1)).createRegion(any(), any(), any());
  }
}
