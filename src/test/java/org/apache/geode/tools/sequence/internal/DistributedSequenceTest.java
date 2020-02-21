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
import static org.mockito.Mockito.mock;

import java.io.Serializable;

import junitparams.JUnitParamsRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.tools.sequence.RegionType;

@RunWith(JUnitParamsRunner.class)
public class DistributedSequenceTest implements Serializable {
  private ClientCache clientCache;

  @Before
  public void setUp() {
    clientCache = mock(ClientCache.class);
  }

  @Test
  public void constructorShouldThrownExceptionWhenClientCacheOrRegionTypeAreNull() {
    assertThatThrownBy(() -> new DistributedSequenceImpl(null, null, null)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new DistributedSequenceImpl(null, null, clientCache)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new DistributedSequenceImpl(null, RegionType.REPLICATE, null)).isInstanceOf(NullPointerException.class);

    assertThatThrownBy(() -> new DistributedSequenceImpl(null, null, null, null)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new DistributedSequenceImpl(null, null, null, clientCache)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new DistributedSequenceImpl(null, RegionType.REPLICATE, null, null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void constructorShouldThrownExceptionWhenSequenceIdIsEmptyOrNull() {
    assertThatThrownBy(() -> new DistributedSequenceImpl("", RegionType.REPLICATE, clientCache))
        .isInstanceOf(IllegalArgumentException.class).hasMessage("SequenceId can not be null nor empty.");
    assertThatThrownBy(() -> new DistributedSequenceImpl(null, RegionType.REPLICATE, clientCache))
        .isInstanceOf(IllegalArgumentException.class).hasMessage("SequenceId can not be null nor empty.");

    assertThatThrownBy(() -> new DistributedSequenceImpl("", RegionType.REPLICATE, "poolName", clientCache))
        .isInstanceOf(IllegalArgumentException.class).hasMessage("SequenceId can not be null nor empty.");
    assertThatThrownBy(() -> new DistributedSequenceImpl(null, RegionType.REPLICATE, "poolName", clientCache))
        .isInstanceOf(IllegalArgumentException.class).hasMessage("SequenceId can not be null nor empty.");
  }

  @Test
  public void constructorShouldThrownExceptionWhenPoolIsNull() {
    assertThatThrownBy(() -> new DistributedSequenceImpl("sequenceId", RegionType.REPLICATE, clientCache))
        .isInstanceOf(IllegalArgumentException.class).hasMessage("The default pool could not be found.");
    assertThatThrownBy(() -> new DistributedSequenceImpl("sequenceId", RegionType.REPLICATE, clientCache))
        .isInstanceOf(IllegalArgumentException.class).hasMessage("The default pool could not be found.");

    assertThatThrownBy(() -> new DistributedSequenceImpl("sequenceId", RegionType.REPLICATE, null, clientCache))
        .isInstanceOf(IllegalArgumentException.class).hasMessage("The specified pool (null) could not be found.");
    assertThatThrownBy(() -> new DistributedSequenceImpl("sequenceId", RegionType.REPLICATE, "nonExistingPool", clientCache))
        .isInstanceOf(IllegalArgumentException.class).hasMessage("The specified pool (nonExistingPool) could not be found.");
  }
}
