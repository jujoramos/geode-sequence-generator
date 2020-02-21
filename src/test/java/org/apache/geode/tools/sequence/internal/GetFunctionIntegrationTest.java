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
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.util.List;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.junit.rules.ServerStarterRule;
import org.apache.geode.tools.sequence.internal.GetFunction;

@RunWith(JUnitParamsRunner.class)
public class GetFunctionIntegrationTest {
  private static final String SEQUENCE_ID = "mySequence";
  private File logFile;
  private GetFunction getFunction;
  private Region<String, Long> sequencesRegion;

  @Rule
  public TestName testName = new TestName();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ServerStarterRule serverStarterRule = new ServerStarterRule();

  @Before
  public void setUp() throws IOException {
    getFunction = spy(GetFunction.class);
    logFile = temporaryFolder.newFile(testName.getMethodName());
    serverStarterRule.withProperty("log-file", logFile.getAbsolutePath()).startServer();
  }

  @SuppressWarnings("unused")
  static RegionShortcut[] getRegionTypes() {
      return new RegionShortcut[] {
          RegionShortcut.REPLICATE,
          RegionShortcut.PARTITION,
      };
  }

  public void parametrizedSetUp(Long lastSequence, String regionName, RegionShortcut regionShortcut) {
    sequencesRegion = serverStarterRule.getCache()
        .<String, Long>createRegionFactory(regionShortcut)
        .create(regionName);
    sequencesRegion.put(SEQUENCE_ID, lastSequence);
    FunctionService.registerFunction(getFunction);
  }

  @Test
  @TestCaseName("{method}_{0}")
  @SuppressWarnings("unchecked")
  @Parameters(method = "getRegionTypes")
  public void executeShouldThrowExceptionWhenContextIsNotInstanceOfRegionFunctionContext(RegionShortcut regionShortcut) {
    parametrizedSetUp(0L, testName.getMethodName(), regionShortcut);

    FunctionService
        .onMember(serverStarterRule.getCache().getMyId())
        .setArguments("")
        .execute(getFunction);

    LogFileAssert.assertThat(logFile).contains("org.apache.geode.cache.execute.FunctionException: This is a data aware function, and has to be called using FunctionService.onRegion.");
  }

  @Test
  @TestCaseName("{method}_{0}")
  @SuppressWarnings("unchecked")
  @Parameters(method = "getRegionTypes")
  public void executeShouldReturnExpectedValue(RegionShortcut regionShortcut) {
    long lastSequence = 100L;
    parametrizedSetUp(lastSequence, testName.getMethodName(), regionShortcut);

    ResultCollector<Long, List<Long>> collector = FunctionService
        .onRegion(sequencesRegion)
        .setArguments(SEQUENCE_ID)
        .execute(getFunction);

    List<Long> sequences = collector.getResult();
    assertThat(sequences).isNotNull();
    assertThat(sequences.size()).isEqualTo(1);
    assertThat(sequences.get(0)).isNotNull();
    assertThat(sequences.get(0)).isEqualTo(lastSequence);
  }
}
