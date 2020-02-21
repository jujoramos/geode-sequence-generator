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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

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

@RunWith(JUnitParamsRunner.class)
public class NextBatchFunctionIntegrationTest {
  private static final String SEQUENCE_ID = "mySequence";
  private File logFile;
  private NextBatchFunction nextBatchFunction;
  private Region<String, Long> sequencesRegion;

  @Rule
  public TestName testName = new TestName();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ServerStarterRule serverStarterRule = new ServerStarterRule();

  @Before
  public void setUp() throws IOException {
    nextBatchFunction = spy(NextBatchFunction.class);
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
    FunctionService.registerFunction(nextBatchFunction);
  }

  @Test
  @TestCaseName("{method}_{0}")
  @SuppressWarnings("unchecked")
  @Parameters(method = "getRegionTypes")
  public void executeShouldThrowExceptionWhenContextIsNotInstanceOfRegionFunctionContext(RegionShortcut regionShortcut) {
    parametrizedSetUp(0L, testName.getMethodName(), regionShortcut);

    FunctionService
        .onMember(serverStarterRule.getCache().getMyId())
        .setArguments(new NextBatchFunction.Args("", 1))
        .execute(nextBatchFunction);

    LogFileAssert.assertThat(logFile).contains("org.apache.geode.cache.execute.FunctionException: This is a data aware function, and has to be called using FunctionService.onRegion.");
  }

  @Test
  @TestCaseName("{method}_{0}")
  @SuppressWarnings("unchecked")
  @Parameters(method = "getRegionTypes")
  public void executeShouldReturnExpectedValues(RegionShortcut regionShortcut) {
    int batchSize = 1;
    long lastSequence = 0L;
    parametrizedSetUp(lastSequence, testName.getMethodName(), regionShortcut);

    ResultCollector<List<Long>, List<List<Long>>> collector = FunctionService
        .onRegion(sequencesRegion)
        .setArguments(new NextBatchFunction.Args(SEQUENCE_ID, batchSize))
        .execute(nextBatchFunction);

    List<Long> expectedValues = new ArrayList<>();
    LongStream.iterate(lastSequence, val -> val + 1).limit(batchSize).forEach(expectedValues::add);

    List<List<Long>> sequences = collector.getResult();
    assertThat(sequences).isNotNull();
    assertThat(sequences.size()).isEqualTo(1);
    assertThat(sequences.get(0)).isNotNull();
    assertThat(sequences.get(0)).isEqualTo(expectedValues);
  }

  @Test
  @TestCaseName("{method}_{0}")
  @SuppressWarnings("unchecked")
  @Parameters(method = "getRegionTypes")
  public void executeShouldReturnExpectedValuesWhenExecutedConcurrently(RegionShortcut regionShortcut) throws InterruptedException, ExecutionException {
    int threads = 50;
    int batchSize = 200;
    long lastSequence = 0L;
    CyclicBarrier cyclicBarrier = new CyclicBarrier(threads);
    List<Callable<List<Long>>> retrievers = new ArrayList<>();
    ExecutorService executorService = Executors.newFixedThreadPool(threads);
    parametrizedSetUp(lastSequence, testName.getMethodName(), regionShortcut);

    // Launch threads to execute the function concurrently on the server
    IntStream.range(0, threads).forEach(value -> retrievers.add(() -> {
      cyclicBarrier.await();

      ResultCollector<List<Long>, List<List<Long>>> collector = FunctionService
          .onRegion(sequencesRegion)
          .setArguments(new NextBatchFunction.Args(SEQUENCE_ID, batchSize))
          .execute(new NextBatchFunction());

      List<List<Long>> sequences = collector.getResult();
      assertThat(sequences).isNotNull();
      assertThat(sequences.size()).isEqualTo(1);
      assertThat(sequences.get(0)).isNotNull();
      assertThat(sequences.get(0).size()).isEqualTo(batchSize);

      return sequences.get(0);
    }));

    // Aggregate and and assert results
    List<Long> actualResults = new ArrayList<>();
    List<Long> expectedResults = new ArrayList<>();
    LongStream.iterate(lastSequence, val -> val + 1).limit(threads * batchSize).forEach(expectedResults::add);
    Collection<Future<List<Long>>> futures = executorService.invokeAll(retrievers);
    for (Future<List<Long>> future : futures) {
      actualResults.addAll(future.get());
    }

    assertThat(actualResults.containsAll(expectedResults)).isTrue();
    assertThat(expectedResults.containsAll(actualResults)).isTrue();
  }
}
