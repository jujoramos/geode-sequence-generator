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
package org.apache.geode.tools.sequence;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.tools.sequence.internal.SetUpFunction.SEQUENCES_DISK_STORE_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.tools.sequence.internal.SequenceFunction;
import org.apache.geode.tools.sequence.internal.SetUpFunction;

@RunWith(JUnitParamsRunner.class)
public class GeodeSequenceDistributedTest implements Serializable {
  private MemberVM server1, server2;
  private ClientVM client1, client2;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule(5);

  @Before
  public void setUp() throws Exception {
    MemberVM locator = cluster.startLocatorVM(0);
    final int locatorPort = locator.getPort();

    server1 = cluster.startServerVM(1, locatorPort);
    server2 = cluster.startServerVM(2, locatorPort);

    client1 = cluster.startClientVM(3, cf -> cf.withLocatorConnection(locatorPort));
    client2 = cluster.startClientVM(4, cf -> cf.withLocatorConnection(locatorPort));

    MemberVM.invokeInEveryMember(() -> {
      FunctionService.registerFunction(new SetUpFunction());
      FunctionService.registerFunction(new SequenceFunction());
    }, server1, server2);
  }

  private List<Long> buildExpectedResult(long lastSequence, int batchSize) {
    List<Long> expectedValues = new ArrayList<>();
    LongStream.iterate(lastSequence, val -> val + 1).limit(batchSize).forEach(expectedValues::add);

    return expectedValues;
  }

  private void initializeClients(GeodeSequence.RegionType regionType) {
    ClientCache clientCache = ClusterStartupRule.getClientCache();
    assertThat(clientCache).isNotNull();
    GeodeSequence.initialize(clientCache, clientCache.getDefaultPool().getName(), regionType);
  }

  private void launchClientInitializerThreads(int threadsPerClientVm, GeodeSequence.RegionType regionType) throws InterruptedException, ExecutionException {
    ClientCache clientCache = ClusterStartupRule.getClientCache();
    assertThat(clientCache).isNotNull();
    List<Callable<Object>> invokers = new ArrayList<>(threadsPerClientVm);
    ExecutorService executorService = Executors.newFixedThreadPool(threadsPerClientVm);
    IntStream.range(0, threadsPerClientVm).forEach(value -> invokers.add(Executors.callable(new InitializeRunnable(clientCache.getDefaultPool().getName(), clientCache, regionType))));

    List<Future<Object>> initializers = executorService.invokeAll(invokers);
    for (Future<Object> initializer : initializers) {
      initializer.get();
    }
  }

  private List<Long> launchClientSequenceRetrieverThreads(int threadsPerClientVm, String sequenceId, int batchSize) throws InterruptedException, ExecutionException {
    List<Long> aggregateResults = new ArrayList<>();
    ClientCache clientCache = ClusterStartupRule.getClientCache();
    assertThat(clientCache).isNotNull();
    List<Callable<List<Long>>> invokers = new ArrayList<>(threadsPerClientVm);
    ExecutorService executorService = Executors.newFixedThreadPool(threadsPerClientVm);
    IntStream.range(0, threadsPerClientVm).forEach(value -> invokers.add(new SequenceRetrieverCallable(batchSize, sequenceId, clientCache)));

    List<Future<List<Long>>> retrievers = executorService.invokeAll(invokers);
    for (Future<List<Long>> retriever : retrievers) {
      aggregateResults.addAll(retriever.get());
    }

    return aggregateResults;
  }

  @Test
  public void initializeShouldThrowExceptionWhenParametersAreNull() {
    assertThatThrownBy(() -> GeodeSequence.initialize(null, null, null)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> GeodeSequence.initialize(mock(ClientCache.class), null, null)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> GeodeSequence.initialize(null, null, GeodeSequence.RegionType.PARTITION)).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void nextBatchShouldThrowExceptionWhenParametersAreNull() {
    assertThatThrownBy(() -> GeodeSequence.nextBatch(null, null, null)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> GeodeSequence.nextBatch(mock(ClientCache.class), null, null)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> GeodeSequence.nextBatch(mock(ClientCache.class), "", null)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> GeodeSequence.nextBatch(mock(ClientCache.class), null, 10)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> GeodeSequence.nextBatch(null, "", null)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> GeodeSequence.nextBatch(null, null, 10)).isInstanceOf(NullPointerException.class);
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void initializeShouldCreateRegionAndDiskStoreOnSelectedServersOnly(GeodeSequence.RegionType regionType) {
    // Execute initialization on server1 only.
    client1.invoke(() -> {
      PoolFactory poolFactory = PoolManager.createFactory();
      poolFactory.addServer("localhost", server1.getPort()).create("TestPool");
      GeodeSequence.initialize(ClusterStartupRule.getClientCache(), "TestPool", regionType);
      assertThat(ClusterStartupRule.getClientCache().getRegion(SetUpFunction.SEQUENCES_REGION_ID)).isNotNull();
    });

    // Check region and disk store not created on server2 and server3
    MemberVM.invokeInEveryMember(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      assertThat(internalCache).isNotNull();
      assertThat(internalCache.findDiskStore(SEQUENCES_DISK_STORE_ID)).isNull();
      assertThat(internalCache.getRegion(SetUpFunction.SEQUENCES_REGION_ID)).isNull();
    }, server2);

    // Check region and disk store created on server1
    server1.invoke(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      assertThat(internalCache).isNotNull();

      // Disk Store
      await().untilAsserted((() -> assertThat(internalCache.findDiskStore(SEQUENCES_DISK_STORE_ID)).isNotNull()));

      // Region Attributes
      Region<?, ?> sequencesRegion = internalCache.getRegion(SetUpFunction.SEQUENCES_REGION_ID);
      assertThat(sequencesRegion).isNotNull();
      RegionAttributes<?, ?> regionAttributes = sequencesRegion.getAttributes();
      assertThat(regionAttributes.getKeyConstraint()).isEqualTo(String.class);
      assertThat(regionAttributes.getValueConstraint()).isEqualTo(Long.class);
      assertThat(regionAttributes.getScope()).isEqualTo(Scope.DISTRIBUTED_ACK);
      assertThat(regionAttributes.isDiskSynchronous()).isTrue();
      assertThat(regionAttributes.getDiskStoreName()).isEqualTo(SEQUENCES_DISK_STORE_ID);
      assertThat(regionAttributes.getDataPolicy()).isEqualTo(GeodeSequence.regionTypeToDataPolicy(regionType));
    });
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void initializeShouldCreateRegionAndDiskStoreOnAllServersWhenDefaultPoolIsUsed(GeodeSequence.RegionType regionType) {
    client1.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      assertThat(clientCache).isNotNull();
      GeodeSequence.initialize(clientCache, clientCache.getDefaultPool().getName(), regionType);
      assertThat(clientCache.getRegion(SetUpFunction.SEQUENCES_REGION_ID)).isNotNull();
    });

    // Not specific pool used, region and disk store should be created in all members.
    MemberVM.invokeInEveryMember(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      assertThat(internalCache).isNotNull();

      // Disk Store
      await().untilAsserted((() -> assertThat(internalCache.findDiskStore(SEQUENCES_DISK_STORE_ID)).isNotNull()));

      // Region Attributes
      Region<?, ?> sequencesRegion = internalCache.getRegion(SetUpFunction.SEQUENCES_REGION_ID);
      assertThat(sequencesRegion).isNotNull();
      RegionAttributes<?, ?> regionAttributes = sequencesRegion.getAttributes();
      assertThat(regionAttributes.getKeyConstraint()).isEqualTo(String.class);
      assertThat(regionAttributes.getValueConstraint()).isEqualTo(Long.class);
      assertThat(regionAttributes.getScope()).isEqualTo(Scope.DISTRIBUTED_ACK);
      assertThat(regionAttributes.isDiskSynchronous()).isTrue();
      assertThat(regionAttributes.getDiskStoreName()).isEqualTo(SEQUENCES_DISK_STORE_ID);
      assertThat(regionAttributes.getDataPolicy()).isEqualTo(GeodeSequence.regionTypeToDataPolicy(regionType));
    }, server1, server2);
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void initializeCanBeInvokedConcurrentlyByMultipleClients(GeodeSequence.RegionType regionType) throws ExecutionException, InterruptedException {
    AsyncInvocation<?> asyncInvocationClient1 = client1.invokeAsync(() -> launchClientInitializerThreads(30, regionType));
    AsyncInvocation<?> asyncInvocationClient2 = client2.invokeAsync(() -> launchClientInitializerThreads(40, regionType));

    asyncInvocationClient1.await();
    asyncInvocationClient2.await();

    // Region should be created in clients.
    MemberVM.invokeInEveryMember(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      assertThat(clientCache).isNotNull();
      assertThat(clientCache.getRegion(SetUpFunction.SEQUENCES_REGION_ID)).isNotNull();
    }, client1, client2);

    // Region and disk store should be created in all members.
    MemberVM.invokeInEveryMember(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      assertThat(internalCache).isNotNull();

      // Disk Store
      await().untilAsserted((() -> assertThat(internalCache.findDiskStore(SEQUENCES_DISK_STORE_ID)).isNotNull()));

      // Region Attributes
      Region<?, ?> sequencesRegion = internalCache.getRegion(SetUpFunction.SEQUENCES_REGION_ID);
      assertThat(sequencesRegion).isNotNull();
      RegionAttributes<?, ?> regionAttributes = sequencesRegion.getAttributes();
      assertThat(regionAttributes.getKeyConstraint()).isEqualTo(String.class);
      assertThat(regionAttributes.getValueConstraint()).isEqualTo(Long.class);
      assertThat(regionAttributes.getScope()).isEqualTo(Scope.DISTRIBUTED_ACK);
      assertThat(regionAttributes.isDiskSynchronous()).isTrue();
      assertThat(regionAttributes.getDiskStoreName()).isEqualTo(SEQUENCES_DISK_STORE_ID);
      assertThat(regionAttributes.getDataPolicy()).isEqualTo(GeodeSequence.regionTypeToDataPolicy(regionType));
    }, server1, server2);
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void nextBatchShouldReturnRequestedValues(GeodeSequence.RegionType regionType) {
    MemberVM.invokeInEveryMember(() -> initializeClients(regionType), client1, client2);

    // First invocation, should create the sequence and retrieve the requested batchSize
    client1.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      assertThat(GeodeSequence.nextBatch(clientCache, "sequenceId", 10)).isEqualTo(buildExpectedResult(0L, 10));
    });

    // Invoke from other client, should use the existing sequence and retrieve the requested batchSize
    client2.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      assertThat(GeodeSequence.nextBatch(clientCache, "sequenceId", 100)).isEqualTo(buildExpectedResult(10L, 100));
    });
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void nextBatchShouldBeCallableByMultipleClientsConcurrentlyForTheSameSequenceAndReturnRequestedValues(GeodeSequence.RegionType regionType) throws ExecutionException, InterruptedException {
    MemberVM.invokeInEveryMember(() -> initializeClients(regionType), client1, client2);
    AsyncInvocation<List<Long>> asyncInvocationClient1 = client1.invokeAsync(() -> launchClientSequenceRetrieverThreads(30, "sequenceId", 50));
    AsyncInvocation<List<Long>> asyncInvocationClient2 = client2.invokeAsync(() -> launchClientSequenceRetrieverThreads(40, "sequenceId", 100));

    asyncInvocationClient1.await();
    asyncInvocationClient2.await();
    List<Long> client1Results = asyncInvocationClient1.get();
    List<Long> client2Results = asyncInvocationClient2.get();

    // Assert that all sequences were received.
    assertThat(client1Results.size()).isEqualTo(30 * 50);
    assertThat(client2Results.size()).isEqualTo(40 * 100);

    // Assert that the same client did not receive duplicate sequences.
    assertThat(client1Results.stream().distinct().count()).isEqualTo(client1Results.size());
    assertThat(client2Results.stream().distinct().count()).isEqualTo(client2Results.size());

    // Assert that different clients did not receive duplicate sequences.
    Set<Long> merge = new HashSet<>();
    merge.addAll(client1Results);
    merge.addAll(client2Results);
    assertThat(merge.size()).isEqualTo(client1Results.size() + client2Results.size());
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void nextBatchShouldBeCallableByMultipleClientsConcurrentlyForDifferentSequencesAndReturnRequestedValues(GeodeSequence.RegionType regionType) throws ExecutionException, InterruptedException {
    int threads = 30;
    int batchSize = 1000;
    int expectedCount = threads * batchSize;
    MemberVM.invokeInEveryMember(() -> initializeClients(regionType), client1, client2);
    AsyncInvocation<List<Long>> asyncInvocationClient1 = client1.invokeAsync(() -> launchClientSequenceRetrieverThreads(threads, "sequence1", batchSize));
    AsyncInvocation<List<Long>> asyncInvocationClient2 = client2.invokeAsync(() -> launchClientSequenceRetrieverThreads(threads, "sequence2", batchSize));

    asyncInvocationClient1.await();
    asyncInvocationClient2.await();
    List<Long> client1Results = asyncInvocationClient1.get();
    List<Long> client2Results = asyncInvocationClient2.get();

    // Assert that all sequences were received.
    assertThat(client1Results.size()).isEqualTo(expectedCount);
    assertThat(client2Results.size()).isEqualTo(expectedCount);

    // Assert that the same client did not receive duplicate sequences.
    assertThat(client1Results.stream().distinct().count()).isEqualTo(client1Results.size());
    assertThat(client2Results.stream().distinct().count()).isEqualTo(client2Results.size());

    // Assert that both clients got the same results (different sequenceId but same parameters)
    assertThat(client1Results.containsAll(client2Results)).isTrue();
    assertThat(client2Results.containsAll(client1Results)).isTrue();
  }

  private static class InitializeRunnable implements Runnable {
    private final String poolName;
    private final ClientCache clientCache;
    private final GeodeSequence.RegionType regionType;

    public InitializeRunnable(String poolName, ClientCache clientCache, GeodeSequence.RegionType regionType) {
      this.poolName = poolName;
      this.clientCache = clientCache;
      this.regionType = regionType;
    }

    @Override
    public void run() {
      GeodeSequence.initialize(clientCache, poolName, regionType);
    }
  }

  private static class SequenceRetrieverCallable implements Callable<List<Long>> {
    private final int batchSize;
    private final String sequenceId;
    private final ClientCache clientCache;

    public SequenceRetrieverCallable(int batchSize, String sequenceId, ClientCache clientCache) {
      this.batchSize = batchSize;
      this.sequenceId = sequenceId;
      this.clientCache = clientCache;
    }

    @Override
    public List<Long> call() {
      return GeodeSequence.nextBatch(clientCache, sequenceId, batchSize);
    }
  }
}
