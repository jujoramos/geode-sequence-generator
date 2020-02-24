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
import static org.apache.geode.tools.sequence.internal.TestAccessHelper.getDistributedSequencesRegionDiskStoreId;
import static org.apache.geode.tools.sequence.internal.TestAccessHelper.getDistributedSequencesRegionId;
import static org.apache.geode.tools.sequence.internal.TestAccessHelper.getSetUpFunctionId;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
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
import junitparams.naming.TestCaseName;
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
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.metrics.FunctionStats;
import org.apache.geode.internal.cache.execute.metrics.FunctionStatsManager;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.tools.sequence.internal.TestAccessHelper;

@RunWith(JUnitParamsRunner.class)
public class DistributedSequenceDistributedTest implements Serializable {
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

    TestAccessHelper.registerFunctions(server1, server2);
  }

  private List<Long> buildExpectedResult(long lastSequence, int batchSize) {
    List<Long> expectedValues = new ArrayList<>();
    LongStream.iterate(lastSequence, val -> val + 1).limit(batchSize).forEach(expectedValues::add);

    return expectedValues;
  }

  private void initializeClients(RegionType regionType) {
    MemberVM.invokeInEveryMember(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      assertThat(clientCache).isNotNull();
      DistributedSequenceFactory.initialize(clientCache, regionType);
    }, client1, client2);
  }

  private void launchClientInitializerThreads(int threadsPerClientVm, RegionType regionType) throws InterruptedException, ExecutionException {
    ClientCache clientCache = ClusterStartupRule.getClientCache();
    assertThat(clientCache).isNotNull();
    List<Callable<Object>> invokers = new ArrayList<>(threadsPerClientVm);
    ExecutorService executorService = Executors.newFixedThreadPool(threadsPerClientVm);
    IntStream.range(0, threadsPerClientVm).forEach(value -> invokers.add(Executors.callable(new InitializeRunnable(regionType, clientCache))));

    List<Future<Object>> initializers = executorService.invokeAll(invokers);
    for (Future<Object> initializer : initializers) {
      initializer.get();
    }
  }

  private List<Long> launchNextBatchThreads(int threadsPerClientVm, String sequenceId, int batchSize) throws InterruptedException, ExecutionException {
    ClientCache clientCache = ClusterStartupRule.getClientCache();
    assertThat(clientCache).isNotNull();
    List<Callable<List<Long>>> invokers = new ArrayList<>(threadsPerClientVm);
    ExecutorService executorService = Executors.newFixedThreadPool(threadsPerClientVm);
    IntStream.range(0, threadsPerClientVm).forEach(value -> invokers.add(new NextBatchCallable(batchSize, DistributedSequenceFactory.getSequence(sequenceId))));

    List<Long> aggregateResults = new ArrayList<>();
    List<Future<List<Long>>> retrievers = executorService.invokeAll(invokers);
    for (Future<List<Long>> retriever : retrievers) {
      aggregateResults.addAll(retriever.get());
    }

    return aggregateResults;
  }

  private List<Long> launchIncrementAndGetThreads(int threadsPerClientVm, String sequenceId) throws InterruptedException, ExecutionException {
    ClientCache clientCache = ClusterStartupRule.getClientCache();
    assertThat(clientCache).isNotNull();
    List<Callable<Long>> invokers = new ArrayList<>(threadsPerClientVm);
    ExecutorService executorService = Executors.newFixedThreadPool(threadsPerClientVm);
    IntStream.range(0, threadsPerClientVm).forEach(value -> invokers.add(new IncrementAndGetCallable(DistributedSequenceFactory.getSequence(sequenceId))));

    List<Long> aggregateResults = new ArrayList<>();
    List<Future<Long>> retrievers = executorService.invokeAll(invokers);
    for (Future<Long> retriever : retrievers) {
      aggregateResults.add(retriever.get());
    }

    return aggregateResults;
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  @TestCaseName("[{index}] {method}(RegionType:{0})")
  public void initializeShouldCreateRegionAndDiskStoreOnSelectedServersOnly(RegionType regionType) {
    // Execute initialization on server1 only.
    client1.invoke(() -> {
      PoolFactory poolFactory = PoolManager.createFactory();
      poolFactory.addServer("localhost", server1.getPort()).create("TestPool");
      DistributedSequenceFactory.initialize(ClusterStartupRule.getClientCache(), regionType, "TestPool");

      assertThat(ClusterStartupRule.getClientCache().getRegion(getDistributedSequencesRegionId())).isNotNull();
    });

    // Check region and disk store not created on server2
    MemberVM.invokeInEveryMember(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      assertThat(internalCache).isNotNull();
      assertThat(internalCache.findDiskStore(getDistributedSequencesRegionDiskStoreId())).isNull();
      assertThat(internalCache.getRegion(getDistributedSequencesRegionId())).isNull();
    }, server2);

    // Check region and disk store created on server1
    server1.invoke(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      assertThat(internalCache).isNotNull();

      // Disk Store
      await().untilAsserted((() -> assertThat(internalCache.findDiskStore(getDistributedSequencesRegionDiskStoreId())).isNotNull()));

      // Region Attributes
      Region<?, ?> sequencesRegion = internalCache.getRegion(getDistributedSequencesRegionId());
      assertThat(sequencesRegion).isNotNull();
      RegionAttributes<?, ?> regionAttributes = sequencesRegion.getAttributes();
      assertThat(regionAttributes.getKeyConstraint()).isEqualTo(String.class);
      assertThat(regionAttributes.getValueConstraint()).isEqualTo(Long.class);
      assertThat(regionAttributes.getScope()).isEqualTo(Scope.DISTRIBUTED_ACK);
      assertThat(regionAttributes.isDiskSynchronous()).isTrue();
      assertThat(regionAttributes.getDiskStoreName()).isEqualTo(getDistributedSequencesRegionDiskStoreId());
      assertThat(regionAttributes.getDataPolicy()).isEqualTo(regionType.getDataPolicy());
    });
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  @TestCaseName("[{index}] {method}(RegionType:{0})")
  public void initializeShouldCreateRegionAndDiskStoreOnAllServersWhenDefaultPoolIsUsed(RegionType regionType) {
    client1.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      assertThat(clientCache).isNotNull();
      DistributedSequenceFactory.initialize(clientCache, regionType);
      assertThat(clientCache.getRegion(getDistributedSequencesRegionId())).isNotNull();
    });

    // Not specific pool used, region and disk store should be created in all members.
    MemberVM.invokeInEveryMember(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      assertThat(internalCache).isNotNull();

      // Disk Store
      await().untilAsserted((() -> assertThat(internalCache.findDiskStore(getDistributedSequencesRegionDiskStoreId())).isNotNull()));

      // Region Attributes
      Region<?, ?> sequencesRegion = internalCache.getRegion(getDistributedSequencesRegionId());
      assertThat(sequencesRegion).isNotNull();
      RegionAttributes<?, ?> regionAttributes = sequencesRegion.getAttributes();
      assertThat(regionAttributes.getKeyConstraint()).isEqualTo(String.class);
      assertThat(regionAttributes.getValueConstraint()).isEqualTo(Long.class);
      assertThat(regionAttributes.getScope()).isEqualTo(Scope.DISTRIBUTED_ACK);
      assertThat(regionAttributes.getDataPolicy()).isEqualTo(regionType.getDataPolicy());
      assertThat(regionAttributes.isDiskSynchronous()).isTrue();
      assertThat(regionAttributes.getDiskStoreName()).isEqualTo(getDistributedSequencesRegionDiskStoreId());
    }, server1, server2);
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  @TestCaseName("[{index}] {method}(RegionType:{0})")
  public void initializeCanBeInvokedConcurrentlyByMultipleClients(RegionType regionType) throws ExecutionException, InterruptedException {
    AsyncInvocation<?> asyncInvocationClient1 = client1.invokeAsync(() -> launchClientInitializerThreads(30, regionType));
    AsyncInvocation<?> asyncInvocationClient2 = client2.invokeAsync(() -> launchClientInitializerThreads(40, regionType));

    asyncInvocationClient1.await();
    asyncInvocationClient2.await();

    // Region should be created in both clients.
    MemberVM.invokeInEveryMember(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      assertThat(clientCache).isNotNull();
      assertThat(clientCache.getRegion(getDistributedSequencesRegionId())).isNotNull();
    }, client1, client2);

    // Region and disk store should be created in all members
    MemberVM.invokeInEveryMember(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      assertThat(internalCache).isNotNull();

      // Disk Store
      await().untilAsserted((() -> assertThat(internalCache.findDiskStore(getDistributedSequencesRegionDiskStoreId())).isNotNull()));

      // Region Attributes
      Region<?, ?> sequencesRegion = internalCache.getRegion(getDistributedSequencesRegionId());
      assertThat(sequencesRegion).isNotNull();
      RegionAttributes<?, ?> regionAttributes = sequencesRegion.getAttributes();
      assertThat(regionAttributes.getKeyConstraint()).isEqualTo(String.class);
      assertThat(regionAttributes.getValueConstraint()).isEqualTo(Long.class);
      assertThat(regionAttributes.getScope()).isEqualTo(Scope.DISTRIBUTED_ACK);
      assertThat(regionAttributes.getDataPolicy()).isEqualTo(regionType.getDataPolicy());
      assertThat(regionAttributes.isDiskSynchronous()).isTrue();
      assertThat(regionAttributes.getDiskStoreName()).isEqualTo(getDistributedSequencesRegionDiskStoreId());
    }, server1, server2);

    // SetUpFunction invoked only once per member.
    MemberVM.invokeInEveryMember(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      assertThat(internalCache).isNotNull();
      FunctionStats functionStats = FunctionStatsManager.getFunctionStats(getSetUpFunctionId());
      assertThat(functionStats.getFunctionExecutionCalls()).isEqualTo(2);
    }, server1, server2);
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  @TestCaseName("[{index}] {method}(RegionType:{0})")
  public void getShouldReturnRequestedValue(RegionType regionType) {
    initializeClients(regionType);
    String sequenceId = "mySequence";

    // First invocation, should create the sequence and retrieve the requested value
    client1.invoke(() -> {
      DistributedSequence distributedSequence = DistributedSequenceFactory.getSequence(sequenceId);
      assertThat(distributedSequence.get()).isEqualTo(0L);
    });

    // Manually modify the sequence (clients must not do this).
    server1.invoke(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      assertThat(internalCache).isNotNull();
      Region<String, Long> sequencesRegion = internalCache.getRegion(getDistributedSequencesRegionId());
      sequencesRegion.put(sequenceId, 10L);
    });

    // Invoke from other client, should use the existing sequence and retrieve the requested value
    client2.invoke(() -> {
      DistributedSequence distributedSequence = DistributedSequenceFactory.getSequence(sequenceId);
      assertThat(distributedSequence.get()).isEqualTo(10L);
    });
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  @TestCaseName("[{index}] {method}(RegionType:{0})")
  public void nextBatchShouldReturnRequestedValues(RegionType regionType) {
    initializeClients(regionType);
    String sequenceId = "mySequence";

    // First invocation, should create the sequence and retrieve the requested batchSize
    client1.invoke(() -> {
      DistributedSequence distributedSequence = DistributedSequenceFactory.getSequence(sequenceId);
      assertThat(distributedSequence.nextBatch(10)).isEqualTo(buildExpectedResult(0L, 10));
    });

    // Invoke from other client, should use the existing sequence and retrieve the requested batchSize
    client2.invoke(() -> {
      DistributedSequence distributedSequence = DistributedSequenceFactory.getSequence(sequenceId);
      assertThat(distributedSequence.nextBatch(100)).isEqualTo(buildExpectedResult(10L, 100));
    });
  }

  @Test
  @Parameters({
      "REPLICATE, mySequence, mySequence",
      "REPLICATE, seqClient1, seqClient2",
      "PARTITION, mySequence, mySequence",
      "PARTITION, seqClient1, seqClient2",
  })
  @TestCaseName("[{index}] {method}(RegionType:{0},SequenceClient1:{1},SequenceClient2:{2})")
  public void nextBatchCanBeInvokedByMultipleClientsConcurrentlyAndReturnRequestedValues(RegionType regionType, String sequenceClient1, String sequenceClient2) throws ExecutionException, InterruptedException {
    initializeClients(regionType);
    AsyncInvocation<List<Long>> asyncInvocationClient1 = client1.invokeAsync(() -> launchNextBatchThreads(30, sequenceClient1, 50));
    AsyncInvocation<List<Long>> asyncInvocationClient2 = client2.invokeAsync(() -> launchNextBatchThreads(40, sequenceClient2, 100));

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
    if (sequenceClient1.equals(sequenceClient2)) {
      Set<Long> merge = new HashSet<>();
      merge.addAll(client1Results);
      merge.addAll(client2Results);
      assertThat(merge.size()).isEqualTo(client1Results.size() + client2Results.size());
    }
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  @TestCaseName("[{index}] {method}(RegionType:{0})")
  public void incrementAndGetShouldReturnRequestedValue(RegionType regionType) {
    initializeClients(regionType);
    String sequenceId = "mySequence";

    // Invoke the incrementAndGet some times randomly from the clients.
    Random random = new Random();
    List<ClientVM> clientVMS = Arrays.asList(client1, client2);
    LongStream.range(0, 50).forEach(value -> {
      ClientVM clientVM = clientVMS.get(random.nextInt(2));
      clientVM.invoke(() -> {
        DistributedSequence distributedSequence = DistributedSequenceFactory.getSequence(sequenceId);
        assertThat(distributedSequence.incrementAndGet()).isEqualTo(value);
      });
    });
  }

  @Test
  @Parameters({
      "REPLICATE, mySequence, mySequence",
      "REPLICATE, seqClient1, seqClient2",
      "PARTITION, mySequence, mySequence",
      "PARTITION, seqClient1, seqClient2",
  })
  @TestCaseName("[{index}] {method}(RegionType:{0},SequenceClient1:{1},SequenceClient2:{2})")
  public void incrementAndGetCanBeInvokedByMultipleClientsConcurrentlyAndReturnRequestedValues(RegionType regionType, String sequenceClient1, String sequenceClient2) throws ExecutionException, InterruptedException {
    initializeClients(regionType);
    AsyncInvocation<List<Long>> asyncInvocationClient1 = client1.invokeAsync(() -> launchIncrementAndGetThreads(30, sequenceClient1));
    AsyncInvocation<List<Long>> asyncInvocationClient2 = client2.invokeAsync(() -> launchIncrementAndGetThreads(40, sequenceClient2));

    asyncInvocationClient1.await();
    asyncInvocationClient2.await();
    List<Long> client1Results = asyncInvocationClient1.get();
    List<Long> client2Results = asyncInvocationClient2.get();

    // Assert that all sequences were received.
    assertThat(client1Results.size()).isEqualTo(30);
    assertThat(client2Results.size()).isEqualTo(40);

    // Assert that the same client did not receive duplicate sequences.
    assertThat(client1Results.stream().distinct().count()).isEqualTo(client1Results.size());
    assertThat(client2Results.stream().distinct().count()).isEqualTo(client2Results.size());

    // Assert that different clients did not receive duplicate sequences.
    if (sequenceClient1.equals(sequenceClient2)) {
      Set<Long> merge = new HashSet<>();
      merge.addAll(client1Results);
      merge.addAll(client2Results);
      assertThat(merge.size()).isEqualTo(client1Results.size() + client2Results.size());
    }
  }

  private static class InitializeRunnable implements Runnable {
    private final RegionType regionType;
    private final ClientCache clientCache;

    public InitializeRunnable(RegionType regionType, ClientCache clientCache) {
      this.regionType = regionType;
      this.clientCache = clientCache;
    }

    @Override
    public void run() {
      DistributedSequenceFactory.initialize(clientCache, regionType);
    }
  }

  private static class NextBatchCallable implements Callable<List<Long>> {
    private final int batchSize;
    private final DistributedSequence distributedSequence;

    public NextBatchCallable(int batchSize, DistributedSequence distributedSequence) {
      this.batchSize = batchSize;
      this.distributedSequence = distributedSequence;
    }

    @Override
    public List<Long> call() {
      return distributedSequence.nextBatch(batchSize);
    }
  }

  private static class IncrementAndGetCallable implements Callable<Long> {
    private final DistributedSequence distributedSequence;

    public IncrementAndGetCallable(DistributedSequence distributedSequence) {
      this.distributedSequence = distributedSequence;
    }

    @Override
    public Long call() {
      return distributedSequence.incrementAndGet();
    }
  }
}
