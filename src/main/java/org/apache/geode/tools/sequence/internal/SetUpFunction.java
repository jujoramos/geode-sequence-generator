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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.xmlcache.RegionAttributesCreation;
import org.apache.geode.management.internal.security.ResourcePermissions;
import org.apache.geode.security.ResourcePermission;

/**
 * Function used to set up the distributed sequence tool.
 */
public class SetUpFunction implements Function<DataPolicy> {
  static final String FUNCTION_ID = "DSequenceSetUp";
  static final String DISTRIBUTED_SEQUENCES_REGION_ID = "DSequences";
  static final String DISTRIBUTED_SEQUENCES_DISK_STORE_ID = "DSequences_DiskStore";
  private static final ReentrantLock reentrantLock = new ReentrantLock();

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
    return false;
  }

  @Override
  public boolean optimizeForWrite() {
    return false;
  }

  @Override
  public Collection<ResourcePermission> getRequiredPermissions(String regionName) {
    return Collections.singletonList(ResourcePermissions.DATA_WRITE);
  }

  @Override
  public Collection<ResourcePermission> getRequiredPermissions(String regionName, Object args) {
    return getRequiredPermissions(regionName);
  }

  /**
   * Get (or create) the {@link DiskStore} used by the sequences region.
   * The method is invoked while the lock is held, so no other threads should be creating the
   * store at the same time.
   *
   * @param internalCache Geode cache used to retrieve/create the disk store.
   * @return The {@link DiskStore} to be used by the sequences region.
   */
  DiskStore getOrCreateDiskStore(InternalCache internalCache) {
    DiskStore diskStore = internalCache.findDiskStore(DISTRIBUTED_SEQUENCES_DISK_STORE_ID);

    if (diskStore == null) {
      diskStore = internalCache.createDiskStoreFactory().create(DISTRIBUTED_SEQUENCES_DISK_STORE_ID);
    }

    return diskStore;
  }

  /**
   * Creates the internal region used by the sequences.
   *
   * @param internalCache The Geode cache.
   * @param diskStoreName Disk store name to associate the region with.
   * @param dataPolicy The data policy for the region holding the sequences.
   */
  @SuppressWarnings("unchecked")
  void createRegion(InternalCache internalCache, String diskStoreName, DataPolicy dataPolicy) {
    // Check again while holding the lock.
    if (internalCache.getRegion(DISTRIBUTED_SEQUENCES_REGION_ID) == null) {
      // Set Up Region Attributes
      RegionAttributesCreation regionAttributesCreation = new RegionAttributesCreation();
      regionAttributesCreation.setDataPolicy(dataPolicy);
      regionAttributesCreation.setKeyConstraint(String.class);
      regionAttributesCreation.setValueConstraint(Long.class);
      regionAttributesCreation.setDiskSynchronous(true);
      regionAttributesCreation.setDiskStoreName(diskStoreName);
      regionAttributesCreation.setScope(Scope.DISTRIBUTED_ACK);
      InternalRegionArguments internalRegionArguments = new InternalRegionArguments().setInternalRegion(true);

      try {
        // InternalCacheForClientAccess to allow creating an internal region from client side.
        InternalCacheForClientAccess cacheForClientAccess = internalCache.getCacheForProcessingClientRequests();
        cacheForClientAccess.createInternalRegion(DISTRIBUTED_SEQUENCES_REGION_ID, regionAttributesCreation, internalRegionArguments);
      } catch (IOException | ClassNotFoundException exception) {
        throw new FunctionException("Internal error while creating the region", exception);
      }
    }
  }

  @Override
  public void execute(FunctionContext<DataPolicy> context) {
    if (!(context.getCache() instanceof InternalCache)) {
      throw new FunctionException("The function needs an instance of InternalCache to execute some non-public methods.");
    }

    // Create region if not already created.
    InternalCache internalCache = (InternalCache) context.getCache();
    if (internalCache.getRegion(DISTRIBUTED_SEQUENCES_REGION_ID) == null) {
      // Hold the lock to avoid multiple threads creating the region and/or diskStore at the same time.
      reentrantLock.lock();

      try {
        DiskStore diskStore = getOrCreateDiskStore(internalCache);
        createRegion(internalCache, diskStore.getName(), context.getArguments());
      } finally {
        reentrantLock.unlock();
      }
    }
  }
}
