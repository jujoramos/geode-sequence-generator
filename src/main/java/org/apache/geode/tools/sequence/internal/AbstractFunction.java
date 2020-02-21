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

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.DistributedLockService;

// TODO: check the region type and just lock the primary bucket for partitioned regions.
class AbstractFunction {
  private final static Object LOCK_SYNC = new Object();
  private final static String DISTRIBUTED_LOCK_SERVICE_SUFFIX = "_DSequenceLockService";

  /**
   * Obtains (or creates) the {@link DistributedLockService} associated to the sequences.
   *
   * @param cache Geode cache to use when obtaining or crating the {@link DistributedLockService}.
   * @param sequenceId Name of the sequence to use for building the {@link DistributedLockService}.
   * @return the {@link DistributedLockService} to use generating sequences.
   */
  DistributedLockService getDistributedLockService(Cache cache, String sequenceId) {
    DistributedLockService distributedLockService;
    String lockServiceName = sequenceId + DISTRIBUTED_LOCK_SERVICE_SUFFIX;
    distributedLockService = DistributedLockService.getServiceNamed(lockServiceName);

    if (distributedLockService == null) {
      synchronized (LOCK_SYNC) {
        distributedLockService = DistributedLockService.getServiceNamed(lockServiceName);
        if (distributedLockService == null) {
          distributedLockService = DistributedLockService.create(lockServiceName, cache.getDistributedSystem());
        }
      }
    }

    return distributedLockService;
  }
}
