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

import java.util.List;

/**
 * The distributed sequence wrapper.
 * This class can only be used on the client side and it's a simple wrapper for the distributed
 * sequence functionality.
 * Nothing is stored locally, every operation involves a function execution on the Geode cluster.
 */
public interface DistributedSequence {

  /**
   * Obtains the current value for this sequence.
   * @return The current sequence's value.
   */
  Long get();

  /**
   * Returns the next batch of generated sequences.
   * @param batchSize The size of the batch requested (how many consecutive sequences to generate).
   * @return The list of sequences generated.
   */
  List<Long> nextBatch(Integer batchSize);

  /**
   * Increments the sequence and returns its new value.
   * @return The current sequence value + 1.
   */
  default Long incrementAndGet() {
    return nextBatch(1).get(0);
  }
}
