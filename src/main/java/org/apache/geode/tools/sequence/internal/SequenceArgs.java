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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;

public class SequenceArgs implements DataSerializable {
  private String id;
  private Integer batchSize;

  public String getId() {
    return id;
  }

  public Integer getBatchSize() {
    return batchSize;
  }

  @SuppressWarnings("unused")
  public SequenceArgs() {
  }

  public SequenceArgs(String id, Integer batchSize) {
    Objects.requireNonNull(id);
    Objects.requireNonNull(batchSize);

    this.id = id;
    this.batchSize = batchSize;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(id, out);
    DataSerializer.writeInteger(batchSize, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException {
    this.id = DataSerializer.readString(in);
    this.batchSize = DataSerializer.readInteger(in);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SequenceArgs that = (SequenceArgs) o;

    if (!id.equals(that.id)) {
      return false;
    }
    return batchSize.equals(that.batchSize);
  }

  @Override
  public int hashCode() {
    int result = id.hashCode();
    result = 31 * result + batchSize.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "SequenceArguments{" +
        "name='" + id + '\'' +
        ", rangeSize=" + batchSize +
        '}';
  }
}
