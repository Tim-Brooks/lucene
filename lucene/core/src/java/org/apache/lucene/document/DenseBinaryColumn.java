/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.document;

import java.nio.ByteOrder;
import java.util.Objects;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;

/**
 * A {@link Column} that provides dense long values encoded as raw bytes for consecutive documents
 * in a batch. Every document in the batch has exactly one value. Values are returned in bulk as
 * {@link BytesRef} chunks interpreted as packed longs in the specified {@link ByteOrder}.
 *
 * @lucene.experimental
 */
public abstract class DenseBinaryColumn extends Column {

  private final ByteOrder byteOrder;

  /**
   * Creates a DenseBinaryColumn with the given field name, type, and byte order.
   *
   * @param name the field name
   * @param fieldType describes how this field should be indexed
   * @param byteOrder the byte order used to encode long values in the byte arrays
   */
  protected DenseBinaryColumn(String name, IndexableFieldType fieldType, ByteOrder byteOrder) {
    super(name, fieldType);
    this.byteOrder = Objects.requireNonNull(byteOrder, "byteOrder must not be null");
  }

  /** Returns the byte order used to encode long values in the byte arrays. */
  public ByteOrder byteOrder() {
    return byteOrder;
  }

  /**
   * Returns the next batch of values as a {@link BytesRef}. The bytes encode packed long values in
   * the column's {@link #byteOrder()}. The length must be a multiple of {@link Long#BYTES}. Returns
   * {@code null} when there are no more values.
   *
   * <p>The returned {@link BytesRef} is only valid until the next call to {@code nextBytes()}.
   *
   * @return a BytesRef containing the next batch of encoded values, or {@code null} if exhausted
   */
  public abstract BytesRef nextBytes();
}
