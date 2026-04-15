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

import org.apache.lucene.index.IndexableFieldType;

/**
 * A {@link Column} that provides values via a sparse doc-id iterator. Not every document in the
 * batch needs to have a value; the cursor skips documents without values. Doc-ids are batch-local
 * (0 to numDocs-1) and must be returned in non-decreasing order.
 *
 * <p>Subclasses specialize the value type: {@link LongColumn} for numeric values and {@link
 * BinaryColumn} for binary/bytes values.
 *
 * @lucene.experimental
 */
public abstract class SparseColumn extends Column {

  /** Sentinel value returned by {@code nextDoc()} when there are no more documents. */
  public static final int NO_MORE_DOCS = Integer.MAX_VALUE;

  /**
   * Creates a SparseColumn with the given field name and type.
   *
   * @param name the field name
   * @param fieldType describes how this field should be indexed
   */
  protected SparseColumn(String name, IndexableFieldType fieldType) {
    super(name, fieldType);
  }

  /**
   * Advances to the next doc-id that has a value and returns it, or {@link #NO_MORE_DOCS} if there
   * are no more values. Doc-ids are batch-local (0 to numDocs-1).
   *
   * @return the next batch-local doc-id, or {@link #NO_MORE_DOCS}
   */
  public abstract int nextDoc();
}
