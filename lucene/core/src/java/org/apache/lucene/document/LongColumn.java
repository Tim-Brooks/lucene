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
 * A {@link Column} that provides long values. Used for {@link
 * org.apache.lucene.index.DocValuesType#NUMERIC NUMERIC} and {@link
 * org.apache.lucene.index.DocValuesType#SORTED_NUMERIC SORTED_NUMERIC} doc values.
 *
 * <p>The cursor is advanced by calling {@link #nextDoc()}, which returns the next batch-local
 * doc-id that has a value, or {@link #NO_MORE_DOCS} when exhausted. After {@code nextDoc()} returns
 * a valid doc-id, call {@link #longValue()} to retrieve the value.
 *
 * <p>For single-valued fields (NUMERIC), doc-ids are strictly increasing. For multi-valued fields
 * (SORTED_NUMERIC), the same doc-id may appear multiple times (once per value), in non-decreasing
 * order.
 *
 * @lucene.experimental
 */
public abstract class LongColumn extends SparseColumn {

  /** Creates a LongColumn with the given field name and type. */
  protected LongColumn(String name, IndexableFieldType fieldType) {
    super(name, fieldType);
  }

  /**
   * Returns the long value for the current cursor position. Must only be called after {@link
   * #nextDoc()} returns a valid doc-id.
   *
   * @return the long value
   */
  public abstract long longValue();
}
