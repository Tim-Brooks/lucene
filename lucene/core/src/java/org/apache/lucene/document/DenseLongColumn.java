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
import org.apache.lucene.util.LongsRef;

/**
 * A {@link Column} that provides dense long values for consecutive documents in a batch. Every
 * document in the batch has exactly one value, so there is no doc-id iterator. Instead, values are
 * returned in bulk via {@link #nextLongs()}.
 *
 * @lucene.experimental
 */
public abstract class DenseLongColumn extends Column {

  /**
   * Creates a DenseLongColumn with the given field name and type.
   *
   * @param name the field name
   * @param fieldType describes how this field should be indexed
   */
  protected DenseLongColumn(String name, IndexableFieldType fieldType) {
    super(name, fieldType);
  }

  /**
   * Returns the next batch of long values as a {@link LongsRef}. The values correspond to
   * consecutive batch-local doc-ids starting from where the previous call left off (or from doc-id
   * 0 on the first call). Returns {@code null} when there are no more values.
   *
   * <p>The returned {@link LongsRef} is only valid until the next call to {@code nextLongs()}.
   *
   * @return a LongsRef containing the next batch of values, or {@code null} if exhausted
   */
  public abstract LongsRef nextLongs();
}
