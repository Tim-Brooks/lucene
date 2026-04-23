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
package org.apache.lucene.document.column;

import java.util.Objects;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

/**
 * A {@link Column} that provides long values. Used for {@link
 * org.apache.lucene.index.DocValuesType#NUMERIC NUMERIC} and {@link
 * org.apache.lucene.index.DocValuesType#SORTED_NUMERIC SORTED_NUMERIC} doc values and for stored
 * numeric fields.
 *
 * <p>Iteration is performed via cursors. {@link #tuples()} is always available and yields {@code
 * (docID, longValue)} pairs. {@link #values()} is a bulk cursor over consecutive doc-ids; it must
 * be overridden when {@link #density()} is {@link Column.Density#DENSE DENSE} and is only consulted
 * in that case.
 *
 * <p>{@link #numericKind()} marks how the long bits should be interpreted. Defaults to {@link
 * NumericKind#LONG LONG}; pass {@link NumericKind#INT INT} (low 32 bits, sign-extended), {@link
 * NumericKind#FLOAT FLOAT} (low 32 bits via {@link Float#intBitsToFloat}), or {@link
 * NumericKind#DOUBLE DOUBLE} (full 64 bits via {@link Double#longBitsToDouble}) to the constructor
 * to select another interpretation. The numeric kind drives the default {@link #storedType()} and
 * will drive the sortable-byte encoding when points support is added.
 *
 * @lucene.experimental
 */
public abstract class LongColumn extends Column {

  /** The numeric interpretation of the column's long values. */
  public enum NumericKind {
    /** Signed 32-bit int, taken from the low 32 bits of the long value. */
    INT,
    /** Signed 64-bit long. */
    LONG,
    /**
     * IEEE 754 32-bit float, bit-packed in the low 32 bits of the long value (see {@link
     * Float#floatToRawIntBits}).
     */
    FLOAT,
    /**
     * IEEE 754 64-bit double, bit-packed in the long value (see {@link
     * Double#doubleToRawLongBits}).
     */
    DOUBLE,
  }

  private final NumericKind numericKind;

  /** Creates a LongColumn with {@link NumericKind#LONG}. */
  protected LongColumn(String name, IndexableFieldType fieldType, Density density) {
    this(name, fieldType, density, NumericKind.LONG);
  }

  /** Creates a LongColumn with the given numeric interpretation. */
  protected LongColumn(
      String name, IndexableFieldType fieldType, Density density, NumericKind numericKind) {
    super(name, fieldType, density);
    this.numericKind = Objects.requireNonNull(numericKind, "numericKind must not be null");
  }

  /** Returns a fresh tuple cursor starting at the beginning of the batch. */
  public abstract LongTupleCursor tuples();

  /**
   * Returns a fresh values cursor iterating dense long values for doc-ids {@code [0, numDocs)}.
   * Must be overridden when {@link #density()} is {@link Column.Density#DENSE DENSE}; the default
   * implementation throws {@link UnsupportedOperationException} and is never called for {@link
   * Column.Density#SPARSE SPARSE} columns.
   */
  public LongValuesCursor values() {
    throw new UnsupportedOperationException(
        "values() requires density() == DENSE for column \"" + name() + "\"");
  }

  /** The numeric interpretation of the column's long values. */
  public final NumericKind numericKind() {
    return numericKind;
  }

  /**
   * The stored-field variant emitted for this column. The default derives from {@link
   * #numericKind()} — {@code INT→INTEGER}, {@code LONG→LONG}, {@code FLOAT→FLOAT}, {@code
   * DOUBLE→DOUBLE} — so a caller that wants the natural numeric variant does not need to override
   * this method. Only numeric {@link StoredValue.Type} values are permitted; non-numeric stored
   * data should use a {@link BinaryColumn}.
   */
  public StoredValue.Type storedType() {
    return switch (numericKind) {
      case INT -> StoredValue.Type.INTEGER;
      case LONG -> StoredValue.Type.LONG;
      case FLOAT -> StoredValue.Type.FLOAT;
      case DOUBLE -> StoredValue.Type.DOUBLE;
    };
  }

  /**
   * Returns a fresh tuple cursor yielding {@code (docID, sortable-point-bytes)} pairs suitable for
   * feeding {@link org.apache.lucene.index.PointValues}. The default implementation wraps {@link
   * #tuples()} and encodes each long value via {@link NumericUtils} according to {@link
   * #numericKind()} — {@code INT→intToSortableBytes}, {@code LONG→longToSortableBytes}, {@code
   * FLOAT→sortableFloatBits+intToSortableBytes}, {@code
   * DOUBLE→sortableDoubleBits+longToSortableBytes}. The returned {@link BytesRef} has length {@code
   * 4} for INT/FLOAT and {@code 8} for LONG/DOUBLE, and is reused across calls. Subclasses may
   * override when they already store sortable bytes.
   */
  public BinaryTupleCursor pointsTuples() {
    final LongTupleCursor inner = tuples();
    final NumericKind kind = numericKind;
    final byte[] scratch = new byte[pointByteWidth(kind)];
    final BytesRef ref = new BytesRef(scratch);
    return new BinaryTupleCursor() {
      @Override
      public int nextDoc() {
        return inner.nextDoc();
      }

      @Override
      public BytesRef binaryValue() {
        encodeSortableBytes(inner.longValue(), kind, scratch);
        return ref;
      }
    };
  }

  /**
   * Returns a fresh dense values cursor yielding consecutive sortable-point-bytes for doc-ids
   * {@code [0, numDocs)}. The default implementation wraps {@link #values()} and encodes each long
   * according to {@link #numericKind()}; each call to {@link BinaryValuesCursor#nextValues()} emits
   * one sortable-encoded value (of length {@code 4} for INT/FLOAT or {@code 8} for LONG/DOUBLE).
   * Must be overridden — or not invoked — when {@link #density()} is not {@link
   * Column.Density#DENSE DENSE}; the default throws {@link UnsupportedOperationException} in that
   * case. Subclasses may override when they already store sortable bytes.
   */
  public BinaryValuesCursor pointsValues() {
    if (density() != Density.DENSE) {
      throw new UnsupportedOperationException(
          "pointsValues() requires density() == DENSE for column \"" + name() + "\"");
    }
    final LongValuesCursor inner = values();
    final NumericKind kind = numericKind;
    final byte[] scratch = new byte[pointByteWidth(kind)];
    final BytesRef ref = new BytesRef(scratch);
    return new BinaryValuesCursor() {
      int remaining = inner.size();

      @Override
      public BytesRef nextValues() {
        if (remaining <= 0) {
          return null;
        }
        encodeSortableBytes(inner.nextLong(), kind, scratch);
        remaining--;
        return ref;
      }
    };
  }

  private static int pointByteWidth(NumericKind kind) {
    return (kind == NumericKind.INT || kind == NumericKind.FLOAT) ? Integer.BYTES : Long.BYTES;
  }

  private static void encodeSortableBytes(long raw, NumericKind kind, byte[] scratch) {
    switch (kind) {
      case INT -> NumericUtils.intToSortableBytes((int) raw, scratch, 0);
      case LONG -> NumericUtils.longToSortableBytes(raw, scratch, 0);
      case FLOAT -> {
        int sortable = NumericUtils.sortableFloatBits((int) raw);
        NumericUtils.intToSortableBytes(sortable, scratch, 0);
      }
      case DOUBLE -> {
        long sortable = NumericUtils.sortableDoubleBits(raw);
        NumericUtils.longToSortableBytes(sortable, scratch, 0);
      }
    }
  }
}
