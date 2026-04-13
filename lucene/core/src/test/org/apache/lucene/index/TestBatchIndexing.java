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
package org.apache.lucene.index;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.document.Batch;
import org.apache.lucene.document.BinaryColumn;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Column;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongColumn;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

/** Tests for column-oriented batch indexing via {@link IndexWriter#addBatch}. */
public class TestBatchIndexing extends LuceneTestCase {

  public void testNumericDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    long[] values = {10, 20, 30};
    int[] docIds = {0, 1, 2};
    w.addBatch(
        simpleBatch(3, new ArrayLongColumn("numeric", NumericDocValuesField.TYPE, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    NumericDocValues dv = leaf.getNumericDocValues("numeric");
    for (int i = 0; i < values.length; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(values[i], dv.longValue());
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

  public void testSortedNumericDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // Doc 0 has two values, doc 1 has one value
    int[] docIds = {0, 0, 1};
    long[] values = {5, 15, 25};
    w.addBatch(
        simpleBatch(
            2,
            new ArrayLongColumn(
                "sortedNumeric", SortedNumericDocValuesField.TYPE, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    SortedNumericDocValues dv = leaf.getSortedNumericDocValues("sortedNumeric");

    assertEquals(0, dv.nextDoc());
    assertEquals(2, dv.docValueCount());
    assertEquals(5, dv.nextValue());
    assertEquals(15, dv.nextValue());

    assertEquals(1, dv.nextDoc());
    assertEquals(1, dv.docValueCount());
    assertEquals(25, dv.nextValue());

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

  public void testBinaryDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    BytesRef[] values = {newBytesRef("aaa"), newBytesRef("bbb"), newBytesRef("ccc")};
    int[] docIds = {0, 1, 2};
    w.addBatch(
        simpleBatch(
            3, new ArrayBinaryColumn("binary", BinaryDocValuesField.TYPE, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    BinaryDocValues dv = leaf.getBinaryDocValues("binary");
    for (int i = 0; i < values.length; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(values[i], dv.binaryValue());
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

  public void testSortedDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    BytesRef[] values = {newBytesRef("x"), newBytesRef("y"), newBytesRef("x")};
    int[] docIds = {0, 1, 2};
    w.addBatch(
        simpleBatch(
            3, new ArrayBinaryColumn("sorted", SortedDocValuesField.TYPE, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    SortedDocValues dv = leaf.getSortedDocValues("sorted");

    assertEquals(0, dv.nextDoc());
    assertEquals(newBytesRef("x"), dv.lookupOrd(dv.ordValue()));
    assertEquals(1, dv.nextDoc());
    assertEquals(newBytesRef("y"), dv.lookupOrd(dv.ordValue()));
    assertEquals(2, dv.nextDoc());
    assertEquals(newBytesRef("x"), dv.lookupOrd(dv.ordValue()));

    // "x" and "y" should share ord space
    assertEquals(2, dv.getValueCount());

    r.close();
    w.close();
    dir.close();
  }

  public void testSortedSetDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // Doc 0 has two values, doc 1 has one value
    int[] docIds = {0, 0, 1};
    BytesRef[] values = {newBytesRef("a"), newBytesRef("b"), newBytesRef("a")};
    w.addBatch(
        simpleBatch(
            2, new ArrayBinaryColumn("sortedSet", SortedSetDocValuesField.TYPE, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    SortedSetDocValues dv = leaf.getSortedSetDocValues("sortedSet");

    assertEquals(0, dv.nextDoc());
    assertEquals(2, dv.docValueCount());
    assertEquals(newBytesRef("a"), dv.lookupOrd(dv.nextOrd()));
    assertEquals(newBytesRef("b"), dv.lookupOrd(dv.nextOrd()));

    assertEquals(1, dv.nextDoc());
    assertEquals(1, dv.docValueCount());
    assertEquals(newBytesRef("a"), dv.lookupOrd(dv.nextOrd()));

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

  public void testMultipleColumns() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    int[] allDocs = {0, 1, 2};
    long[] numericValues = {100, 200, 300};
    BytesRef[] sortedValues = {newBytesRef("a"), newBytesRef("b"), newBytesRef("c")};

    w.addBatch(
        simpleBatch(
            3,
            new ArrayLongColumn("numeric", NumericDocValuesField.TYPE, allDocs, numericValues),
            new ArrayBinaryColumn("sorted", SortedDocValuesField.TYPE, allDocs, sortedValues)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    NumericDocValues ndv = leaf.getNumericDocValues("numeric");
    SortedDocValues sdv = leaf.getSortedDocValues("sorted");
    for (int i = 0; i < 3; i++) {
      assertEquals(i, ndv.nextDoc());
      assertEquals(numericValues[i], ndv.longValue());
      assertEquals(i, sdv.nextDoc());
      assertEquals(sortedValues[i], sdv.lookupOrd(sdv.ordValue()));
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testSparseDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // Only doc 1 has a value (docs 0 and 2 are missing)
    int[] docIds = {1};
    long[] values = {42};
    w.addBatch(
        simpleBatch(3, new ArrayLongColumn("sparse", NumericDocValuesField.TYPE, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    NumericDocValues dv = leaf.getNumericDocValues("sparse");
    assertEquals(1, dv.nextDoc());
    assertEquals(42, dv.longValue());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());

    r.close();
    w.close();
    dir.close();
  }

  public void testParentFieldIndexed() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig();
    config.setParentField("_parent");
    IndexWriter w = new IndexWriter(dir, config);

    int[] docIds = {0, 1, 2};
    long[] values = {1, 2, 3};
    w.addBatch(
        simpleBatch(3, new ArrayLongColumn("numeric", NumericDocValuesField.TYPE, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    // Every batch doc should have the parent field
    NumericDocValues parentDv = leaf.getNumericDocValues("_parent");
    assertNotNull(parentDv);
    for (int i = 0; i < 3; i++) {
      assertEquals(i, parentDv.nextDoc());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testPointsColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // Create a points-only FieldType (1 dimension, Integer.BYTES)
    FieldType pointType = new FieldType();
    pointType.setDimensions(1, Integer.BYTES);
    pointType.freeze();

    int[] docIds = {0, 1, 2};
    BytesRef[] values = {IntPoint.pack(10), IntPoint.pack(20), IntPoint.pack(30)};
    w.addBatch(simpleBatch(3, new ArrayBinaryColumn("point", pointType, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);
    assertEquals(1, searcher.count(IntPoint.newExactQuery("point", 10)));
    assertEquals(1, searcher.count(IntPoint.newExactQuery("point", 20)));
    assertEquals(1, searcher.count(IntPoint.newExactQuery("point", 30)));
    assertEquals(0, searcher.count(IntPoint.newExactQuery("point", 99)));
    assertEquals(3, searcher.count(IntPoint.newRangeQuery("point", 10, 30)));

    r.close();
    w.close();
    dir.close();
  }

  public void testPointsWithDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // FieldType with both points and sorted doc values
    FieldType pointAndDvType = new FieldType();
    pointAndDvType.setDimensions(1, Integer.BYTES);
    pointAndDvType.setDocValuesType(DocValuesType.SORTED);
    pointAndDvType.freeze();

    int[] docIds = {0, 1, 2};
    BytesRef[] values = {IntPoint.pack(10), IntPoint.pack(20), IntPoint.pack(30)};
    w.addBatch(simpleBatch(3, new ArrayBinaryColumn("field", pointAndDvType, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);

    // Verify points
    assertEquals(1, searcher.count(IntPoint.newExactQuery("field", 10)));
    assertEquals(3, searcher.count(IntPoint.newRangeQuery("field", 10, 30)));

    // Verify doc values
    LeafReader leaf = getOnlyLeafReader(r);
    SortedDocValues dv = leaf.getSortedDocValues("field");
    for (int i = 0; i < 3; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(values[i], dv.lookupOrd(dv.ordValue()));
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testSparsePointsColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType pointType = new FieldType();
    pointType.setDimensions(1, Integer.BYTES);
    pointType.freeze();

    // Only doc 1 out of 3 has a point value
    int[] docIds = {1};
    BytesRef[] values = {IntPoint.pack(42)};
    w.addBatch(simpleBatch(3, new ArrayBinaryColumn("point", pointType, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);
    assertEquals(1, searcher.count(IntPoint.newExactQuery("point", 42)));
    assertEquals(0, searcher.count(IntPoint.newExactQuery("point", 0)));

    r.close();
    w.close();
    dir.close();
  }

  public void testColumnWithNoneDocValuesTypeAndNoPointsThrows() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // FieldType with NONE doc values type and no points
    FieldType badType = new FieldType();
    badType.freeze();

    int[] docIds = {0};
    long[] values = {1};
    expectThrows(
        IllegalArgumentException.class,
        () -> w.addBatch(simpleBatch(1, new ArrayLongColumn("bad", badType, docIds, values))));

    // Writer should still be usable after the failure
    w.addBatch(
        simpleBatch(
            1,
            new ArrayLongColumn(
                "numeric", NumericDocValuesField.TYPE, new int[] {0}, new long[] {42})));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    NumericDocValues dv = leaf.getNumericDocValues("numeric");
    assertNotNull(dv);
    // The failed batch's doc was marked deleted; the successful batch's doc is still live
    int doc = dv.nextDoc();
    assertTrue(doc != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(42, dv.longValue());

    r.close();
    w.close();
    dir.close();
  }

  // --- Test Column implementations backed by arrays ---

  private static Batch simpleBatch(int numDocs, Column... columns) {
    return new Batch() {
      @Override
      public int numDocs() {
        return numDocs;
      }

      @Override
      public Iterable<Column> columns() {
        return List.of(columns);
      }
    };
  }

  private static class ArrayLongColumn extends LongColumn {
    private final int[] docIds;
    private final long[] values;
    private int pos = -1;

    ArrayLongColumn(String name, IndexableFieldType fieldType, int[] docIds, long[] values) {
      super(name, fieldType);
      assert docIds.length == values.length;
      this.docIds = docIds;
      this.values = values;
    }

    @Override
    public int nextDoc() {
      pos++;
      return pos < docIds.length ? docIds[pos] : NO_MORE_DOCS;
    }

    @Override
    public long longValue() {
      return values[pos];
    }
  }

  private static class ArrayBinaryColumn extends BinaryColumn {
    private final int[] docIds;
    private final BytesRef[] values;
    private int pos = -1;

    ArrayBinaryColumn(String name, IndexableFieldType fieldType, int[] docIds, BytesRef[] values) {
      super(name, fieldType);
      assert docIds.length == values.length;
      this.docIds = docIds;
      this.values = values;
    }

    @Override
    public int nextDoc() {
      pos++;
      return pos < docIds.length ? docIds[pos] : NO_MORE_DOCS;
    }

    @Override
    public BytesRef binaryValue() {
      return values[pos];
    }
  }
}
