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
import java.nio.ByteOrder;
import java.util.List;
import org.apache.lucene.document.Batch;
import org.apache.lucene.document.BinaryColumn;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Column;
import org.apache.lucene.document.DenseBinaryColumn;
import org.apache.lucene.document.DenseLongColumn;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongColumn;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongsRef;

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
        simpleBatch(3, new ArrayBinaryColumn("binary", BinaryDocValuesField.TYPE, docIds, values)));

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
        simpleBatch(3, new ArrayBinaryColumn("sorted", SortedDocValuesField.TYPE, docIds, values)));

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

  public void testStoredLongColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // stored + NUMERIC doc values
    FieldType storedNumericType = new FieldType();
    storedNumericType.setStored(true);
    storedNumericType.setDocValuesType(DocValuesType.NUMERIC);
    storedNumericType.freeze();

    int[] docIds = {0, 1, 2};
    long[] values = {100, 200, 300};
    w.addBatch(simpleBatch(3, new ArrayLongColumn("val", storedNumericType, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    // Verify stored fields
    StoredFields storedFields = leaf.storedFields();
    for (int i = 0; i < 3; i++) {
      Document doc = storedFields.document(i);
      assertEquals(values[i], doc.getField("val").numericValue().longValue());
    }

    // Verify doc values
    NumericDocValues dv = leaf.getNumericDocValues("val");
    for (int i = 0; i < 3; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(values[i], dv.longValue());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testStoredBinaryColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // stored + SORTED doc values
    FieldType storedSortedType = new FieldType();
    storedSortedType.setStored(true);
    storedSortedType.setDocValuesType(DocValuesType.SORTED);
    storedSortedType.freeze();

    int[] docIds = {0, 1, 2};
    BytesRef[] values = {newBytesRef("aaa"), newBytesRef("bbb"), newBytesRef("ccc")};
    w.addBatch(simpleBatch(3, new ArrayBinaryColumn("val", storedSortedType, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    // Verify stored fields
    StoredFields storedFields = leaf.storedFields();
    for (int i = 0; i < 3; i++) {
      Document doc = storedFields.document(i);
      assertEquals(values[i], doc.getField("val").binaryValue());
    }

    // Verify doc values
    SortedDocValues dv = leaf.getSortedDocValues("val");
    for (int i = 0; i < 3; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(values[i], dv.lookupOrd(dv.ordValue()));
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testStoredOnlyColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // stored only — no doc values, no points
    FieldType storedOnlyType = new FieldType();
    storedOnlyType.setStored(true);
    storedOnlyType.freeze();

    int[] docIds = {0, 1, 2};
    long[] values = {10, 20, 30};
    w.addBatch(simpleBatch(3, new ArrayLongColumn("stored", storedOnlyType, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    StoredFields storedFields = leaf.storedFields();
    for (int i = 0; i < 3; i++) {
      Document doc = storedFields.document(i);
      assertEquals(values[i], doc.getField("stored").numericValue().longValue());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testMixedStoredAndNonStoredColumns() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType storedNumericType = new FieldType();
    storedNumericType.setStored(true);
    storedNumericType.setDocValuesType(DocValuesType.NUMERIC);
    storedNumericType.freeze();

    int[] allDocs = {0, 1, 2};
    long[] storedValues = {100, 200, 300};
    long[] dvOnlyValues = {1, 2, 3};
    w.addBatch(
        simpleBatch(
            3,
            new ArrayLongColumn("stored_field", storedNumericType, allDocs, storedValues),
            new ArrayLongColumn("dv_only", NumericDocValuesField.TYPE, allDocs, dvOnlyValues)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    // Verify stored field
    StoredFields storedFields = leaf.storedFields();
    for (int i = 0; i < 3; i++) {
      Document doc = storedFields.document(i);
      assertEquals(storedValues[i], doc.getField("stored_field").numericValue().longValue());
      assertNull(doc.getField("dv_only")); // non-stored column should not appear
    }

    // Verify both doc values columns
    NumericDocValues storedDv = leaf.getNumericDocValues("stored_field");
    NumericDocValues dvOnly = leaf.getNumericDocValues("dv_only");
    for (int i = 0; i < 3; i++) {
      assertEquals(i, storedDv.nextDoc());
      assertEquals(storedValues[i], storedDv.longValue());
      assertEquals(i, dvOnly.nextDoc());
      assertEquals(dvOnlyValues[i], dvOnly.longValue());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testStoredPointsColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // stored + points
    FieldType storedPointType = new FieldType();
    storedPointType.setStored(true);
    storedPointType.setDimensions(1, Integer.BYTES);
    storedPointType.freeze();

    int[] docIds = {0, 1, 2};
    BytesRef[] values = {IntPoint.pack(10), IntPoint.pack(20), IntPoint.pack(30)};
    w.addBatch(simpleBatch(3, new ArrayBinaryColumn("pt", storedPointType, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    // Verify stored fields
    StoredFields storedFields = leaf.storedFields();
    for (int i = 0; i < 3; i++) {
      Document doc = storedFields.document(i);
      assertEquals(values[i], doc.getField("pt").binaryValue());
    }

    // Verify points
    IndexSearcher searcher = new IndexSearcher(r);
    assertEquals(1, searcher.count(IntPoint.newExactQuery("pt", 10)));
    assertEquals(3, searcher.count(IntPoint.newRangeQuery("pt", 10, 30)));

    r.close();
    w.close();
    dir.close();
  }

  public void testInvertedColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // StringField-like: DOCS, omitNorms, non-tokenized
    FieldType stringType = new FieldType();
    stringType.setIndexOptions(IndexOptions.DOCS);
    stringType.setOmitNorms(true);
    stringType.setTokenized(false);
    stringType.freeze();

    int[] docIds = {0, 1, 2};
    BytesRef[] values = {newBytesRef("alpha"), newBytesRef("beta"), newBytesRef("alpha")};
    w.addBatch(simpleBatch(3, new ArrayBinaryColumn("tag", stringType, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);
    assertEquals(2, searcher.count(new TermQuery(new Term("tag", "alpha"))));
    assertEquals(1, searcher.count(new TermQuery(new Term("tag", "beta"))));
    assertEquals(0, searcher.count(new TermQuery(new Term("tag", "gamma"))));

    r.close();
    w.close();
    dir.close();
  }

  public void testInvertedWithDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // Inverted + SORTED doc values (like a StringField with doc values)
    FieldType invertedDvType = new FieldType();
    invertedDvType.setIndexOptions(IndexOptions.DOCS);
    invertedDvType.setOmitNorms(true);
    invertedDvType.setTokenized(false);
    invertedDvType.setDocValuesType(DocValuesType.SORTED);
    invertedDvType.freeze();

    int[] docIds = {0, 1, 2};
    BytesRef[] values = {newBytesRef("x"), newBytesRef("y"), newBytesRef("x")};
    w.addBatch(simpleBatch(3, new ArrayBinaryColumn("field", invertedDvType, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);

    // Verify inverted index
    assertEquals(2, searcher.count(new TermQuery(new Term("field", "x"))));
    assertEquals(1, searcher.count(new TermQuery(new Term("field", "y"))));

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

  public void testInvertedWithStored() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // Inverted + stored (like StringField with Store.YES)
    FieldType invertedStoredType = new FieldType(StringField.TYPE_STORED);
    invertedStoredType.freeze();

    int[] docIds = {0, 1, 2};
    BytesRef[] values = {newBytesRef("aaa"), newBytesRef("bbb"), newBytesRef("ccc")};
    w.addBatch(simpleBatch(3, new ArrayBinaryColumn("field", invertedStoredType, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);

    // Verify inverted index
    assertEquals(1, searcher.count(new TermQuery(new Term("field", "aaa"))));
    assertEquals(1, searcher.count(new TermQuery(new Term("field", "bbb"))));

    // Verify stored fields
    LeafReader leaf = getOnlyLeafReader(r);
    StoredFields storedFields = leaf.storedFields();
    for (int i = 0; i < 3; i++) {
      Document doc = storedFields.document(i);
      assertEquals(values[i], doc.getField("field").binaryValue());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testInvertedWithStoredAndDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // Inverted + stored + SORTED doc values
    FieldType allType = new FieldType();
    allType.setIndexOptions(IndexOptions.DOCS);
    allType.setOmitNorms(true);
    allType.setTokenized(false);
    allType.setStored(true);
    allType.setDocValuesType(DocValuesType.SORTED);
    allType.freeze();

    int[] docIds = {0, 1, 2};
    BytesRef[] values = {newBytesRef("x"), newBytesRef("y"), newBytesRef("z")};
    w.addBatch(simpleBatch(3, new ArrayBinaryColumn("field", allType, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    IndexSearcher searcher = new IndexSearcher(r);

    // Verify inverted index
    assertEquals(1, searcher.count(new TermQuery(new Term("field", "x"))));

    // Verify stored fields
    StoredFields storedFields = leaf.storedFields();
    for (int i = 0; i < 3; i++) {
      assertEquals(values[i], storedFields.document(i).getField("field").binaryValue());
    }

    // Verify doc values
    SortedDocValues dv = leaf.getSortedDocValues("field");
    for (int i = 0; i < 3; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(values[i], dv.lookupOrd(dv.ordValue()));
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testInvertedSparse() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    FieldType stringType = new FieldType();
    stringType.setIndexOptions(IndexOptions.DOCS);
    stringType.setOmitNorms(true);
    stringType.setTokenized(false);
    stringType.freeze();

    // Only doc 1 out of 3 has a term
    int[] docIds = {1};
    BytesRef[] values = {newBytesRef("found")};
    w.addBatch(simpleBatch(3, new ArrayBinaryColumn("tag", stringType, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);
    assertEquals(1, searcher.count(new TermQuery(new Term("tag", "found"))));

    r.close();
    w.close();
    dir.close();
  }

  public void testTokenizedColumn() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, config);

    // TextField-like: tokenized, DOCS_AND_FREQS_AND_POSITIONS
    int[] docIds = {0, 1, 2};
    BytesRef[] values = {
      newBytesRef("quick brown fox"), newBytesRef("lazy brown dog"), newBytesRef("quick fox jumps")
    };
    w.addBatch(
        simpleBatch(3, new ArrayBinaryColumn("text", TextField.TYPE_NOT_STORED, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher searcher = new IndexSearcher(r);

    // Each word was tokenized — verify individual terms
    assertEquals(2, searcher.count(new TermQuery(new Term("text", "quick"))));
    assertEquals(2, searcher.count(new TermQuery(new Term("text", "brown"))));
    assertEquals(2, searcher.count(new TermQuery(new Term("text", "fox"))));
    assertEquals(1, searcher.count(new TermQuery(new Term("text", "lazy"))));
    assertEquals(1, searcher.count(new TermQuery(new Term("text", "dog"))));
    assertEquals(1, searcher.count(new TermQuery(new Term("text", "jumps"))));
    assertEquals(0, searcher.count(new TermQuery(new Term("text", "missing"))));

    r.close();
    w.close();
    dir.close();
  }

  public void testTokenizedWithStored() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, config);

    int[] docIds = {0, 1};
    BytesRef[] values = {newBytesRef("hello world"), newBytesRef("goodbye world")};
    w.addBatch(
        simpleBatch(2, new ArrayBinaryColumn("text", TextField.TYPE_STORED, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    IndexSearcher searcher = new IndexSearcher(r);

    // Verify tokenized search
    assertEquals(2, searcher.count(new TermQuery(new Term("text", "world"))));
    assertEquals(1, searcher.count(new TermQuery(new Term("text", "hello"))));

    // Verify stored fields
    StoredFields storedFields = leaf.storedFields();
    assertEquals(values[0], storedFields.document(0).getField("text").binaryValue());
    assertEquals(values[1], storedFields.document(1).getField("text").binaryValue());

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

  public void testStoredIndexedWithDocValuesAndPoints() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // stored + indexed(DOCS, omitNorms) + SORTED DV + 4-byte points
    FieldType allType = new FieldType();
    allType.setStored(true);
    allType.setIndexOptions(IndexOptions.DOCS);
    allType.setOmitNorms(true);
    allType.setTokenized(false);
    allType.setDocValuesType(DocValuesType.SORTED);
    allType.setDimensions(1, Integer.BYTES);
    allType.freeze();

    int[] docIds = {0, 1, 2};
    BytesRef[] values = {
      newBytesRef(IntPoint.pack(10)), newBytesRef(IntPoint.pack(20)), newBytesRef(IntPoint.pack(30))
    };
    w.addBatch(simpleBatch(3, new ArrayBinaryColumn("field", allType, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    IndexSearcher searcher = new IndexSearcher(r);

    // Verify inverted index (terms)
    assertEquals(1, searcher.count(new TermQuery(new Term("field", values[0]))));
    assertEquals(1, searcher.count(new TermQuery(new Term("field", values[1]))));
    assertEquals(1, searcher.count(new TermQuery(new Term("field", values[2]))));

    // Verify stored fields
    StoredFields storedFields = leaf.storedFields();
    for (int i = 0; i < 3; i++) {
      assertEquals(values[i], storedFields.document(i).getField("field").binaryValue());
    }

    // Verify doc values
    SortedDocValues dv = leaf.getSortedDocValues("field");
    for (int i = 0; i < 3; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(values[i], dv.lookupOrd(dv.ordValue()));
    }

    // Verify points
    assertEquals(3, leaf.getPointValues("field").size());

    r.close();
    w.close();
    dir.close();
  }

  public void testMultiValuedStoredWithDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // stored + SORTED_NUMERIC doc values (multi-valued)
    FieldType storedSortedNumericType = new FieldType();
    storedSortedNumericType.setStored(true);
    storedSortedNumericType.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    storedSortedNumericType.freeze();

    // Doc 0 has two values (10, 20), doc 1 has one value (30)
    int[] docIds = {0, 0, 1};
    long[] values = {10, 20, 30};
    w.addBatch(simpleBatch(2, new ArrayLongColumn("val", storedSortedNumericType, docIds, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    // Verify stored fields — each value occurrence is stored separately
    StoredFields storedFields = leaf.storedFields();
    Document doc0 = storedFields.document(0);
    assertEquals(2, doc0.getFields("val").length);
    assertEquals(10L, doc0.getFields("val")[0].numericValue().longValue());
    assertEquals(20L, doc0.getFields("val")[1].numericValue().longValue());
    Document doc1 = storedFields.document(1);
    assertEquals(1, doc1.getFields("val").length);
    assertEquals(30L, doc1.getFields("val")[0].numericValue().longValue());

    // Verify doc values
    SortedNumericDocValues dv = leaf.getSortedNumericDocValues("val");
    assertEquals(0, dv.nextDoc());
    assertEquals(2, dv.docValueCount());
    assertEquals(10, dv.nextValue());
    assertEquals(20, dv.nextValue());
    assertEquals(1, dv.nextDoc());
    assertEquals(1, dv.docValueCount());
    assertEquals(30, dv.nextValue());

    r.close();
    w.close();
    dir.close();
  }

  public void testDenseNumericDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    long[] values = {100, 200, 300};
    w.addBatch(simpleBatch(3, new ArrayDenseLongColumn("val", NumericDocValuesField.TYPE, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    NumericDocValues dv = leaf.getNumericDocValues("val");
    for (int i = 0; i < 3; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(values[i], dv.longValue());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testDenseSortedNumericDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    long[] values = {10, 20, 30, 40, 50};
    w.addBatch(
        simpleBatch(5, new ArrayDenseLongColumn("val", SortedNumericDocValuesField.TYPE, values)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    SortedNumericDocValues dv = leaf.getSortedNumericDocValues("val");
    for (int i = 0; i < 5; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(1, dv.docValueCount());
      assertEquals(values[i], dv.nextValue());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testDenseColumnCountMismatchThrows() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // 2 values but batch expects 3 documents
    long[] values = {10, 20};
    expectThrows(
        IllegalArgumentException.class,
        () ->
            w.addBatch(
                simpleBatch(
                    3, new ArrayDenseLongColumn("val", NumericDocValuesField.TYPE, values))));

    // Writer should still be usable after the failure — use a different field to avoid
    // the partially-written DV entries from the failed batch
    w.addBatch(
        simpleBatch(
            1, new ArrayDenseLongColumn("val2", NumericDocValuesField.TYPE, new long[] {42})));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    NumericDocValues dv = leaf.getNumericDocValues("val2");
    assertNotNull(dv);
    int doc = dv.nextDoc();
    assertTrue(doc != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(42, dv.longValue());

    r.close();
    w.close();
    dir.close();
  }

  public void testDenseColumnTooManyValuesThrows() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // 5 values but batch only has 3 documents
    long[] values = {10, 20, 30, 40, 50};
    expectThrows(
        IllegalArgumentException.class,
        () ->
            w.addBatch(
                simpleBatch(
                    3, new ArrayDenseLongColumn("val", NumericDocValuesField.TYPE, values))));

    // Writer should still be usable — no values were written past numDocs
    w.addBatch(
        simpleBatch(
            1, new ArrayDenseLongColumn("val2", NumericDocValuesField.TYPE, new long[] {42})));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    NumericDocValues dv = leaf.getNumericDocValues("val2");
    assertNotNull(dv);
    int doc = dv.nextDoc();
    assertTrue(doc != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(42, dv.longValue());

    r.close();
    w.close();
    dir.close();
  }

  public void testDenseBinaryNumericDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    long[] values = {100, 200, 300};
    byte[] bytes = longsToBytes(values, ByteOrder.LITTLE_ENDIAN);
    w.addBatch(
        simpleBatch(
            3,
            new ArrayDenseBinaryColumn(
                "val", NumericDocValuesField.TYPE, ByteOrder.LITTLE_ENDIAN, bytes)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    NumericDocValues dv = leaf.getNumericDocValues("val");
    for (int i = 0; i < 3; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(values[i], dv.longValue());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testDenseBinarySortedNumericDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    long[] values = {10, 20, 30, 40, 50};
    byte[] bytes = longsToBytes(values, ByteOrder.BIG_ENDIAN);
    w.addBatch(
        simpleBatch(
            5,
            new ArrayDenseBinaryColumn(
                "val", SortedNumericDocValuesField.TYPE, ByteOrder.BIG_ENDIAN, bytes)));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);

    SortedNumericDocValues dv = leaf.getSortedNumericDocValues("val");
    for (int i = 0; i < 5; i++) {
      assertEquals(i, dv.nextDoc());
      assertEquals(1, dv.docValueCount());
      assertEquals(values[i], dv.nextValue());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testDenseBinaryColumnTooManyValuesThrows() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // 5 values but batch only has 3 documents
    long[] values = {10, 20, 30, 40, 50};
    byte[] bytes = longsToBytes(values, ByteOrder.LITTLE_ENDIAN);
    expectThrows(
        IllegalArgumentException.class,
        () ->
            w.addBatch(
                simpleBatch(
                    3,
                    new ArrayDenseBinaryColumn(
                        "val", NumericDocValuesField.TYPE, ByteOrder.LITTLE_ENDIAN, bytes))));

    // Writer should still be usable
    w.addBatch(
        simpleBatch(
            1,
            new ArrayDenseBinaryColumn(
                "val2",
                NumericDocValuesField.TYPE,
                ByteOrder.LITTLE_ENDIAN,
                longsToBytes(new long[] {42}, ByteOrder.LITTLE_ENDIAN))));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    NumericDocValues dv = leaf.getNumericDocValues("val2");
    assertNotNull(dv);
    int doc = dv.nextDoc();
    assertTrue(doc != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(42, dv.longValue());

    r.close();
    w.close();
    dir.close();
  }

  public void testDenseBinaryColumnCountMismatchThrows() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // 2 values but batch expects 3 documents
    long[] values = {10, 20};
    byte[] bytes = longsToBytes(values, ByteOrder.LITTLE_ENDIAN);
    expectThrows(
        IllegalArgumentException.class,
        () ->
            w.addBatch(
                simpleBatch(
                    3,
                    new ArrayDenseBinaryColumn(
                        "val", NumericDocValuesField.TYPE, ByteOrder.LITTLE_ENDIAN, bytes))));

    // Writer should still be usable
    w.addBatch(
        simpleBatch(
            1,
            new ArrayDenseBinaryColumn(
                "val2",
                NumericDocValuesField.TYPE,
                ByteOrder.LITTLE_ENDIAN,
                longsToBytes(new long[] {42}, ByteOrder.LITTLE_ENDIAN))));

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    NumericDocValues dv = leaf.getNumericDocValues("val2");
    assertNotNull(dv);
    int doc = dv.nextDoc();
    assertTrue(doc != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(42, dv.longValue());

    r.close();
    w.close();
    dir.close();
  }

  private static byte[] longsToBytes(long[] values, ByteOrder byteOrder) {
    byte[] bytes = new byte[values.length * Long.BYTES];
    java.lang.invoke.VarHandle vh =
        byteOrder == ByteOrder.LITTLE_ENDIAN
            ? org.apache.lucene.util.BitUtil.VH_LE_LONG
            : org.apache.lucene.util.BitUtil.VH_BE_LONG;
    for (int i = 0; i < values.length; i++) {
      vh.set(bytes, i * Long.BYTES, values[i]);
    }
    return bytes;
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

    @Override
    public void reset() {
      pos = -1;
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

    @Override
    public void reset() {
      pos = -1;
    }
  }

  private static class ArrayDenseLongColumn extends DenseLongColumn {

    private final LongsRef ref;
    private boolean exhausted;

    ArrayDenseLongColumn(String name, IndexableFieldType fieldType, long[] values) {
      super(name, fieldType);
      this.ref = new LongsRef(values, 0, values.length);
      this.exhausted = false;
    }

    @Override
    public LongsRef nextLongs() {
      if (exhausted) return null;
      exhausted = true;
      return ref;
    }

    @Override
    public void reset() {
      exhausted = false;
    }
  }

  private static class ArrayDenseBinaryColumn extends DenseBinaryColumn {
    private final BytesRef ref;
    private boolean exhausted;

    ArrayDenseBinaryColumn(
        String name, IndexableFieldType fieldType, ByteOrder byteOrder, byte[] bytes) {
      super(name, fieldType, byteOrder);
      this.ref = new BytesRef(bytes, 0, bytes.length);
      this.exhausted = false;
    }

    @Override
    public BytesRef nextBytes() {
      if (exhausted) return null;
      exhausted = true;
      return ref;
    }

    @Override
    public void reset() {
      exhausted = false;
    }
  }
}
