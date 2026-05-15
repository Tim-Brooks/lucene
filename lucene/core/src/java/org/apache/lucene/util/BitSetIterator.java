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
package org.apache.lucene.util;

import java.io.IOException;
import org.apache.lucene.search.AbstractDocIdSetIterator;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * A {@link DocIdSetIterator} which iterates over set bits in a bit set.
 *
 * @lucene.internal
 */
public class BitSetIterator extends AbstractDocIdSetIterator {

  private static <T extends BitSet> T getBitSet(
      DocIdSetIterator iterator, Class<? extends T> clazz) {
    if (iterator instanceof BitSetIterator) {
      BitSet bits = ((BitSetIterator) iterator).bits;
      assert bits != null;
      if (clazz.isInstance(bits)) {
        return clazz.cast(bits);
      }
    }
    return null;
  }

  /** If the provided iterator wraps a {@link FixedBitSet}, returns it, otherwise returns null. */
  public static FixedBitSet getFixedBitSetOrNull(DocIdSetIterator iterator) {
    return getBitSet(iterator, FixedBitSet.class);
  }

  /**
   * If the provided iterator wraps a {@link SparseFixedBitSet}, returns it, otherwise returns null.
   */
  public static SparseFixedBitSet getSparseFixedBitSetOrNull(DocIdSetIterator iterator) {
    return getBitSet(iterator, SparseFixedBitSet.class);
  }

  private final BitSet bits;
  private final int length;
  private final long cost;

  // Word-level state for FixedBitSet fast path. null when bits is not a FixedBitSet.
  private final long[] words;
  // Current word index. -1 is a sentinel meaning the next advance() must reload the word, used to
  // handle setDocId() repositioning and initial state uniformly.
  private int wordIndex;
  private long currentWord;

  /** Sole constructor. */
  public BitSetIterator(BitSet bits, long cost) {
    if (cost < 0) {
      throw new IllegalArgumentException("cost must be >= 0, got " + cost);
    }
    this.bits = bits;
    this.length = bits.length();
    this.cost = cost;
    this.words = bits instanceof FixedBitSet fbs ? fbs.getBits() : null;
    this.wordIndex = -1;
    this.currentWord = 0;
  }

  /** Return the wrapped {@link BitSet}. */
  public BitSet getBitSet() {
    return bits;
  }

  /** Set the current doc id that this iterator is on. */
  public void setDocId(int docId) {
    this.doc = docId;
    if (words != null) {
      wordIndex = -1;
    }
  }

  @Override
  public int nextDoc() {
    if (words != null) {
      if (wordIndex == -1) {
        return advanceFixed(doc + 1);
      }
      currentWord &= currentWord - 1; // clear the bit we just returned
      if (currentWord != 0) {
        return doc = (wordIndex << 6) + Long.numberOfTrailingZeros(currentWord);
      }
      return nextDocAdvanceWord();
    }
    return advance(doc + 1);
  }

  private int nextDocAdvanceWord() {
    while (++wordIndex < words.length) {
      currentWord = words[wordIndex];
      if (currentWord != 0) {
        return doc = (wordIndex << 6) + Long.numberOfTrailingZeros(currentWord);
      }
    }
    return doc = NO_MORE_DOCS;
  }

  @Override
  public int advance(int target) {
    assert docID() < target;
    if (words != null) {
      return advanceFixed(target);
    }
    if (target >= length) {
      return doc = NO_MORE_DOCS;
    }
    return doc = bits.nextSetBit(target);
  }

  private int advanceFixed(int target) {
    if (target >= length) {
      return doc = NO_MORE_DOCS;
    }
    int newWordIndex = target >> 6;
    if (newWordIndex != wordIndex) {
      wordIndex = newWordIndex;
      currentWord = words[wordIndex];
    }
    // Clear bits below target within the current word. When called from nextDoc() with
    // target = doc + 1, this clears the bit we just returned along with anything below it.
    currentWord &= ~0L << (target & 63);

    while (currentWord == 0) {
      if (++wordIndex >= words.length) {
        return doc = NO_MORE_DOCS;
      }
      currentWord = words[wordIndex];
    }
    return doc = (wordIndex << 6) + Long.numberOfTrailingZeros(currentWord);
  }

  @Override
  public long cost() {
    return cost;
  }

  @Override
  public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
    if (upTo > doc && bits instanceof FixedBitSet fixedBits) {
      int actualUpto = Math.min(upTo, length);
      // The destination bit set may be shorter than this bit set. This is only legal if all bits
      // beyond offset + bitSet.length() are clear. If not, the below call to `super.intoBitSet`
      // will throw an exception.
      actualUpto = MathUtil.unsignedMin(actualUpto, offset + bitSet.length());
      FixedBitSet.orRange(fixedBits, doc, bitSet, doc - offset, actualUpto - doc);
      advance(actualUpto); // set the current doc, updating word-level state via advanceFixed
    }
    super.intoBitSet(upTo, bitSet, offset);
  }
}
