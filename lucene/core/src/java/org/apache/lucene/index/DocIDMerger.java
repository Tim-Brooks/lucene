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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.search.DocIdSetIterator; // javadocs
import org.apache.lucene.util.LoserTree;

/**
 * Utility class to help merging documents from sub-readers according to either simple concatenated
 * (unsorted) order, or by a specified index-time sort, skipping deleted documents and remapping
 * non-deleted documents.
 */
public abstract class DocIDMerger<T extends DocIDMerger.Sub> {

  /** Represents one sub-reader being merged */
  public abstract static class Sub {
    /** Mapped doc ID */
    public int mappedDocID;

    /** Map from old to new doc IDs */
    public final MergeState.DocMap docMap;

    /** Sole constructor */
    protected Sub(MergeState.DocMap docMap) {
      this.docMap = docMap;
    }

    /**
     * Returns the next document ID from this sub reader, and {@link DocIdSetIterator#NO_MORE_DOCS}
     * when done
     */
    public abstract int nextDoc() throws IOException;

    /**
     * Like {@link #nextDoc()} but skips over unmapped docs and returns the next mapped doc ID, or
     * {@link DocIdSetIterator#NO_MORE_DOCS} when exhausted. This method sets {@link #mappedDocID}
     * as a side effect.
     */
    public final int nextMappedDoc() throws IOException {
      while (true) {
        int doc = nextDoc();
        if (doc == NO_MORE_DOCS) {
          return this.mappedDocID = NO_MORE_DOCS;
        }
        int mappedDoc = docMap.get(doc);
        if (mappedDoc != -1) {
          return this.mappedDocID = mappedDoc;
        }
      }
    }
  }

  /** Construct this from the provided subs, specifying the maximum sub count */
  public static <T extends DocIDMerger.Sub> DocIDMerger<T> of(
      List<T> subs, int maxCount, boolean indexIsSorted) throws IOException {
    if (indexIsSorted && maxCount > 1) {
      return new SortedDocIDMerger<>(subs, maxCount);
    } else {
      return new SequentialDocIDMerger<>(subs);
    }
  }

  /** Construct this from the provided subs */
  public static <T extends DocIDMerger.Sub> DocIDMerger<T> of(List<T> subs, boolean indexIsSorted)
      throws IOException {
    return of(subs, subs.size(), indexIsSorted);
  }

  /** Reuse API, currently only used by postings during merge */
  public abstract void reset() throws IOException;

  /**
   * Returns null when done. <b>NOTE:</b> after the iterator has exhausted you should not call this
   * method, as it may result in unpredicted behavior.
   */
  public abstract T next() throws IOException;

  private DocIDMerger() {}

  private static class SequentialDocIDMerger<T extends DocIDMerger.Sub> extends DocIDMerger<T> {

    private final List<T> subs;
    private T current;
    private int nextIndex;

    private SequentialDocIDMerger(List<T> subs) throws IOException {
      this.subs = subs;
      reset();
    }

    @Override
    public void reset() throws IOException {
      if (subs.size() > 0) {
        current = subs.get(0);
        nextIndex = 1;
      } else {
        current = null;
        nextIndex = 0;
      }
    }

    @Override
    public T next() throws IOException {
      while (current.nextMappedDoc() == NO_MORE_DOCS) {
        if (nextIndex == subs.size()) {
          current = null;
          return null;
        }
        current = subs.get(nextIndex);
        nextIndex++;
      }
      return current;
    }
  }

  private static class SortedDocIDMerger<T extends DocIDMerger.Sub> extends DocIDMerger<T> {

    private final List<T> subs;
    private T current;
    private final LoserTree<T> tree;
    private int queueMinDocID;

    private SortedDocIDMerger(List<T> subs, int maxCount) throws IOException {
      if (maxCount <= 1) {
        throw new IllegalArgumentException();
      }
      this.subs = subs;
      // One leaf per non-current sub. Sized once to the maximum (maxCount-1, since one sub is
      // always held out as `current`); the active leaf count is set per reset() to match the
      // current subs list, which lets this instance be reused across postings merges whose arity
      // varies per term.
      int capacity = maxCount - 1;
      // Keys are mappedDocIDs (non-negative ints or NO_MORE_DOCS); the int-keyed branchless tree
      // orders by (docID, leafIndex), eliminating data-dependent branches on the replay hot path.
      tree = LoserTree.create(capacity);
      reset();
    }

    private void setQueueMinDocID() {
      if (tree.size() > 0) {
        queueMinDocID = tree.topKey();
      } else {
        queueMinDocID = DocIdSetIterator.NO_MORE_DOCS;
      }
    }

    @Override
    public void reset() throws IOException {
      // Active leaves = all subs except the one held out as `current`. May vary per reset when
      // this merger is reused (e.g. per-term postings merges).
      tree.reset(Math.max(0, subs.size() - 1));
      current = null;
      boolean first = true;
      for (T sub : subs) {
        if (first) {
          // by setting mappedDocID = -1, this entry is guaranteed to win the first fast-path
          // check so the first call to next() will advance it
          sub.mappedDocID = -1;
          current = sub;
          first = false;
        } else {
          // Advance the sub (may be NO_MORE_DOCS for all-deleted segments); add it as a leaf
          // regardless so the tree is always fully populated. NO_MORE_DOCS subs become
          // permanent losers and are never surfaced as the champion.
          sub.nextMappedDoc();
          tree.add(sub.mappedDocID, sub);
        }
      }
      setQueueMinDocID();
    }

    @Override
    public T next() throws IOException {
      int nextDoc = current.nextMappedDoc();
      if (nextDoc < queueMinDocID) {
        // This should be the common case when index sorting is either disabled, or enabled on a
        // low-cardinality field, or enabled on a field that correlates with index order.
        return current;
      }
      // The cold path lives in a separate method so next() stays tiny and call-free on the hot
      // path. That lets C2 inline next() into the merge consumer and keep current/queueMinDocID in
      // registers, instead of being forced into a call-safe register layout around the
      // (non-inlined)
      // updateTop call — which otherwise shows up as hot-path self-time in next().
      return nextSlow();
    }

    private T nextSlow() {
      if (tree.size() == 0) {
        // No other subs — current (now exhausted) is the only one.
        current = null;
        return null;
      }

      // Slow path: current lost the race (its nextDoc >= tree minimum) or is exhausted.
      // Swap current into the tree and surface the tree's champion as the new current.
      // If current is NO_MORE_DOCS it becomes a permanent loser; termination is detected
      // uniformly below.
      T newCurrent = tree.top();
      tree.updateTop(current.mappedDocID, current);
      current = newCurrent;
      setQueueMinDocID();
      if (current.mappedDocID == NO_MORE_DOCS) {
        current = null;
        return null;
      }
      return current;
    }
  }
}
