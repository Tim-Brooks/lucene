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

import java.util.Arrays;
import java.util.Comparator;

/**
 * A tournament (loser) tree for k-way merging. Each internal node stores the index of the loser of
 * the match played at that node; the current winner (minimum element) is tracked separately as the
 * champion.
 *
 * <p>After all leaves are filled via {@link #add}, replaying after the champion advances costs at
 * most &lceil;log&#8322;(k)&rceil; comparisons (the depth of the champion's leaf, which is
 * &lfloor;log&#8322;(k)&rfloor; or &lceil;log&#8322;(k)&rceil; depending on the leaf) — roughly
 * half the cost of a binary heap — because the leaf-to-root path is fixed and requires no
 * child-vs-child comparisons. The traversal path is also data-independent, which is friendlier to
 * branch prediction and cache than a heap sift-down.
 *
 * <p>Usage: call {@link #add} once per leaf (leaves 0..numLeaves-1 in order) until the tree is full
 * (size == numLeaves), then alternate between reading {@link #top} and advancing via {@link
 * #updateTop} or {@link #updateTop(Object)}.
 *
 * <p>The arrays are sized once to a fixed <i>capacity</i> at construction. The number of leaves
 * actually contested can be smaller and may vary between rounds: call {@link #reset(int)} to set a
 * new active leaf count (&le; capacity) and start a fresh {@link #add} cycle, which is what allows
 * a single instance to be reused across many merges of differing arity (e.g. per-term postings
 * merges). The active count defaults to the full capacity.
 *
 * <p>Exhausted inputs should be represented by a sentinel element that compares greater than every
 * real element (e.g. {@link Integer#MAX_VALUE} or {@code DocIdSetIterator.NO_MORE_DOCS}). Such
 * sentinels become permanent losers and are never surfaced as the champion.
 *
 * @lucene.internal
 */
public final class LoserTree<T> {

  private final PriorityQueue.LessThan<? super T> lessThan;

  private final T[] leaves;

  /**
   * Internal loser nodes. {@code tree[k]} stores the leaf index of the loser of the match at node
   * {@code k}. Index 0 is unused; internal nodes occupy {@code tree[1..numLeaves-1]}.
   */
  private final int[] tree;

  /** Leaf index of the current minimum (champion). -1 when the tree is not yet built. */
  private int champion;

  /** Number of leaves added so far via {@link #add}. */
  private int size;

  /**
   * Number of leaves contested in the current round. Always {@code <= leaves.length} (the
   * capacity). The tournament is built once {@code size == numLeaves}, and {@link #build}, {@link
   * #replay} and {@link #top} all operate over {@code leaves[0..numLeaves-1]}.
   */
  private int numLeaves;

  @SuppressWarnings("unchecked")
  private LoserTree(int capacity, PriorityQueue.LessThan<? super T> lessThan) {
    if (capacity < 0) {
      throw new IllegalArgumentException("capacity must be >= 0; got: " + capacity);
    }
    this.lessThan = lessThan;
    this.leaves = (T[]) new Object[capacity];
    // Index 0 unused; indices 1..capacity-1 are internal loser nodes (n-1 nodes for n leaves).
    this.tree = new int[capacity];
    this.champion = -1;
    this.size = 0;
    this.numLeaves = capacity;
  }

  /**
   * Creates a loser tree ordered by the provided comparator. {@code capacity} is the maximum number
   * of leaves; the active leaf count defaults to {@code capacity} and can be lowered per round with
   * {@link #reset(int)}.
   */
  public static <T> LoserTree<T> usingComparator(int capacity, Comparator<? super T> comparator) {
    return new LoserTree<>(capacity, (a, b) -> comparator.compare(a, b) < 0);
  }

  /**
   * Creates a loser tree ordered by the provided {@link PriorityQueue.LessThan}. {@code capacity}
   * is the maximum number of leaves; the active leaf count defaults to {@code capacity} and can be
   * lowered per round with {@link #reset(int)}.
   */
  public static <T> LoserTree<T> usingLessThan(
      int capacity, PriorityQueue.LessThan<? super T> lessThan) {
    return new LoserTree<>(capacity, lessThan);
  }

  /**
   * Adds the next leaf element. Leaves must be added in order from 0 to {@code numLeaves-1}. The
   * tournament is built automatically when the last leaf is added.
   *
   * @throws ArrayIndexOutOfBoundsException if called more than {@code numLeaves} times
   */
  public void add(T element) {
    leaves[size++] = element;
    if (size == numLeaves) {
      build();
    }
  }

  /** Returns the number of leaves added so far. */
  public int size() {
    return size;
  }

  /**
   * Resets to empty, keeping the current active leaf count, ready for a fresh {@link #add} cycle.
   */
  public void clear() {
    Arrays.fill(leaves, 0, numLeaves, null);
    size = 0;
    champion = -1;
  }

  /**
   * Resets to empty and sets a new active leaf count for the next {@link #add} cycle. Use this to
   * reuse a single instance across rounds whose arity differs (the arrays are sized once to the
   * capacity passed at construction).
   *
   * @param numLeaves the number of leaves that will be added this round; must be in {@code [0,
   *     capacity]}
   */
  public void reset(int numLeaves) {
    if (numLeaves < 0 || numLeaves > leaves.length) {
      throw new IllegalArgumentException(
          "numLeaves must be in [0, " + leaves.length + "]; got: " + numLeaves);
    }
    // Null out whichever range was previously active to release references.
    Arrays.fill(leaves, 0, Math.max(this.numLeaves, numLeaves), null);
    this.numLeaves = numLeaves;
    size = 0;
    champion = -1;
  }

  /**
   * Returns the minimum element (the champion), or {@code null} if the tree is not yet built. Valid
   * only after all {@code numLeaves} elements have been added via {@link #add}.
   */
  public T top() {
    return champion >= 0 ? leaves[champion] : null;
  }

  /**
   * Called after the champion element has been mutated in place by the caller. Replays the
   * champion's leaf-to-root path to find the new minimum.
   *
   * @return the new minimum element
   */
  public T updateTop() {
    assert champion >= 0 : "tree is not yet built";
    replay();
    return leaves[champion];
  }

  /**
   * Replaces the champion element with {@code newTop} and replays the champion's leaf-to-root path
   * to find the new minimum.
   *
   * @return the new minimum element
   */
  public T updateTop(T newTop) {
    assert champion >= 0 : "tree is not yet built";
    leaves[champion] = newTop;
    replay();
    return leaves[champion];
  }

  /**
   * Builds the tournament from scratch after all leaves are filled.
   *
   * <p>Algorithm: insert leaves 0..n-1 one by one, walking each leaf up to the root. Each internal
   * node stores the loser of its first contested match; the overall winner ends up in {@code
   * champion}. Build is O(n) amortized (each node is contested at most twice across all
   * insertions).
   */
  private void build() {
    int n = numLeaves;
    if (n == 1) {
      champion = 0;
      return;
    }
    // -1 marks an uninitialized internal node (no loser assigned yet).
    Arrays.fill(tree, 1, n, -1);
    champion = -1;
    for (int i = 0; i < n; i++) {
      // Walk leaf i up to the root. Parent of conceptual position (n+i) is (n+i)>>1.
      int winner = i;
      int pos = (n + i) >> 1;
      while (pos > 0) {
        if (tree[pos] == -1) {
          // First arrival at this node: park as placeholder loser; winner does not continue.
          tree[pos] = winner;
          winner = -1;
          break;
        }
        int loserLeaf = tree[pos];
        if (!lessThan.lessThan(leaves[winner], leaves[loserLeaf])) {
          // Winner loses this match: becomes new loser; stored loser becomes new winner.
          tree[pos] = winner;
          winner = loserLeaf;
        }
        // Winner (unchanged or swapped) continues up.
        pos >>= 1;
      }
      if (winner >= 0) {
        // Reached the root (pos == 0): this is the tournament champion.
        champion = winner;
      }
    }
  }

  /**
   * Replays the champion's leaf-to-root path after its element has changed. At each internal node,
   * the carried winner is compared against the stored loser; on a loss they swap. The survivor
   * reaching the root becomes the new champion.
   */
  private void replay() {
    int n = numLeaves;
    if (n <= 1) {
      return; // 0 or 1 leaf: nothing to replay
    }
    int winner = champion;
    int pos = (n + winner) >> 1;
    while (pos > 0) {
      int loserLeaf = tree[pos];
      if (!lessThan.lessThan(leaves[winner], leaves[loserLeaf])) {
        tree[pos] = winner;
        winner = loserLeaf;
      }
      pos >>= 1;
    }
    champion = winner;
  }
}
