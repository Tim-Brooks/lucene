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

/**
 * A tournament (loser) tree for k-way merging. Keys must be non-negative ints; each internal node
 * stores the packed {@code (key << 32) | leafIndex} value of the loser of the match played at that
 * node. The current winner (minimum key) is tracked separately as the champion.
 *
 * <p>By specializing the comparison key to an {@code int} and packing {@code (key << 32) |
 * leafIndex} into a single {@code long}, the hot-path {@link #replay} is completely branchless:
 * each level is one load, one {@code Math.min} (intrinsified by C2 to a conditional move), two
 * xors, and one store — no data-dependent branch. This eliminates the branch-misprediction penalty
 * that dominates when comparison outcomes are near-random (the interleaved segment-merge case).
 * Long ordering equals {@code (key, leafIndex)} order, giving a deterministic tie-break by
 * insertion order.
 *
 * <p>Usage: call {@link #add} once per leaf (leaves 0..numLeaves-1 in order) until the tree is full
 * ({@code size == numLeaves}), then alternate between reading {@link #top}/{@link #topKey} and
 * advancing via {@link #updateTop(int)} or {@link #updateTop(int, Object)}.
 *
 * <p>The arrays are sized once to a fixed <i>capacity</i> at construction. The number of leaves
 * actually contested can be smaller and may vary between rounds: call {@link #reset(int)} to set a
 * new active leaf count ({@code <= capacity}) and start a fresh {@link #add} cycle, which is what
 * allows a single instance to be reused across many merges of differing arity (e.g. per-term
 * postings merges).
 *
 * <p>Exhausted inputs should be represented by a sentinel key of {@link Integer#MAX_VALUE} (e.g.
 * {@link org.apache.lucene.search.DocIdSetIterator#NO_MORE_DOCS}). Such sentinels become permanent
 * losers and are never surfaced as the champion. Keys must be {@code >= 0}.
 *
 * @lucene.internal
 */
public final class LoserTree<T> {

  private final T[] leaves;

  /**
   * Internal loser nodes. {@code tree[k]} stores the packed {@code (key << 32) | leafIndex} of the
   * loser of the match at node {@code k}. Index 0 is unused; internal nodes occupy {@code
   * tree[1..numLeaves-1]}.
   */
  private final long[] tree;

  /**
   * Packed {@code (key << 32) | leafIndex} for each leaf position; populated during {@link #add}
   * and consumed by {@link #build}.
   */
  private final long[] packedLeaves;

  /**
   * Packed {@code (key << 32) | leafIndex} of the current minimum (champion). {@code -1L} when the
   * tree is not yet built.
   */
  private long champion;

  /** Number of leaves added so far via {@link #add}. */
  private int size;

  /**
   * Number of leaves contested in the current round. Always {@code <= leaves.length} (the
   * capacity). The tournament is built once {@code size == numLeaves}, and {@link #build}, {@link
   * #replay} and {@link #top} all operate over {@code leaves[0..numLeaves-1]}.
   */
  private int numLeaves;

  @SuppressWarnings("unchecked")
  private LoserTree(int capacity) {
    if (capacity < 0) {
      throw new IllegalArgumentException("capacity must be >= 0; got: " + capacity);
    }
    this.leaves = (T[]) new Object[capacity];
    // Index 0 unused; indices 1..capacity-1 are internal loser nodes (n-1 nodes for n leaves).
    this.tree = new long[capacity];
    this.packedLeaves = new long[capacity];
    this.champion = -1L;
    this.size = 0;
    this.numLeaves = capacity;
  }

  /**
   * Creates a loser tree ordered by non-negative int keys. {@code capacity} is the maximum number
   * of leaves; the active leaf count defaults to {@code capacity} and can be lowered per round with
   * {@link #reset(int)}.
   */
  public static <T> LoserTree<T> create(int capacity) {
    return new LoserTree<>(capacity);
  }

  /**
   * Adds the next leaf element with the given key. Leaves must be added in order from 0 to {@code
   * numLeaves-1}. The tournament is built automatically when the last leaf is added.
   *
   * @param key the ordering key; must be {@code >= 0}
   * @param element the leaf element
   * @throws ArrayIndexOutOfBoundsException if called more than {@code numLeaves} times
   */
  public void add(int key, T element) {
    assert key >= 0 : "key must be non-negative; got: " + key;
    int idx = size++;
    leaves[idx] = element;
    packedLeaves[idx] = ((long) key << 32) | idx;
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
    champion = -1L;
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
    champion = -1L;
  }

  /**
   * Returns the minimum element (the champion), or {@code null} if the tree is not yet built. Valid
   * only after all {@code numLeaves} elements have been added via {@link #add}.
   */
  public T top() {
    return champion >= 0 ? leaves[(int) champion] : null;
  }

  /**
   * Returns the key of the current minimum (champion). Valid only after the tree is built; returns
   * {@code -1} when not built.
   */
  public int topKey() {
    return champion >= 0 ? (int) (champion >>> 32) : -1;
  }

  /**
   * Called after the champion element has been mutated in place by the caller, with a new key.
   * Replays the champion's leaf-to-root path to find the new minimum.
   *
   * @param newKey the updated key for the champion leaf; must be {@code >= 0}
   * @return the new minimum element
   */
  public T updateTop(int newKey) {
    assert checkUpdateTop(newKey);
    long contender = ((long) newKey << 32) | (champion & 0xFFFFFFFFL);
    replay(contender);
    return leaves[(int) champion];
  }

  /**
   * Replaces the champion element with {@code newElement} and replays the champion's leaf-to-root
   * path to find the new minimum.
   *
   * @param newKey the key for the new element; must be {@code >= 0}
   * @param newElement the new element to place at the champion's leaf position
   * @return the new minimum element
   */
  public T updateTop(int newKey, T newElement) {
    assert checkUpdateTop(newKey);
    int leafIdx = (int) champion;
    leaves[leafIdx] = newElement;
    long contender = ((long) newKey << 32) | leafIdx;
    replay(contender);
    return leaves[(int) champion];
  }

  private boolean checkUpdateTop(int newKey) {
    assert newKey >= 0 : "key must be non-negative; got: " + newKey;
    assert champion >= 0 : "tree is not yet built";
    return true;
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
      champion = packedLeaves[0];
      return;
    }
    // -1L marks an uninitialized internal node (no loser assigned yet; valid packed values are >=
    // 0).
    Arrays.fill(tree, 1, n, -1L);
    champion = -1L;
    for (int i = 0; i < n; i++) {
      // Walk leaf i up to the root. Parent of conceptual position (n+i) is (n+i)>>1.
      long winner = packedLeaves[i];
      int pos = (n + i) >> 1;
      while (pos > 0) {
        long stored = tree[pos];
        if (stored == -1L) {
          // First arrival at this node: park as placeholder loser; winner does not continue.
          tree[pos] = winner;
          winner = -1L;
          break;
        }
        if (winner >= stored) {
          // Winner loses this match (larger packed value = larger key or same key + larger index):
          // becomes new loser; stored loser becomes new winner.
          long tmp = winner;
          winner = stored;
          tree[pos] = tmp;
        }
        // Winner (unchanged or swapped) continues up.
        pos >>= 1;
      }
      if (winner != -1L) {
        // Reached the root (pos == 0): this is the tournament champion so far.
        champion = winner;
      }
    }
  }

  /**
   * Replays the champion's leaf-to-root path after its key/element has changed. The traversal is
   * branchless: {@code Math.min} (intrinsified by C2 to a conditional move) selects the winner and
   * the XOR trick stores the loser — no data-dependent branch on the hot path.
   *
   * <p>XOR trick: {@code winner ^ stored ^ min} equals the non-{@code min} of the two values (the
   * loser), because if {@code min == winner} then {@code winner ^ stored ^ winner == stored}, and
   * if {@code min == stored} then {@code winner ^ stored ^ stored == winner}.
   */
  private void replay(long contender) {
    int n = numLeaves;
    if (n <= 1) {
      // 0 or 1 leaf: champion is just the contender.
      champion = contender;
      return;
    }
    int pos = (n + (int) contender) >> 1; // (int) contender extracts the leaf index
    long winner = contender;
    while (pos > 0) {
      long stored = tree[pos];
      long min = Math.min(winner, stored); // C2 intrinsifies to cmov — branchless
      tree[pos] = winner ^ stored ^ min; // loser (max) stays at the node
      winner = min;
      pos >>= 1;
    }
    champion = winner;
  }
}
