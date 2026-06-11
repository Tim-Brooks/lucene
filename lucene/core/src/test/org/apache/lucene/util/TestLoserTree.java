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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestLoserTree extends LuceneTestCase {

  // Each leaf is an int[]{sourceIndex, currentValue}; tree comparator uses currentValue.
  private static LoserTree<int[]> buildTree(int[][] leafRefs) {
    LoserTree<int[]> tree =
        LoserTree.usingComparator(leafRefs.length, (a, b) -> Integer.compare(a[1], b[1]));
    for (int[] leaf : leafRefs) tree.add(leaf);
    return tree;
  }

  // Returns a mirror array of currentValues (index i = value at leaf i).
  private static int[] mirrorValues(int[][] leafRefs) {
    int[] vals = new int[leafRefs.length];
    for (int i = 0; i < leafRefs.length; i++) vals[i] = leafRefs[i][1];
    return vals;
  }

  // Brute-force minimum across mirror array; matches what tree.top()[1] should be.
  private static int trueMin(int[] mirror) {
    int m = Integer.MAX_VALUE;
    for (int v : mirror) m = Math.min(m, v);
    return m;
  }

  // ---- basic three-source merge (deterministic) ------------------------------------

  public void testThreeSourceMerge() {
    int[][] sources = {{1, 4, 7}, {2, 5, 8}, {3, 6, 9}};
    int k = sources.length;
    int[] pos = new int[k];

    int[][] leafRefs = new int[k][];
    for (int i = 0; i < k; i++) {
      leafRefs[i] = new int[] {i, sources[i][0]};
      pos[i] = 1;
    }

    LoserTree<int[]> tree = buildTree(leafRefs);

    List<Integer> result = new ArrayList<>();
    while (true) {
      int[] champ = tree.top();
      if (champ[1] == Integer.MAX_VALUE) break;
      result.add(champ[1]);
      int src = champ[0];
      int nextVal = pos[src] < sources[src].length ? sources[src][pos[src]++] : Integer.MAX_VALUE;
      // Mutate the champion element in-place and use the no-arg updateTop.
      champ[1] = nextVal;
      tree.updateTop();
    }

    List<Integer> expected = new ArrayList<>();
    for (int[] src : sources) for (int v : src) expected.add(v);
    Collections.sort(expected);
    assertEquals(expected, result);
  }

  // ---- single leaf (degenerate tree) -----------------------------------------------

  public void testSingleLeaf() {
    LoserTree<Integer> tree = LoserTree.usingComparator(1, Integer::compare);
    tree.add(42);
    assertEquals(Integer.valueOf(42), tree.top());

    tree.updateTop(7);
    assertEquals(Integer.valueOf(7), tree.top());

    tree.updateTop(Integer.MAX_VALUE);
    assertEquals(Integer.valueOf(Integer.MAX_VALUE), tree.top());
  }

  // ---- all-equal values ------------------------------------------------------------

  public void testAllEqualValues() {
    int k = 5;
    int[][] leafRefs = new int[k][];
    for (int i = 0; i < k; i++) leafRefs[i] = new int[] {i, 3};
    LoserTree<int[]> tree = buildTree(leafRefs);
    int[] mirror = mirrorValues(leafRefs);

    // All values equal — champion should be the first leaf (smallest index wins ties).
    assertEquals(trueMin(mirror), tree.top()[1]);

    // Replace champion with a larger value; new champion should still be some leaf with value 3.
    int[] champ = tree.top();
    champ[1] = 99;
    mirror[champ[0]] = 99;
    tree.updateTop();
    assertEquals(trueMin(mirror), tree.top()[1]);
  }

  // ---- sources that are immediately exhausted --------------------------------------

  public void testAllExhaustedSources() {
    int k = 4;
    int[][] leafRefs = new int[k][];
    for (int i = 0; i < k; i++) leafRefs[i] = new int[] {i, Integer.MAX_VALUE};
    LoserTree<int[]> tree = buildTree(leafRefs);
    // Every leaf is a sentinel — top should be MAX_VALUE.
    assertEquals(Integer.MAX_VALUE, tree.top()[1]);
  }

  public void testSomeExhaustedSources() {
    // Leaves: 5, MAX, 3, MAX
    int[][] leafRefs = {
      new int[] {0, 5},
      new int[] {1, Integer.MAX_VALUE},
      new int[] {2, 3},
      new int[] {3, Integer.MAX_VALUE}
    };
    LoserTree<int[]> tree = buildTree(leafRefs);
    assertEquals(3, tree.top()[1]); // leaf 2 (value 3) is champion

    // Advance leaf 2 to exhausted
    int[] champ = tree.top();
    champ[1] = Integer.MAX_VALUE;
    tree.updateTop();
    assertEquals(5, tree.top()[1]); // leaf 0 (value 5) is now champion

    // Advance leaf 0 to exhausted
    champ = tree.top();
    champ[1] = Integer.MAX_VALUE;
    tree.updateTop();
    assertEquals(Integer.MAX_VALUE, tree.top()[1]); // all exhausted
  }

  // ---- updateTop(newTop) variant (replaces champion element) -----------------------

  public void testUpdateTopReplacement() {
    int[][] leafRefs = {new int[] {0, 10}, new int[] {1, 20}, new int[] {2, 30}};
    LoserTree<int[]> tree = buildTree(leafRefs);
    assertEquals(10, tree.top()[1]);

    // Replace champion with a new object carrying value 15 (still loses to 20 and 30).
    tree.updateTop(new int[] {0, 15});
    assertEquals(15, tree.top()[1]);

    // Replace with 25 (loses to 30, beats... nothing below, but 20 should now be champion).
    tree.updateTop(new int[] {0, 25});
    assertEquals(20, tree.top()[1]);
  }

  // ---- clear and rebuild -----------------------------------------------------------

  public void testClearAndRebuild() {
    int[][] leafRefs = {new int[] {0, 5}, new int[] {1, 2}, new int[] {2, 8}};
    LoserTree<int[]> tree = buildTree(leafRefs);
    assertEquals(2, tree.top()[1]);

    tree.clear();
    assertEquals(0, tree.size());
    assertNull(tree.top());

    // Re-add with different values
    int[][] newRefs = {new int[] {0, 9}, new int[] {1, 1}, new int[] {2, 4}};
    for (int[] leaf : newRefs) tree.add(leaf);
    assertEquals(1, tree.top()[1]);
  }

  // ---- reuse across rounds of differing arity via reset(int) -----------------------

  public void testResetVaryingLeafCount() {
    // A single instance sized to a fixed capacity, reused for rounds with fewer active leaves.
    // This mirrors the per-term postings merge, where the number of subs varies per term.
    int capacity = 8;
    LoserTree<int[]> tree =
        LoserTree.usingComparator(capacity, (a, b) -> Integer.compare(a[1], b[1]));

    for (int activeLeaves : new int[] {3, 1, 8, 0, 5, 2}) {
      tree.reset(activeLeaves);
      assertEquals(0, tree.size());

      if (activeLeaves == 0) {
        // No leaves contested: nothing is built and there is no champion.
        assertNull(tree.top());
        continue;
      }

      int[] expected = new int[activeLeaves];
      for (int i = 0; i < activeLeaves; i++) {
        int val = random().nextInt(1000);
        tree.add(new int[] {i, val});
        expected[i] = val;
      }
      Arrays.sort(expected);
      assertEquals(expected[0], tree.top()[1]);
    }
  }

  // ---- randomized k-way merge against sorted reference (main oracle test) ----------

  public void testRandomMerge() {
    int iterations = atLeast(200);
    for (int iter = 0; iter < iterations; iter++) {
      int k = TestUtil.nextInt(random(), 1, 20);
      int valRange = TestUtil.nextInt(random(), 1, 100);

      int[][] sources = new int[k][];
      List<Integer> allValues = new ArrayList<>();
      for (int i = 0; i < k; i++) {
        int len = TestUtil.nextInt(random(), 0, 30);
        sources[i] = new int[len];
        for (int j = 0; j < len; j++) sources[i][j] = random().nextInt(valRange);
        Arrays.sort(sources[i]);
        for (int v : sources[i]) allValues.add(v);
      }
      Collections.sort(allValues);

      int[] pos = new int[k];
      int[][] leafRefs = new int[k][];
      for (int i = 0; i < k; i++) {
        int initVal = pos[i] < sources[i].length ? sources[i][pos[i]++] : Integer.MAX_VALUE;
        leafRefs[i] = new int[] {i, initVal};
      }

      LoserTree<int[]> tree = buildTree(leafRefs);
      // Mirror tracks current leaf values for invariant checking.
      int[] mirror = mirrorValues(leafRefs);

      List<Integer> result = new ArrayList<>();
      while (true) {
        int[] champ = tree.top();
        assertNotNull("top() should not be null once built", champ);
        assertEquals(
            "top()[1] must equal the true min of all leaves",
            trueMin(mirror),
            champ[1]);
        if (champ[1] == Integer.MAX_VALUE) break;
        result.add(champ[1]);
        int src = champ[0];
        int nextVal =
            pos[src] < sources[src].length ? sources[src][pos[src]++] : Integer.MAX_VALUE;
        // Use the in-place mutation + no-arg updateTop path.
        int champIdx = src; // champ[0] == src
        champ[1] = nextVal;
        mirror[champIdx] = nextVal;
        tree.updateTop();
      }

      assertEquals("merged result must match globally sorted values", allValues, result);
    }
  }

  // ---- invariant check across random update sequence --------------------------------

  public void testInvariantAfterUpdates() {
    int k = TestUtil.nextInt(random(), 2, 16);
    int[] currentVals = new int[k];
    int[][] leafRefs = new int[k][];
    for (int i = 0; i < k; i++) {
      currentVals[i] = random().nextInt(50);
      leafRefs[i] = new int[] {i, currentVals[i]};
    }
    LoserTree<int[]> tree = buildTree(leafRefs);

    int steps = atLeast(500);
    for (int s = 0; s < steps; s++) {
      assertEquals(trueMin(currentVals), tree.top()[1]);

      int[] champ = tree.top();
      int src = champ[0];
      int newVal = random().nextInt(100);
      currentVals[src] = newVal;

      if (random().nextBoolean()) {
        // In-place mutation path
        champ[1] = newVal;
        tree.updateTop();
      } else {
        // Replacement path
        int[] newLeaf = new int[] {src, newVal};
        tree.updateTop(newLeaf);
        leafRefs[src] = newLeaf; // keep leafRefs consistent (used for src lookup)
      }
    }
  }

  // ---- power-of-2 and non-power-of-2 leaf counts ------------------------------------

  public void testVariousLeafCounts() {
    for (int k : new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 15, 16, 17}) {
      int[][] leafRefs = new int[k][];
      int[] expected = new int[k];
      for (int i = 0; i < k; i++) {
        int val = random().nextInt(1000);
        leafRefs[i] = new int[] {i, val};
        expected[i] = val;
      }
      LoserTree<int[]> tree = buildTree(leafRefs);
      Arrays.sort(expected);
      // Champion should be the global minimum.
      assertEquals(expected[0], tree.top()[1]);
    }
  }
}
