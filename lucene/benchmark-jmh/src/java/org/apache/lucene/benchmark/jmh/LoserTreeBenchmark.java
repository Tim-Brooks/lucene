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
package org.apache.lucene.benchmark.jmh;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.LoserTree;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmarks the LoserTree tournament structure in isolation, simulating k-way doc-id merges under
 * two interleaving modes:
 *
 * <ul>
 *   <li><b>uniform</b> — sources interleave perfectly every k steps; every pop triggers the slow
 *       path and comparison outcomes are near-random. This is the workload where branchless replay
 *       should win.
 *   <li><b>skewed</b> — one source dominates; the tournament champion rarely changes. This measures
 *       the degenerate case (in production DocIDMerger, the {@code queueMinDocID} fast-path absorbs
 *       this before it reaches the tournament).
 * </ul>
 *
 * <p>To measure branch-miss reduction, run with {@code -prof perfnorm} and compare the {@code
 * branch-misses} counter between the branchless (current) and a branchy baseline. Also use {@code
 * -XX:+PrintIntrinsics} to confirm that {@code Math.min(long, long)} is intrinsified to a cmov.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xmx1g", "-Xms1g", "-XX:+AlwaysPreTouch"})
public class LoserTreeBenchmark {

  /** Number of k-way sources to merge. */
  @Param({"2", "4", "8", "16", "32"})
  int k;

  /**
   * Interleaving mode: {@code uniform} (max interleaving, worst case for branchy code) or {@code
   * skewed} (one dominant source, best case for branchy code).
   */
  @Param({"uniform", "skewed"})
  String interleaving;

  private static final int DOCS_PER_SOURCE = 10_000;

  /** Pre-generated sorted doc-id arrays per source. */
  private int[][] sourceDocs;

  /** Current position per source; reset before each invocation. */
  private int[] positions;

  /**
   * Pre-allocated boxed Integer objects (one per source). Used as the leaf element so that no
   * allocation occurs in the benchmark loop; the element encodes the source index.
   */
  private Integer[] boxedIndices;

  /** The tournament tree being benchmarked. */
  private LoserTree<Integer> tree;

  @Setup(Level.Trial)
  public void setup() {
    sourceDocs = new int[k][];
    boxedIndices = new Integer[k];
    for (int i = 0; i < k; i++) {
      boxedIndices[i] = i;
    }

    if ("uniform".equals(interleaving)) {
      // Source i gets doc-ids {i, k+i, 2k+i, ...}: perfectly interleaved, maximum slow-path hits.
      for (int i = 0; i < k; i++) {
        sourceDocs[i] = new int[DOCS_PER_SOURCE];
        for (int j = 0; j < DOCS_PER_SOURCE; j++) {
          sourceDocs[i][j] = i + j * k;
        }
      }
    } else {
      // Source 0 has a dense run [0..DOCS_PER_SOURCE); all other sources are immediately exhausted.
      // The tournament champion rarely changes — minimal interleaving.
      sourceDocs[0] = new int[DOCS_PER_SOURCE];
      for (int j = 0; j < DOCS_PER_SOURCE; j++) {
        sourceDocs[0][j] = j;
      }
      for (int i = 1; i < k; i++) {
        sourceDocs[i] = new int[0];
      }
    }
    positions = new int[k];
    tree = LoserTree.create(k);
  }

  /** Reset source positions and rebuild the tree before each invocation. */
  @Setup(Level.Invocation)
  public void resetInvocation() {
    Arrays.fill(positions, 0);
    tree.reset(k);
    for (int i = 0; i < k; i++) {
      int key =
          positions[i] < sourceDocs[i].length ? sourceDocs[i][positions[i]++] : Integer.MAX_VALUE;
      tree.add(key, boxedIndices[i]);
    }
  }

  /**
   * Drains a complete k-way merge through the tournament, returning a checksum of all popped
   * doc-ids to prevent dead-code elimination.
   */
  @Benchmark
  public long drain() {
    long checksum = 0L;
    while (true) {
      int key = tree.topKey();
      if (key == Integer.MAX_VALUE) {
        break;
      }
      checksum += key;
      int src = tree.top(); // Integer auto-unboxed to int
      int nextKey =
          positions[src] < sourceDocs[src].length
              ? sourceDocs[src][positions[src]++]
              : Integer.MAX_VALUE;
      tree.updateTop(nextKey, boxedIndices[src]);
    }
    return checksum;
  }
}
