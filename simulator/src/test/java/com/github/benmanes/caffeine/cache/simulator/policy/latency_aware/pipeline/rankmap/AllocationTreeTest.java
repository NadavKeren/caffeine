package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.rankmap;

import static com.google.common.truth.Truth.assertThat;

import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * The subtree crediting and the shared-work depth solver are both optimizations of loops that are
 * obviously correct when written out per candidate. These tests are those loops.
 */
final class AllocationTreeTest {

    @Test
    void subtreeCreditingMatchesThePerCandidateLoop() {
        var random = new Random(0xC0FFEE);

        for (int stageCount = 2; stageCount <= 4; ++stageCount) {
            for (int totalQuanta : new int[] {0, 1, 5, 12}) {
                var tree = new AllocationTree(stageCount, totalQuanta);
                int capacity = 64;

                int[][] depths = randomizeDepths(tree, random, capacity);
                double[] reference = new double[tree.candidateCount()];

                for (int request = 0; request < 300; ++request) {
                    int[] ranks = new int[stageCount];
                    for (int stage = 0; stage < stageCount; ++stage) {
                        // capacity + 1 is the "not on this board" sentinel, drawn often on purpose.
                        ranks[stage] = 1 + random.nextInt(capacity + 1);
                    }
                    double gain = 1 + random.nextInt(10);

                    tree.credit(ranks, gain);

                    for (int candidate = 0; candidate < tree.candidateCount(); ++candidate) {
                        for (int stage = 0; stage < stageCount; ++stage) {
                            if (ranks[stage] <= depths[candidate][stage]) {
                                reference[candidate] += gain;
                                break;
                            }
                        }
                    }
                }

                tree.foldInterval(0.5);

                for (int candidate = 0; candidate < tree.candidateCount(); ++candidate) {
                    assertThat(tree.score(candidate)).isWithin(1e-9).of(reference[candidate]);
                }
            }
        }
    }

    @Test
    void candidatesAreEveryFeasibleAllocation() {
        var tree = new AllocationTree(3, 4);

        assertThat(tree.candidateCount()).isEqualTo(15); // compositions of 4 into 3 parts

        for (int candidate = 0; candidate < tree.candidateCount(); ++candidate) {
            int[] quota = tree.quotaOf(candidate);
            assertThat(Arrays.stream(quota).sum()).isEqualTo(4);
            assertThat(tree.indexOf(quota)).isEqualTo(candidate);
        }
    }

    @Test
    void solvedDepthsMatchTheReferenceScan() {
        var random = new Random(0xD00D);

        for (int stageCount = 2; stageCount <= 4; ++stageCount) {
            for (int quantumSize : new int[] {1, 2, 8}) {
                int totalQuanta = 6;
                int capacity = totalQuanta * quantumSize;

                var source = new StubRankSource(stageCount, capacity, random);
                var tree = new AllocationTree(stageCount, totalQuanta);
                var solver = new ReachDepthSolver(source, tree, quantumSize);

                solver.refresh();

                int[][] fromTree = solver.treeDepths();
                int[][] naive = solver.naiveDepths();

                for (int candidate = 0; candidate < tree.candidateCount(); ++candidate) {
                    assertThat(fromTree[candidate]).isEqualTo(naive[candidate]);
                }
            }
        }
    }

    @Test
    void aStageWithoutQuantaNeverHoldsAnything() {
        var source = new StubRankSource(2, 8, new Random(7));
        var tree = new AllocationTree(2, 4);
        var solver = new ReachDepthSolver(source, tree, 2);

        solver.refresh();
        int[][] depths = solver.treeDepths();

        for (int candidate = 0; candidate < tree.candidateCount(); ++candidate) {
            int[] quota = tree.quotaOf(candidate);
            for (int stage = 0; stage < 2; ++stage) {
                if (quota[stage] == 0) {
                    assertThat(depths[candidate][stage]).isEqualTo(0);
                }
            }
        }
    }

    /** Randomly fills in the depths a solver would have produced, keeping them monotone per node. */
    private static int[][] randomizeDepths(AllocationTree tree, Random random, int capacity) {
        fill(tree.root(), random, capacity);

        int[][] depths = new int[tree.candidateCount()][tree.stageCount()];
        for (int candidate = 0; candidate < tree.candidateCount(); ++candidate) {
            int[] quota = tree.quotaOf(candidate);
            AllocationTree.Node node = tree.root();

            for (int stage = 0; stage < tree.stageCount() - 1; ++stage) {
                depths[candidate][stage] = node.depths()[quota[stage]];
                node = node.child(quota[stage]);
            }
            depths[candidate][tree.stageCount() - 1] = node.forcedDepth();
        }

        return depths;
    }

    private static void fill(AllocationTree.Node node, Random random, int capacity) {
        int[] depths = node.depths();

        if (node.isLeaf()) {
            // A leaf's quota is forced, so it holds exactly one depth.
            depths[0] = node.remaining() == 0 ? 0 : 1 + random.nextInt(capacity);
            return;
        }

        depths[0] = 0;
        for (int quanta = 1; quanta < depths.length; ++quanta) {
            depths[quanta] = Math.min(capacity, depths[quanta - 1] + random.nextInt(12));
        }

        for (int quanta = 0; quanta <= node.remaining(); ++quanta) {
            fill(node.child(quanta), random, capacity);
        }
    }

    /** Boards with a fixed, arbitrary overlap between the stages. */
    private static final class StubRankSource implements RankSource {
        private final int stageCount;
        private final int capacity;
        private final long[][] boards;

        StubRankSource(int stageCount, int capacity, Random random) {
            this.stageCount = stageCount;
            this.capacity = capacity;
            this.boards = new long[stageCount][];

            for (int stage = 0; stage < stageCount; ++stage) {
                // Draw from a universe twice the capacity so the boards partly overlap.
                List<Long> keys = new ArrayList<>();
                for (long key = 0; key < capacity * 2L; ++key) {
                    keys.add(key);
                }
                Collections.shuffle(keys, random);

                boards[stage] = new long[capacity];
                for (int idx = 0; idx < capacity; ++idx) {
                    boards[stage][idx] = keys.get(idx);
                }
            }
        }

        @Override public int stageCount() { return stageCount; }

        @Override public int capacity() { return capacity; }

        @Override
        public int[] preAccessRanks(long key) {
            int[] ranks = new int[stageCount];
            for (int stage = 0; stage < stageCount; ++stage) {
                ranks[stage] = capacity + 1;
                for (int idx = 0; idx < boards[stage].length; ++idx) {
                    if (boards[stage][idx] == key) {
                        ranks[stage] = idx + 1;
                        break;
                    }
                }
            }

            return ranks;
        }

        @Override
        public long[] snapshotDescending(int stage, int count) {
            return Arrays.copyOf(boards[stage], Math.min(count, boards[stage].length));
        }

        @Override public void update(AccessEvent event) {}

        @Override public boolean isWarm() { return true; }
    }
}
