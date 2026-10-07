package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.rankmap;

import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;

import javax.annotation.Nullable;
import java.util.Arrays;

/***
 * Every candidate allocation, arranged as a tree over the allocation simplex.
 * <p>
 * A node at level {@code i} fixes the quanta of stages {@code 0..i-1}; its children are the possible
 * quanta for stage {@code i}. The last stage's quota is whatever is left, so the nodes at that level
 * are the leaves, one per candidate allocation.
 * <p>
 * The shape is fixed for the whole run. Only the reach depths change, and they are refreshed once
 * per decision interval by {@link ReachDepthSolver}.
 * <p>
 * Two properties make scoring every candidate on every request affordable:
 * <ul>
 *   <li>A node's depths are non-decreasing in the quanta given to its stage, so the children whose
 *       stage would hold the requested object form a suffix. That suffix is credited with one
 *       addition into a difference array over the children - the whole subtree at once, whatever the
 *       downstream assignment is - and only the children before it need to be walked.</li>
 *   <li>The depths stay frozen for the interval, so the per-request weights can simply be summed and
 *       folded into the moving average once at the end. That makes the batched average exact rather
 *       than merely cheap.</li>
 * </ul>
 */
public final class AllocationTree {
    final private int stageCount;
    final private int totalQuanta;
    final private Node root;
    final private int[][] candidates;
    final private double[] score;
    /*** Every folded credit, undecayed: what each candidate would have gained over the whole run. */
    final private double[] lifetime;

    public AllocationTree(int stageCount, int totalQuanta) {
        Assert.assertCondition(stageCount >= 2, () -> "A pipeline needs at least two stages, got " + stageCount);

        this.stageCount = stageCount;
        this.totalQuanta = totalQuanta;

        final var builder = new Builder();
        this.root = builder.build(0, totalQuanta, new int[stageCount]);
        this.candidates = builder.candidates.toArray(new int[0][]);
        this.score = new double[candidates.length];
        this.lifetime = new double[candidates.length];
    }

    public int candidateCount() {
        return candidates.length;
    }

    public int stageCount() {
        return stageCount;
    }

    public Node root() {
        return root;
    }

    public int[] quotaOf(int candidate) {
        return candidates[candidate];
    }

    public double score(int candidate) {
        return score[candidate];
    }

    public double lifetime(int candidate) {
        return lifetime[candidate];
    }

    /*** The candidate whose projected score is the highest. */
    public int best() {
        int best = 0;
        for (int idx = 1; idx < score.length; ++idx) {
            if (score[idx] > score[best]) {
                best = idx;
            }
        }

        return best;
    }

    /*** Walks the tree down to the leaf holding this exact allocation. */
    public int indexOf(int[] quota) {
        Node node = root;

        for (int stage = 0; stage < stageCount - 1; ++stage) {
            Assert.assertCondition(quota[stage] <= node.remaining,
                                   () -> "Infeasible allocation: " + Arrays.toString(quota));
            node = node.children[quota[stage]];
        }

        Assert.assertCondition(quota[stageCount - 1] == node.remaining,
                               () -> "Allocation does not sum to the total quanta: " + Arrays.toString(quota));

        return node.leafIndex;
    }

    /***
     * Credits the weight to every candidate that would have held the object with these ranks.
     * Walks only the nodes where every stage so far failed to hold it.
     */
    public void credit(int[] ranks, double weight) {
        credit(root, ranks, weight);
    }

    private void credit(Node node, int[] ranks, double weight) {
        final int rank = ranks[node.stage];

        if (node.leaf) {
            if (rank <= node.depth[0]) {
                node.leafAcc += weight;
            }
            return;
        }

        final int firstHolding = firstHolding(node.depth, rank);

        if (firstHolding <= node.remaining) {
            node.suffixAcc[firstHolding] += weight;
        }

        final int misses = Math.min(firstHolding, node.remaining + 1);
        for (int quanta = 0; quanta < misses; ++quanta) {
            credit(node.children[quanta], ranks, weight);
        }
    }

    /***
     * The smallest number of quanta that reaches the given rank, or one past the maximum when no
     * feasible quota does. Binary search over the depths, which are non-decreasing.
     */
    private static int firstHolding(int[] depth, int rank) {
        int low = 0;
        int high = depth.length; // depth.length == remaining + 1

        while (low < high) {
            int mid = (low + high) >>> 1;
            if (depth[mid] >= rank) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }

        return low;
    }

    /***
     * Folds the interval's accumulated weights into the projected scores and clears them, by pushing
     * each node's subtree credit down to its leaves.
     */
    public void foldInterval(double intervalDecay) {
        pushDown(root, 0d, intervalDecay);
    }

    private void pushDown(Node node, double inherited, double intervalDecay) {
        if (node.leaf) {
            final double gained = inherited + node.leafAcc;
            node.leafAcc = 0d;
            score[node.leafIndex] = intervalDecay * score[node.leafIndex] + gained;
            lifetime[node.leafIndex] += gained;
            return;
        }

        double running = inherited;
        for (int quanta = 0; quanta <= node.remaining; ++quanta) {
            running += node.suffixAcc[quanta];
            node.suffixAcc[quanta] = 0d;
            pushDown(node.children[quanta], running, intervalDecay);
        }
    }

    /*** Discards everything the current interval accumulated without folding it into the scores. */
    public void discardInterval() {
        discard(root);
    }

    private void discard(Node node) {
        if (node.leaf) {
            node.leafAcc = 0d;
            return;
        }

        for (int quanta = 0; quanta <= node.remaining; ++quanta) {
            node.suffixAcc[quanta] = 0d;
            discard(node.children[quanta]);
        }
    }

    public static final class Node {
        final int stage;
        final int remaining;
        final boolean leaf;
        /***
         * {@code depth[q]} is the board position stage {@code stage} reaches when given q quanta.
         * A leaf's quota is forced, so it keeps only that one depth, at index 0. That matters: sized
         * per quota the leaves would cost on the order of {@code quanta^3 / 6} integers, which is
         * hundreds of megabytes once the quantum gets down to a handful of items.
         */
        final int[] depth;
        @Nullable final Node[] children;
        @Nullable final double[] suffixAcc;
        final int leafIndex;

        double leafAcc;

        Node(int stage, int remaining, boolean leaf, int leafIndex, @Nullable Node[] children) {
            this.stage = stage;
            this.remaining = remaining;
            this.leaf = leaf;
            this.leafIndex = leafIndex;
            this.children = children;
            this.depth = new int[leaf ? 1 : remaining + 1];
            this.suffixAcc = leaf ? null : new double[remaining + 1];
        }

        public int stage() {
            return stage;
        }

        public int remaining() {
            return remaining;
        }

        public boolean isLeaf() {
            return leaf;
        }

        public int[] depths() {
            return depth;
        }

        /*** The depth for a leaf's forced quota. */
        public int forcedDepth() {
            return depth[0];
        }

        public Node child(int quanta) {
            return children[quanta];
        }
    }

    private final class Builder {
        final java.util.List<int[]> candidates = new java.util.ArrayList<>();

        Node build(int stage, int remaining, int[] path) {
            if (stage == stageCount - 1) {
                path[stage] = remaining;
                candidates.add(Arrays.copyOf(path, stageCount));

                return new Node(stage, remaining, true, candidates.size() - 1, null);
            }

            var children = new Node[remaining + 1];
            for (int quanta = 0; quanta <= remaining; ++quanta) {
                path[stage] = quanta;
                children[quanta] = build(stage + 1, remaining - quanta, path);
            }

            return new Node(stage, remaining, false, -1, children);
        }
    }

    public int totalQuanta() {
        return totalQuanta;
    }
}
