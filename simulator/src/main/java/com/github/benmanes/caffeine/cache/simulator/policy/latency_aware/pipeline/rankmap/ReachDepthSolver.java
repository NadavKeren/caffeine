package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.rankmap;

import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

import java.util.Arrays;

/***
 * Works out how far down its own board each stage has to read before it has collected its quota.
 * <p>
 * Prefixes of different boards overlap, so the stages of an allocation together cover fewer than
 * {@code capacity} distinct objects. The model is that each stage claims the shared objects ahead of
 * everything downstream of it, and a downstream stage keeps reading further down its own board until
 * it has collected its full quota of objects nobody upstream claimed. Membership is then a single
 * integer comparison per stage: the stage holds the object when its rank is within that depth.
 * <p>
 * Done once per decision interval, and frozen for the whole of the next one. Freezing is not just an
 * optimization: it is what makes the batched per-interval average exact.
 * <p>
 * The walk is over the allocation tree, so a board is walked once per node instead of once per
 * candidate. One walk yields the depth for <em>every</em> quota that stage could be given, because
 * the walk passes those stopping points in order.
 */
public final class ReachDepthSolver {
    final private RankSource source;
    final private AllocationTree tree;
    final private int capacity;
    final private int quantumSize;
    final private int stageCount;

    final private Long2IntMap keyToSlot;
    final private int[][] boards;
    /*** One scratch buffer per level; the depth-first walk only ever has one node active per level. */
    final private int[][] collectedPerLevel;
    private boolean[] claimed;
    private int slotCount = 0;

    public ReachDepthSolver(RankSource source, AllocationTree tree, int quantumSize) {
        Assert.assertCondition(quantumSize > 0, () -> "Illegal quantum size: " + quantumSize);

        this.source = source;
        this.tree = tree;
        this.capacity = source.capacity();
        this.quantumSize = quantumSize;
        this.stageCount = source.stageCount();

        this.keyToSlot = new Long2IntOpenHashMap(stageCount * capacity * 2);
        this.keyToSlot.defaultReturnValue(-1);
        this.boards = new int[stageCount][];
        this.collectedPerLevel = new int[stageCount][capacity];
        this.claimed = new boolean[stageCount * capacity];
    }

    public void refresh() {
        snapshotBoards();
        descend(tree.root());
    }

    private void snapshotBoards() {
        keyToSlot.clear();
        slotCount = 0;

        for (int stage = 0; stage < stageCount; ++stage) {
            long[] keys = source.snapshotDescending(stage, capacity);
            int[] slots = new int[keys.length];

            for (int idx = 0; idx < keys.length; ++idx) {
                int slot = keyToSlot.get(keys[idx]);
                if (slot < 0) {
                    slot = slotCount++;
                    keyToSlot.put(keys[idx], slot);
                }
                slots[idx] = slot;
            }

            boards[stage] = slots;
        }

        if (claimed.length < slotCount) {
            claimed = new boolean[slotCount];
        } else {
            Arrays.fill(claimed, 0, slotCount, false);
        }
    }

    private void descend(AllocationTree.Node node) {
        final int[] collected = collectedPerLevel[node.stage()];
        final int got = walkBoard(node, collected);

        if (node.isLeaf()) {
            return;
        }

        int marked = 0;
        for (int quanta = 0; quanta <= node.remaining(); ++quanta) {
            final int target = Math.min(quanta * quantumSize, got);
            while (marked < target) {
                claimed[collected[marked]] = true;
                ++marked;
            }

            descend(node.child(quanta));
        }

        while (marked > 0) {
            --marked;
            claimed[collected[marked]] = false;
        }
    }

    /***
     * Reads down this stage's board once, filling in the depth for every quota it could be given and
     * recording the objects it actually collected - the first unclaimed ones in board order, not its
     * top ones. Returns how many it managed to collect.
     */
    private int walkBoard(AllocationTree.Node node, int[] collected) {
        final int[] depth = node.depths();
        final int[] board = boards[node.stage()];
        final int wanted = node.remaining() * quantumSize;
        // A leaf's quota is forced, so it stores only that depth and needs no stopping points on the
        // way down. Everywhere else the walk records the depth for every quota as it passes it.
        final boolean leaf = node.isLeaf();

        if (!leaf) {
            depth[0] = 0;
        }

        int got = 0;
        int position = 0;
        int quota = 1;

        while (position < board.length && got < wanted) {
            final int slot = board[position];
            ++position;

            if (!claimed[slot]) {
                collected[got] = slot;
                ++got;

                if (!leaf) {
                    while (quota <= node.remaining() && got == quota * quantumSize) {
                        depth[quota] = position;
                        ++quota;
                    }
                }
            }
        }

        if (leaf) {
            // During warm-up a board can be too thin to supply the quota. Fall back to the full
            // capacity, which is the deepest any stage is ever allowed to read.
            depth[0] = got == wanted ? position : capacity;
        } else {
            while (quota <= node.remaining()) {
                depth[quota] = capacity;
                ++quota;
            }
        }

        return got;
    }

    /***
     * The reference implementation: every candidate scanned on its own, with no work shared between
     * candidates. Kept as the oracle - it is the only version that stays obviously correct as the
     * stage count grows.
     */
    public int[][] naiveDepths() {
        snapshotBoards();

        final int[][] depths = new int[tree.candidateCount()][stageCount];
        final var claimedKeys = new it.unimi.dsi.fastutil.ints.IntOpenHashSet();

        for (int candidate = 0; candidate < tree.candidateCount(); ++candidate) {
            final int[] quota = tree.quotaOf(candidate);
            claimedKeys.clear();

            for (int stage = 0; stage < stageCount; ++stage) {
                final int[] board = boards[stage];
                final int wanted = quota[stage] * quantumSize;

                if (wanted == 0) {
                    depths[candidate][stage] = 0;
                    continue;
                }

                int got = 0;
                int position = 0;
                final var collected = new it.unimi.dsi.fastutil.ints.IntArrayList(wanted);

                while (position < board.length && got < wanted) {
                    final int slot = board[position];
                    ++position;

                    if (!claimedKeys.contains(slot)) {
                        collected.add(slot);
                        ++got;
                    }
                }

                depths[candidate][stage] = got == wanted ? position : capacity;
                claimedKeys.addAll(collected);
            }
        }

        return depths;
    }

    /*** The depths the tree currently holds, laid out per candidate for comparison with the oracle. */
    public int[][] treeDepths() {
        final int[][] depths = new int[tree.candidateCount()][stageCount];

        for (int candidate = 0; candidate < tree.candidateCount(); ++candidate) {
            final int[] quota = tree.quotaOf(candidate);
            AllocationTree.Node node = tree.root();

            for (int stage = 0; stage < stageCount - 1; ++stage) {
                depths[candidate][stage] = node.depths()[quota[stage]];
                node = node.child(quota[stage]);
            }

            depths[candidate][stageCount - 1] = node.forcedDepth();
        }

        return depths;
    }
}
