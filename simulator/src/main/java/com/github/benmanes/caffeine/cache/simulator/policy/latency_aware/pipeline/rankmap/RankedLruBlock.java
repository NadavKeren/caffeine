package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.rankmap;

import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

/***
 * The recency board: {@code LruBlock}'s discipline at the full cache capacity.
 * <p>
 * The block orders by last access - a hit moves the entry to the MRU end, the victim is the LRU end
 * - so the score is simply an access counter, which is already unique per access.
 */
public final class RankedLruBlock implements RankedBlock {
    final private int capacity;
    final private OrderStatisticTree<ScoredKey> order = new OrderStatisticTree<>(ScoredKey.ORDER);
    final private Long2DoubleMap scores;

    private long opCounter = 0;

    public RankedLruBlock(int capacity) {
        this.capacity = capacity;
        this.scores = new Long2DoubleOpenHashMap(capacity * 2);
        this.scores.defaultReturnValue(Double.NaN);
    }

    @Override
    public String type() {
        return "LRU";
    }

    @Override
    public boolean contains(long key) {
        return !Double.isNaN(scores.get(key));
    }

    @Override
    public int rank(long key) {
        double score = scores.get(key);
        if (Double.isNaN(score)) {
            return capacity + 1;
        }

        return order.rankDescending(new ScoredKey(score, key));
    }

    @Override
    public long[] snapshotDescending(int count) {
        final int wanted = Math.min(count, order.size());
        long[] snapshot = new long[wanted];
        int[] filled = {0};
        order.forEachDescending(wanted, entry -> snapshot[filled[0]++] = entry.key());

        return snapshot;
    }

    @Override
    public void update(AccessEvent event) {
        final long key = event.key();
        final double previous = scores.get(key);

        if (!Double.isNaN(previous)) {
            order.remove(new ScoredKey(previous, key));
        } else if (order.size() >= capacity) {
            ScoredKey victim = order.min();
            order.remove(victim);
            scores.remove(victim.key());
        }

        final double score = ++opCounter;
        scores.put(key, score);
        order.add(new ScoredKey(score, key));

        Assert.assertCondition(order.size() == scores.size(), "LRU board: order and score map diverged");
        Assert.assertCondition(order.size() <= capacity, "LRU board: capacity overflow");
    }

    @Override
    public int size() {
        return order.size();
    }

    @Override
    public int capacity() {
        return capacity;
    }
}
