package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.rankmap;

import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.admission.countmin4.PeriodicResetCountMin4;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.function.LongConsumer;

/***
 * The latency-aware frequency board: keys ordered by {@code LATinyLfu}'s score, the sketch frequency
 * times the latest latency delta (miss penalty minus hit penalty), and admitted only when they
 * outscore the lowest member.
 * <p>
 * This is not a re-implementation of {@code LALfuBlock}, whose eviction order inside its segments is
 * CRA's. It ranks by the latency-aware frequency score directly, which is what that block's admission
 * compares, and is a better proxy for it than the latency-blind frequency board.
 * <p>
 * The sketch moves the frequency of keys that were not touched: hash collisions and the periodic
 * halving. A member's score is therefore taken when the member is accessed and kept until its next
 * access, and the whole tree is re-scored whenever the sketch halves, which is when every stored
 * score has gone stale at once. Between halvings the collisions are left as error.
 */
public final class RankedLaLfuBlock implements RankedBlock {
    final private int capacity;
    final private PeriodicResetCountMin4 sketch;
    final private OrderStatisticTree<ScoredKey> order = new OrderStatisticTree<>(ScoredKey.ORDER);
    final private Long2DoubleMap scores;
    final private Long2DoubleMap latencies;

    private LongConsumer departures = key -> {};
    private int resetsSeen = 0;

    public RankedLaLfuBlock(int capacity, Config config) {
        final String sketchType = config.getString("tiny-lfu.sketch");
        final String reset = config.getString("tiny-lfu.count-min-4.reset");
        Assert.assertCondition(sketchType.equalsIgnoreCase("count-min-4") && reset.equalsIgnoreCase("periodic"),
                               () -> String.format("The LA-LFU board needs a count-min-4 sketch with a periodic reset, got %s / %s",
                                                   sketchType, reset));

        this.capacity = capacity;
        this.sketch = new PeriodicResetCountMin4(config);

        this.scores = new Long2DoubleOpenHashMap(capacity * 2);
        this.scores.defaultReturnValue(Double.NaN);
        this.latencies = new Long2DoubleOpenHashMap(capacity * 2);
    }

    @Override
    public String type() {
        return "LA-LFU";
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

    /*** Every request counts towards the frequency, as LALfuBlock's admittor records every request. */
    @Override
    public void bookkeeping(long key) {
        sketch.increment(key);

        if (sketch.resets() != resetsSeen) {
            resetsSeen = sketch.resets();
            rescoreAll();
        }
    }

    @Override
    public void update(AccessEvent event) {
        final long key = event.key();

        if (contains(key)) {
            order.remove(new ScoredKey(scores.get(key), key));
            store(key);
        } else {
            admit(key, event.missPenalty() - event.hitPenalty());
        }

        Assert.assertCondition(order.size() == scores.size(), "LA-LFU board: order and score map diverged");
        Assert.assertCondition(order.size() <= capacity, "LA-LFU board: capacity overflow");
    }

    /***
     * The latency is the delta of the miss that admits the key, its miss penalty over the hit penalty,
     * as LATinyLfu takes it from the pipeline's estimator.
     */
    private void admit(long key, double latency) {
        if (capacity == 0) {
            departures.accept(key);
            return;
        }

        if (order.size() >= capacity) {
            ScoredKey victim = order.min();

            // LATinyLfu compares fresh scores, so the stored one of the victim is not used here.
            if (score(key, latency) <= score(victim.key(), latencies.get(victim.key()))) {
                departures.accept(key);
                return;
            }

            order.remove(victim);
            scores.remove(victim.key());
            latencies.remove(victim.key());
            departures.accept(victim.key());
        }

        latencies.put(key, latency);
        store(key);
    }

    private void store(long key) {
        final double score = score(key, latencies.get(key));
        scores.put(key, score);
        order.add(new ScoredKey(score, key));
    }

    private double score(long key, double latency) {
        return sketch.frequency(key) * latency;
    }

    private void rescoreAll() {
        var members = new LongArrayList(scores.keySet());

        order.clear();
        for (long key : members) {
            scores.remove(key);
            store(key);
        }
    }

    @Override
    public void onDeparture(LongConsumer listener) {
        this.departures = listener;
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
