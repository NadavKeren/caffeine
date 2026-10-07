package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.rankmap;

import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.LatencyEstimator;
import com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.MovingAverageBurstEstimator;
import com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.PipelinePolicy;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

import java.util.function.LongConsumer;

/***
 * The burstiness board: {@code LbuBlock} / {@code BurstBlock}'s discipline at the full cache
 * capacity, with the min-heap replaced by an order-statistic tree.
 * <p>
 * The board owns its burst estimator rather than sharing the pipeline's. The pipeline's estimator
 * only retains keys that are resident, while a board needs an estimate for every one of its
 * capacity-many members.
 * <p>
 * The tree is keyed on the burst value normalized out of the aging decay. Aging multiplies every
 * entry by the same factor, so in log space it is a constant shift; storing
 * {@code ln(value) - version * ln(1 - ageSmoothing)} makes a stored score independent of the version
 * it was taken at. That is what lets an aging tick leave the tree untouched instead of rebuilding
 * it, and it avoids the underflow that scaling the values directly would hit after a few hundred
 * thousand ticks.
 */
public final class RankedLbuBlock implements RankedBlock {
    final private int capacity;
    final private int agingWindowSize;
    final private double logDecayPerVersion;

    final private LatencyEstimator estimator;
    final private OrderStatisticTree<ScoredKey> order = new OrderStatisticTree<>(ScoredKey.ORDER);
    final private Long2DoubleMap scores;

    private LongConsumer departures = key -> {};
    private int version = 0;
    private int opsSinceAging = 0;

    public RankedLbuBlock(int capacity, Config config) {
        var settings = new PipelinePolicy.PipelineSettings(config);

        this.capacity = capacity;
        this.agingWindowSize = settings.agingWindowSize();
        this.logDecayPerVersion = Math.log(1 - settings.ageSmoothFactor());
        this.estimator = new MovingAverageBurstEstimator(settings.ageSmoothFactor(),
                                                         settings.numOfPartitions(),
                                                         capacity);

        this.scores = new Long2DoubleOpenHashMap(capacity * 2);
        this.scores.defaultReturnValue(Double.NaN);
    }

    @Override
    public String type() {
        return "LBU";
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

        if (contains(key)) {
            estimator.addValueToRecord(key, 0, event.getRequestTime());
            order.remove(new ScoredKey(scores.get(key), key));
            store(key);
        } else if (capacity > 0) {
            estimator.record(key, event.missPenalty(), event.getRequestTime());
            admit(key);
        } else {
            departures.accept(key);
        }

        ageIfNeeded();

        Assert.assertCondition(order.size() == scores.size(), "LBU board: order and score map diverged");
        Assert.assertCondition(order.size() <= capacity, "LBU board: capacity overflow");
    }

    private void admit(long key) {
        if (order.size() < capacity) {
            store(key);
            return;
        }

        ScoredKey victim = order.min();

        // BurstBlock admits on a rounded comparison of the raw estimates; keep that, so a difference
        // below half a unit is a tie there and a tie here.
        int comparison = (int) Math.round(estimator.getLatencyEstimation(key)
                                          - estimator.getLatencyEstimation(victim.key()));
        if (comparison <= 0) {
            estimator.remove(key);
            departures.accept(key);
            return;
        }

        order.remove(victim);
        scores.remove(victim.key());
        estimator.remove(victim.key());
        departures.accept(victim.key());
        store(key);
    }

    private void store(long key) {
        final double score = normalizedScore(key);
        scores.put(key, score);
        order.add(new ScoredKey(score, key));
    }

    /*** The burst value in log space with the aging decay divided out, so it does not go stale. */
    private double normalizedScore(long key) {
        return Math.log(estimator.getLatencyEstimation(key)) - (version * logDecayPerVersion);
    }

    private void ageIfNeeded() {
        ++opsSinceAging;

        if (opsSinceAging >= agingWindowSize) {
            estimator.ageAll();
            ++version;
            opsSinceAging = 0;
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
