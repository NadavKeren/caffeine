package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.rankmap;

import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.PipelinePolicy;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

/***
 * Values a request by its item's average cost per request within a burst.
 * <p>
 * Bursts are followed with the same virtual fetch windows as {@code MovingAverageBurstEstimator}:
 * up to {@code number-of-partitions} overlapping windows of one miss penalty each, a new one opening
 * only a partition length after the newest. The first arrival in a window costs the whole penalty
 * and each later one only what is left of the fetch. Each window also counts its requests, which
 * turns its total into the average cost of one request in that burst.
 * <p>
 * Every request is credited with the item's current average, so N requests in a row earn about N
 * times the cost of a request in a burst - what holding the item through them saves. Unlike summing
 * the windows, a request that falls into several overlapping windows is not paid for once per window.
 * The credit goes to the ranks the item holds at that request, wherever the burst started.
 * <p>
 * An item is tracked only while it is on some board, and forgotten through {@link #reset} once it
 * has left all of them.
 */
public final class ItemBurstCostEstimator {
    final private BenefitEstimatorType type;
    final private int numOfPartitions;
    final private double smoothing;
    final private Long2ObjectMap<Entry> entries;

    public ItemBurstCostEstimator(Config config, BenefitEstimatorType type, int expectedKeys) {
        Assert.assertCondition(type == BenefitEstimatorType.ITEM_CUMULATIVE || type == BenefitEstimatorType.ITEM_EWMA,
                               () -> "Not a per-item benefit estimator: " + type);

        var settings = new PipelinePolicy.PipelineSettings(config);

        this.type = type;
        this.numOfPartitions = settings.numOfPartitions();
        this.smoothing = settings.ageSmoothFactor();
        this.entries = new Long2ObjectOpenHashMap<>(expectedKeys * 2);
    }

    /*** Records the arrival and returns what it is credited with: the item's average cost per burst request. */
    public double onArrival(AccessEvent event) {
        final double latency = event.missPenalty();

        if (latency <= 0) {
            return 0d;
        }

        Entry entry = entries.get(event.key());
        if (entry == null) {
            entry = new Entry(numOfPartitions);
            entries.put(event.key(), entry);
        }

        entry.recordArrival(event.getRequestTime(), latency);

        return type == BenefitEstimatorType.ITEM_CUMULATIVE ? entry.cumulativeAverage() : entry.smoothedAverage();
    }

    /*** Forgets an item that has left every board. */
    public void reset(long key) {
        entries.remove(key);
    }

    public int size() {
        return entries.size();
    }

    private final class Entry {
        final private double[] start;
        final private double[] latency;
        final private double[] benefit;
        final private int[] requests;
        private int size = 0;

        private double closedBenefit = 0d;
        private long closedRequests = 0;
        private double smoothed = Double.NaN;

        Entry(int numOfPartitions) {
            this.start = new double[numOfPartitions];
            this.latency = new double[numOfPartitions];
            this.benefit = new double[numOfPartitions];
            this.requests = new int[numOfPartitions];
        }

        void recordArrival(double timestamp, double missPenalty) {
            // Windows are opened in time order, so the expired ones are always a prefix.
            int expired = 0;
            while (expired < size && timestamp - start[expired] > latency[expired]) {
                close(expired);
                ++expired;
            }
            shiftOut(expired);

            for (int idx = 0; idx < size; ++idx) {
                benefit[idx] += latency[idx] - (timestamp - start[idx]);
                ++requests[idx];
            }

            final double partitionLength = Math.max(1d, missPenalty / start.length);
            final boolean withinLastPartition = size > 0 && (timestamp - start[size - 1]) <= partitionLength;

            if (!withinLastPartition && size < start.length) {
                start[size] = timestamp;
                latency[size] = missPenalty;
                benefit[size] = missPenalty;
                requests[size] = 1;
                ++size;
            }
        }

        private void close(int idx) {
            closedBenefit += benefit[idx];
            closedRequests += requests[idx];

            final double average = benefit[idx] / requests[idx];
            smoothed = Double.isNaN(smoothed) ? average : (1 - smoothing) * smoothed + smoothing * average;
        }

        /*** Everything the item paid since it entered the boards, per request, the open windows included. */
        double cumulativeAverage() {
            double totalBenefit = closedBenefit;
            long totalRequests = closedRequests;

            for (int idx = 0; idx < size; ++idx) {
                totalBenefit += benefit[idx];
                totalRequests += requests[idx];
            }

            return totalBenefit / totalRequests;
        }

        /*** The moving average over closed windows; until one has closed, the mean of the open ones. */
        double smoothedAverage() {
            if (!Double.isNaN(smoothed)) {
                return smoothed;
            }

            double sum = 0d;
            for (int idx = 0; idx < size; ++idx) {
                sum += benefit[idx] / requests[idx];
            }

            return sum / size;
        }

        private void shiftOut(int count) {
            if (count == 0) {
                return;
            }

            for (int idx = 0; idx < size - count; ++idx) {
                start[idx] = start[idx + count];
                latency[idx] = latency[idx + count];
                benefit[idx] = benefit[idx + count];
                requests[idx] = requests[idx + count];
            }

            size -= count;
        }
    }
}
