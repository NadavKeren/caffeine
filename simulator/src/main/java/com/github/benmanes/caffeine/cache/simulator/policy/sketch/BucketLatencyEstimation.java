package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import com.github.benmanes.caffeine.cache.simulator.policy.LatencyEstimator;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

import java.util.ArrayList;
import java.util.List;

public class BucketLatencyEstimation implements LatencyEstimator {
    final private static int INITIAL_SIZE = 1000000;
    final private static float LOAD_FACTOR = 0.875f;
    final private static double MIN_RECORDABLE_VALUE = 5;

    List<Bucket> buckets;
    Long2LongOpenHashMap occurrenceCounts;

    public BucketLatencyEstimation(int numOfBuckets, double epsilon) {
        if (numOfBuckets < 2) {
            throw new IllegalArgumentException(String.format("The number of buckets should be above 2, got: %d", numOfBuckets));
        }

        if (epsilon <= 1) {
            throw new IllegalArgumentException(String.format("The epsilon should be greater than 1, got: %f.2", epsilon));
        }

        buckets = new ArrayList<>(numOfBuckets);
        createBuckets(numOfBuckets, epsilon);
        occurrenceCounts = new Long2LongOpenHashMap(INITIAL_SIZE, LOAD_FACTOR);
        occurrenceCounts.defaultReturnValue(0L);
    }

    @Override
    public void record(long key, double value, double recordTime) {
        updateCount(key);
        for (var bucket : buckets) { // avoiding using java.lang.Math.log for finding the bucket
            if (bucket.isInRange(value)) {
                bucket.increment(key);
            }
        }
    }

    private void updateCount(long key) {
        long count = occurrenceCounts.get(key) + 1;
        occurrenceCounts.put(key, count);
    }

    @Override
    public double getLatencyEstimation(long key) {
        double sum = 0d;

        for (var bucket : buckets) {
            sum += bucket.getValue(key);
        }

        long count = occurrenceCounts.get(key);

        return sum / count;
    }

    private void createBuckets(int numOfBuckets, double epsilon) {
        double minValue = MIN_RECORDABLE_VALUE;
        double maxValue = minValue * 10; // for the first bucket only

        for (int i = 0; i < numOfBuckets - 1; ++i) {
            buckets.set(i, new Bucket(minValue, maxValue));
            minValue = maxValue;
            maxValue = minValue * epsilon;
        }

        buckets.set(numOfBuckets - 1, new Bucket(minValue, Double.MAX_VALUE));
    }

    private static class Bucket {
        final private double minValue;
        final private double maxValue;
        final private Long2LongOpenHashMap occurrences;

        public Bucket(double minValue, double maxValue) {
            this.minValue = minValue;
            this.maxValue = maxValue;
            occurrences = new Long2LongOpenHashMap(INITIAL_SIZE, LOAD_FACTOR);
            occurrences.defaultReturnValue(0L);
        }

        public boolean isInRange(double value) { return value < maxValue && value >= minValue; }

        public void increment(long key) {
            long count = occurrences.get(key) + 1;

            occurrences.put(key, count);
        }

        public double getValue(long key) { return occurrences.getOrDefault(key, 0L) * minValue; }
    }
}
