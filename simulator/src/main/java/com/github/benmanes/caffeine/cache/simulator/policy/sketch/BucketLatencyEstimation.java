package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import com.github.benmanes.caffeine.cache.simulator.policy.LatencyEstimator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BucketLatencyEstimation<KeyType> implements LatencyEstimator<KeyType> {
    final private static int INITIAL_SIZE = 1000000;
    final private static float LOAD_FACTOR = 2.0f;
    final private static double MIN_RECORDABLE_VALUE = 5;

    List<Bucket<KeyType>> buckets;
    Map<KeyType, Long> occurrenceCounts;

    public BucketLatencyEstimation(int numOfBuckets, double epsilon) {
        if (numOfBuckets < 2) {
            throw new IllegalArgumentException(String.format("The number of buckets should be above 2, got: %d", numOfBuckets));
        }

        if (epsilon <= 1) {
            throw new IllegalArgumentException(String.format("The epsilon should be greater than 1, got: %f.2", epsilon));
        }

        buckets = new ArrayList<>(numOfBuckets);
        createBuckets(numOfBuckets, epsilon);
        occurrenceCounts = new HashMap<>(INITIAL_SIZE, LOAD_FACTOR);
    }

    @Override
    public void record(KeyType key, double value, double recordTime) {
        updateCount(key);
        for (var bucket : buckets) { // avoiding using java.lang.Math.log for finding the bucket
            if (bucket.isInRange(value)) {
                bucket.increment(key);
            }
        }
    }

    private void updateCount(KeyType key) {
        Long count = occurrenceCounts.get(key);
        count = count != null ? count + 1 : 1L;
        occurrenceCounts.put(key, count);
    }

    @Override
    public double getLatencyEstimation(KeyType key) {
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
            buckets.set(i, new Bucket<>(minValue, maxValue));
            minValue = maxValue;
            maxValue = minValue * epsilon;
        }

        buckets.set(numOfBuckets - 1, new Bucket<>(minValue, Double.MAX_VALUE));
    }

    private static class Bucket<KeyType> {
        private double minValue;
        private double maxValue;
        Map<KeyType, Long> occurrences;

        public Bucket(double minValue, double maxValue) {
            this.minValue = minValue;
            this.maxValue = maxValue;
            occurrences = new HashMap<>(INITIAL_SIZE, LOAD_FACTOR);
        }

        public boolean isInRange(double value) { return value < maxValue && value >= minValue; }

        public void increment(KeyType key) {
            Long count = occurrences.get(key);
            count = count != null ? count + 1L : 1L;

            occurrences.put(key, count);
        }

        public double getValue(KeyType key) { return occurrences.getOrDefault(key, 0L) * minValue; }
    }
}
