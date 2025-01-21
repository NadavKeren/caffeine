package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import com.github.benmanes.caffeine.cache.simulator.policy.LatencyEstimator;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

public class NaiveBurstLatencyEstimator implements LatencyEstimator {
    final private static int INITIAL_SIZE = 1000000;
    final private static float LOAD_FACTOR = 0.875f;
    final private Long2DoubleOpenHashMap storedValues;

    public NaiveBurstLatencyEstimator() {
        this.storedValues = new Long2DoubleOpenHashMap(INITIAL_SIZE, LOAD_FACTOR);
    }

    @Override
    public void record(long key, double value, double recordTime) {
        storedValues.put(key, 0d);
    }

    @Override
    public void addValueToRecord(long key, double value, double recordTime) {
        if (!storedValues.containsKey(key)) {
            throw new IllegalArgumentException(String.format("Key %s was not present during update attempt", key));
        }

        double currentEstimate = storedValues.get(key);

        double newEstimate = currentEstimate + value;
        storedValues.put(key, newEstimate);
    }

    @Override
    public double getLatencyEstimation(long key) {
        return storedValues.containsKey(key) ? storedValues.get(key) : getCacheHitEstimation();
    }
}
