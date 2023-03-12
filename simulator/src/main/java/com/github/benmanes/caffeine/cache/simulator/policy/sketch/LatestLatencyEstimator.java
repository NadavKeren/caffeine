package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import com.github.benmanes.caffeine.cache.simulator.policy.LatencyEstimator;

import java.util.HashMap;
import java.util.Map;

public class LatestLatencyEstimator<KeyType> implements LatencyEstimator<KeyType> {
    private final static int INITIAL_SIZE = 1000000;
    private final static float LOAD_FACTOR = 2.0f;
    private Map<KeyType, Double> storedValues;

    public LatestLatencyEstimator() {
        this.storedValues = new HashMap<>(INITIAL_SIZE, LOAD_FACTOR);
    }

    @Override
    public void record(KeyType key, double value, double recordTime) {
        storedValues.put(key, value);
    }

    @Override
    public double getLatencyEstimation(KeyType key) {
        Double estimate = storedValues.get(key);
        return estimate != null ? estimate : getCacheHitEstimation();
    }
}
