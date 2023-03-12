package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import com.github.benmanes.caffeine.cache.simulator.policy.LatencyEstimator;

import java.util.HashMap;
import java.util.Map;

public class NaiveBurstLatencyEstimator<KeyType> implements LatencyEstimator<KeyType> {
    private final static int INITIAL_SIZE = 1000000;
    private final static float LOAD_FACTOR = 2.0f;
    private Map<KeyType, Double> storedValues;

    public NaiveBurstLatencyEstimator() {
        this.storedValues = new HashMap<>(INITIAL_SIZE, LOAD_FACTOR);
    }

    @Override
    public void record(KeyType key, double value, double recordTime) {
        storedValues.put(key, 0d);
    }

    @Override
    public void addValueToRecord(KeyType key, double value, double recordTime) {
        Double currentEstimate = storedValues.get(key);
        if (currentEstimate == null) {
            throw new IllegalArgumentException(String.format("Key %s was not present during update attempt", key));
        }

        double newEstimate = currentEstimate + value;
        storedValues.put(key, newEstimate);
    }

    @Override
    public double getLatencyEstimation(KeyType key) {
        Double estimate = storedValues.get(key);
        return estimate != null ? estimate : getCacheHitEstimation();
    }
}
