package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import com.github.benmanes.caffeine.cache.simulator.policy.LatencyEstimator;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

public class LatestLatencyEstimator implements LatencyEstimator {
    private final static int INITIAL_SIZE = 1000000;
    private final static float LOAD_FACTOR = 0.875f;
    final private Long2DoubleOpenHashMap storedValues;

    private double hitPenalty = 0;

    public LatestLatencyEstimator() {
        this.storedValues = new Long2DoubleOpenHashMap(INITIAL_SIZE, LOAD_FACTOR);
    }

    public LatestLatencyEstimator(LatestLatencyEstimator origin) {
        this.hitPenalty = origin.hitPenalty;
        this.storedValues = new Long2DoubleOpenHashMap(origin.storedValues);
    }

    @Override
    public LatencyEstimator createDeepCopy() {
        return new LatestLatencyEstimator(this);
    }

    @Override
    public void record(long key, double value, double recordTime) {
        storedValues.put(key, value);
    }

    @Override
    public void recordHit(double value) {
        this.hitPenalty = value;
    }

    @Override
    public double getCacheHitEstimation() {
        return this.hitPenalty;
    }

    @Override
    public double getLatencyEstimation(long key) {
        return storedValues.containsKey(key) ? storedValues.get(key) : getCacheHitEstimation();
    }

    @Override
    public void remove(long key) {
        this.storedValues.remove(key);
    }

    @Override
    public int size() {
        return this.storedValues.size();
    }
}
