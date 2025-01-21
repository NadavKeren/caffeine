package com.github.benmanes.caffeine.cache.simulator;

import com.github.benmanes.caffeine.cache.simulator.policy.LatencyEstimator;

public class UneditableLatencyEstimatorProxy implements LatencyEstimator {
    final private LatencyEstimator estimator;

    public UneditableLatencyEstimatorProxy(LatencyEstimator origin) {
        estimator = origin;
    }

    @Override
    public void record(long key, double value, double recordTime) { }

    @Override
    public void addValueToRecord(long key, double value, double recordTime) { }

    @Override
    public void recordHit(double value) { }

    @Override
    public double getLatencyEstimation(long key) {
        return estimator.getLatencyEstimation(key);
    }

    @Override
    public double getLatencyEstimation(long key, double time) {
        return estimator.getLatencyEstimation(key, time);
    }

    @Override
    public double getDelta(long key) {
        return estimator.getDelta(key);
    }

    @Override
    public double getCacheHitEstimation() {
        return estimator.getCacheHitEstimation();
    }

    @Override
    public int size() {
        return estimator.size();
    }
}
