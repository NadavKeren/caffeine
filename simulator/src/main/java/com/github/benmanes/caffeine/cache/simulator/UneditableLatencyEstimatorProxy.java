package com.github.benmanes.caffeine.cache.simulator;

import com.github.benmanes.caffeine.cache.simulator.policy.LatencyEstimator;

public class UneditableLatencyEstimatorProxy<K> implements LatencyEstimator<K> {
    final private LatencyEstimator<K> estimator;

    public UneditableLatencyEstimatorProxy(LatencyEstimator<K> origin) {
        estimator = origin;
    }

    @Override
    public void record(K key, double value, double recordTime) { }

    @Override
    public void addValueToRecord(K key, double value, double recordTime) { }

    @Override
    public void recordHit(double value) { }

    @Override
    public double getLatencyEstimation(K key) {
        return estimator.getLatencyEstimation(key);
    }

    @Override
    public double getLatencyEstimation(K key, double time) {
        return estimator.getLatencyEstimation(key, time);
    }

    @Override
    public double getDelta(K key) {
        return estimator.getDelta(key);
    }

    @Override
    public double getCacheHitEstimation() {
        return estimator.getCacheHitEstimation();
    }
}
