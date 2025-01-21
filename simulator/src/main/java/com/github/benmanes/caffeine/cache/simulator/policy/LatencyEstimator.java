package com.github.benmanes.caffeine.cache.simulator.policy;

public interface LatencyEstimator {
    /**
     * @param key The item's key
     * @param value The value to record in the estimator for future estimations
     * */

    void record(long key, double value, double recordTime);

    default void addValueToRecord(long key, double value, double recordTime) {}

    default void recordHit(double value) {}

    /**
     *
     * @param key A valid key that was recorded before.
     * @return An estimated value for the given key.
     */
    double getLatencyEstimation(long key);

    default double getLatencyEstimation(long key, double time) { return getLatencyEstimation(key); }

    default double getDelta(long key) { return getLatencyEstimation(key) - getCacheHitEstimation(); }

    default double getCacheHitEstimation() { return 1; }

    default void remove(long key) {}

    default int size() { throw new UnsupportedOperationException(); }
}
