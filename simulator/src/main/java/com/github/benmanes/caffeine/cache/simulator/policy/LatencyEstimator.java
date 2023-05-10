package com.github.benmanes.caffeine.cache.simulator.policy;

/**
 *
 * @param <KeyType> The type of keys for which the estimation occurs
 */
public interface LatencyEstimator<KeyType> {
    /**
     * @param key The item's key
     * @param value The value to record in the estimator for future estimations
     * */

    void record(KeyType key, double value, double recordTime);

    default void addValueToRecord(KeyType key, double value, double recordTime) {}

    default void recordHit(double value) {}

    /**
     *
     * @param key A valid key that was recorded before.
     * @return An estimated value for the given key.
     */
    double getLatencyEstimation(KeyType key);

    default double getLatencyEstimation(KeyType key, double time) { return getLatencyEstimation(key); }

    default double getDelta(KeyType key) { return getLatencyEstimation(key) - getCacheHitEstimation(); }

    default double getCacheHitEstimation() { return 1; }
}
