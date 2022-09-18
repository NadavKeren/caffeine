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

    void record(KeyType key, double value);

    default void addValueToRecord(KeyType key, double value) {}

    /**
     *
     * @param key A valid key that was recorded before.
     * @return An estimated value for the given key.
     */
    double getLatencyEstimation(KeyType key);

    double getDelta(KeyType key);

    double getCacheHitEstimation();
}
