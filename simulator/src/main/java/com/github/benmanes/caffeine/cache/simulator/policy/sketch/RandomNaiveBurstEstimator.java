package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import static com.google.common.base.Preconditions.checkState;

public class RandomNaiveBurstEstimator<KeyType> extends NaiveBurstLatencyEstimator<KeyType> {
    private double prob;

    public RandomNaiveBurstEstimator(double prob) {
        super();
        this.prob = prob;
        checkState(prob >= 0 && prob <= 1, "Invalid probability");
    }

    @Override
    public double getLatencyEstimation(KeyType key) {
        double randNum = Math.random();
        return randNum < prob ? 0 : super.getLatencyEstimation(key);
    }
}
