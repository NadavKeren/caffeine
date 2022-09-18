package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import com.github.benmanes.caffeine.cache.simulator.policy.LatencyEstimator;

import java.util.HashMap;
import java.util.Map;

import static java.lang.System.Logger;

public class TrueAverageEstimator<KeyType> implements LatencyEstimator<KeyType> {
    private static int INITIAL_SIZE = 1000000;
    private static float LOAD_FACTOR = 2.0f;
    private Map<KeyType, AccumulatedValues> storedValues;

    private static final Logger logger = System.getLogger(LatencyEstimator.class.getName());

    public TrueAverageEstimator() {
        storedValues = new HashMap<>(INITIAL_SIZE, LOAD_FACTOR);
    }

    @Override
    public void record(KeyType key, double value) {
        AccumulatedValues acc = storedValues.get(key);

        logger.log(Logger.Level.DEBUG, String.format("Recording key %s", key));

        if (acc != null) {
            acc.addValue(value);
            storedValues.put(key, acc);
        } else {
            storedValues.put(key, new AccumulatedValues(value));
        }
    }

    @Override
    public double getLatencyEstimation(KeyType key) {
        return storedValues.get(key).getAverage();
    }

    @Override
    public double getDelta(KeyType key) { return getLatencyEstimation(key) - getCacheHitEstimation(); }


    @Override
    public double getCacheHitEstimation() { return 1; }

    private static class AccumulatedValues {
        private double sum;
        private long count;

        public AccumulatedValues(double value) {
            logger.log(Logger.Level.DEBUG, String.format("Adding new value: %f", value));
            this.sum = value;
            this.count = 1;
        }

        public void addValue(double value) {
            logger.log(Logger.Level.DEBUG, String.format("Adding to existing value, sum: %f, count %d, adding %f",
                                                         sum,
                                                         count,
                                                         value));
            sum += value;
            ++count;
        }

        public double getAverage() { return sum / count; }
    }
}
