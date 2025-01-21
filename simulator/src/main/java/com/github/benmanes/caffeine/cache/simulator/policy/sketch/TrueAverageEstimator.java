package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import com.github.benmanes.caffeine.cache.simulator.policy.LatencyEstimator;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import static java.lang.System.Logger;

public class TrueAverageEstimator implements LatencyEstimator {
    private static int INITIAL_SIZE = 1000000;
    private static float LOAD_FACTOR = 0.875f;
    private Long2ObjectOpenHashMap<AccumulatedValues> storedValues;

    private static final Logger logger = System.getLogger(LatencyEstimator.class.getName());

    public TrueAverageEstimator() {
        storedValues = new Long2ObjectOpenHashMap<>(INITIAL_SIZE, LOAD_FACTOR);
    }

    @Override
    public void record(long key, double value, double recordTime) {
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
    public double getLatencyEstimation(long key) {
        return storedValues.get(key).getAverage();
    }

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
