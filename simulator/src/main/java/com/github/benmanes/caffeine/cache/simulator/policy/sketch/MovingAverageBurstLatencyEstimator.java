package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import com.github.benmanes.caffeine.cache.simulator.policy.LatencyEstimator;

import static com.google.common.base.Preconditions.checkState;

import java.util.Map;
import java.util.HashMap;

/***
 * This estimator uses moving average in order to "age" the burst estimation.
 * The values added to the average is the maximum over a latest-latency-sized window.
 *
 * This code uses Exponential Smoothing for the moving average, further reading in:
 * <a href="https://en.wikipedia.org/wiki/Exponential_smoothing">Exponential Smoothing - Wikipedia</a>
 *
 * Smoothing Factor = alpha
 *
 */
public class MovingAverageBurstLatencyEstimator<KeyType> implements LatencyEstimator<KeyType> {
    private static int INITIAL_SIZE = 1000000;
    private static float LOAD_FACTOR = 2.0f;
    protected Map<KeyType, Entry> storedValues;
    final protected double smoothingFactor;
    final protected int numOfPartitions;

    public MovingAverageBurstLatencyEstimator(double smoothingFactor, int numOfPartitions) {
        storedValues = new HashMap<>(INITIAL_SIZE, LOAD_FACTOR);

        checkState(smoothingFactor >= 0 && smoothingFactor <= 1, "Illegal Smoothing-Factor, must be in the range [0, 1]");
        checkState(numOfPartitions > 0, "Illegal number of Partitions");
        this.smoothingFactor = smoothingFactor;
        this.numOfPartitions = numOfPartitions;
    }

    @Override
    public void record(KeyType key, double value, double recordTime) {
        storedValues.put(key, new Entry(value, recordTime));
    }

    @Override
    public void addValueToRecord(KeyType key, double value, double recordTime) {
        Entry entry = storedValues.get(key);
        if (entry == null) {
            throw new IllegalArgumentException(String.format("Key %s was not present during update attempt", key));
        }

        entry.addArrival(value, recordTime);
    }

    @Override
    public double getLatencyEstimation(KeyType key) {
        return storedValues.get(key).getMovingAverage();
    }

    private class Entry {
        private int[] arrivalCounters;
        final private double latency;
        private double lastTickTime;
        private int tick;
        final private double tickDuration;
        private double maxValueInWindow;
        private double movingAverage;


        public Entry(double latency, double currTime) {
            this.arrivalCounters = new int[numOfPartitions];
            arrivalCounters[0] = 1;
            this.lastTickTime = currTime;
            this.tick = 0;

            this.tickDuration = latency / numOfPartitions;

            this.latency = latency;
            this.maxValueInWindow = latency;
            this.movingAverage = 0;
        }

        private double calculateEstimationOfCounters() {
            double res = 0d;
            double partitionSize = latency / numOfPartitions;
            for (int idx = 0; idx < numOfPartitions; ++idx) {
                res += arrivalCounters[idx] * (idx + 1) * partitionSize;
            }

            return res;
        }

        private void updateMovingAverage() {
            movingAverage = movingAverage > 0
                          ? (1 - smoothingFactor) * movingAverage + smoothingFactor * maxValueInWindow
                          : maxValueInWindow;
            maxValueInWindow = 0;
        }

        private void updateCurrMax(double value) {
            if (movingAverage == 0) {
                maxValueInWindow += value;
            } else {
                final double currEstimation = calculateEstimationOfCounters();

                if (currEstimation > maxValueInWindow) {
                    maxValueInWindow = currEstimation;
                }
            }
        }

        public void onWindowEnd(double arrivalTime) {
            updateMovingAverage();

            startNewWindow(arrivalTime);
        }

        public void addArrival(double value, double arrivalTime) {
            int numberOfTicksSinceLastUpdate = (int)((arrivalTime - lastTickTime) / tickDuration);
            if (numberOfTicksSinceLastUpdate > 0) {
                moveCountersUp(numberOfTicksSinceLastUpdate);
                tick += numberOfTicksSinceLastUpdate;

                if (tick >= numOfPartitions) {
                    onWindowEnd(arrivalTime);
                } else {
                    lastTickTime = lastTickTime + numberOfTicksSinceLastUpdate * tickDuration;
                }
            }

            arrivalCounters[0] += 1;
            updateCurrMax(value);
        }

        private void startNewWindow(double startTime) {
            lastTickTime = startTime;
            tick = 0;
            maxValueInWindow = 0;
        }

        private void moveCountersUp(int numOfIndexes) {
            if (numOfIndexes > numOfPartitions) {
                numOfIndexes = numOfPartitions;
            }

            for (int idx = numOfPartitions - 1; idx >= numOfIndexes; --idx) {
                this.arrivalCounters[idx] = arrivalCounters[idx - numOfIndexes];
            }

            for (int idx = 0; idx < numOfIndexes; ++idx) {
                arrivalCounters[idx] = 0;
            }
        }

        public double getMovingAverage() { return movingAverage > 0 ? movingAverage : maxValueInWindow; }
    }
}
