package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import com.github.benmanes.caffeine.cache.simulator.policy.LatencyEstimator;

import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.*;
import java.time.ZoneId;

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
    private static Logger logger = null;
    final private static boolean DEBUG = false;
    final private static int INITIAL_SIZE = 1000000;
    final private static float LOAD_FACTOR = 2.0f;
    protected Map<KeyType, Entry> storedValues;
    final protected long agingWindowSize;
    final protected double ageSmoothingFactor;
    final protected int numOfPartitions;

    private double hitPenalty;

    public MovingAverageBurstLatencyEstimator(long agingWindowSize, double ageSmoothingFactor, int numOfPartitions) {
        storedValues = new HashMap<>(INITIAL_SIZE, LOAD_FACTOR);

        checkState(numOfPartitions > 0, "Illegal number of Partitions");
        this.agingWindowSize = agingWindowSize;
        this.ageSmoothingFactor = ageSmoothingFactor;
        this.numOfPartitions = numOfPartitions;
        this.hitPenalty = 1;

        if (DEBUG) {
            System.out.println(String.format("Aging-window: %d ASF: %.4f partitions: %d", agingWindowSize, ageSmoothingFactor, numOfPartitions));
            setLogger();
        }
    }

    @Override
    public void record(KeyType key, double value, double recordTime) {
        storedValues.put(key, new Entry(key, value, recordTime));
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
    public void recordHit(double value) {
        this.hitPenalty = value;
    }

    @Override
    public double getCacheHitEstimation() {
        return this.hitPenalty;
    }

    @Override
    public double getLatencyEstimation(KeyType key) {
        var entry = storedValues.get(key);
        return entry != null ? entry.getValue() : 0;
    }

    @Override
    public double getLatencyEstimation(KeyType key, double time) {
        Entry entry = storedValues.get(key);
        if (entry == null) {
            return 0;
        }

        return entry.getValue(time);
    }

    private class Entry {
        final KeyType key;
        final private int[] arrivalCounters;
        final private double latency;
        private double lastTickTime;
        private double lastUpdateTime;
        private int tick;
        final private double tickDuration;
        final private double agingWindowDuration;
        private double maxValueInWindow;
        private double value;
        private long updateNum;


        public Entry(KeyType key, double latency, double currTime) {
            this.key = key;
            this.arrivalCounters = new int[numOfPartitions];
            arrivalCounters[0] = 1;
            this.lastTickTime = currTime;
            this.lastUpdateTime = currTime;
            this.tick = 0;

            this.tickDuration = latency / numOfPartitions;
            this.agingWindowDuration = latency * agingWindowSize;

            this.latency = latency;
            this.maxValueInWindow = latency;
            this.value = 0;
            this.updateNum = 0;
        }

        private double calculateEstimationOfCounters() {
            double res = 0d;
            double partitionSize = latency / numOfPartitions;
            for (int idx = 0; idx < numOfPartitions; ++idx) {
                res += arrivalCounters[idx] * (idx + 1) * partitionSize;
            }

            return res;
        }

        private void updateValue(double time) {
            if (DEBUG) {
                logger.warning(String.format("%s, %d, %.2f, %.2f%n",
                                             key.toString(),
                                             updateNum++,
                                             value,
                                             maxValueInWindow));
            }

            if (maxValueInWindow > value) {
                lastUpdateTime = time;
                value = maxValueInWindow;
            }

            maxValueInWindow = 0;
        }

        private void updateCurrMax(double value) {
            if (this.value == 0) {
                maxValueInWindow += value;
            } else {
                final double currEstimation = calculateEstimationOfCounters();

                if (currEstimation > maxValueInWindow) {
                    maxValueInWindow = currEstimation;
                }
            }
        }

        private void updateCurrMax() {
            updateCurrMax(0);
        }

        public void onWindowEnd(double arrivalTime) {
            ageValue(arrivalTime);

            updateValue(arrivalTime);

            startNewWindow(arrivalTime);
        }

        public void addArrival(double value, double arrivalTime) {
            int numberOfTicksSinceLastAddition = getNumberOfTicksSinceAddition(arrivalTime);
            if (numberOfTicksSinceLastAddition > 0) {
                for (int i = 0; i < Math.min(numberOfTicksSinceLastAddition, numOfPartitions); ++i) {
                    moveCountersUp();
                    updateCurrMax();
                }

                tick += numberOfTicksSinceLastAddition;

                if (tick >= numOfPartitions) {
                    onWindowEnd(arrivalTime);
                } else {
                    lastTickTime = lastTickTime + numberOfTicksSinceLastAddition * tickDuration;
                }
            }

            arrivalCounters[0] += 1;
            updateCurrMax(value);
        }

        private void ageValue(double time) {
            int numOfAgingDecays = (int) ((time - lastUpdateTime) / agingWindowDuration);

            if (numOfAgingDecays > 0) {
                value *= Math.pow(1 - ageSmoothingFactor, numOfAgingDecays);
                lastUpdateTime = time;
            }
        }

        public int getNumberOfTicksSinceAddition(double time) {
            return (int) ((time - lastTickTime) / tickDuration);
        }

        private void startNewWindow(double startTime) {
            lastTickTime = startTime;
            tick = 0;
            maxValueInWindow = 0;
        }

        private void moveCountersUp() {
            for (int idx = numOfPartitions - 1; idx > 0; --idx) {
                this.arrivalCounters[idx] = arrivalCounters[idx - 1];
            }

            arrivalCounters[0] = 0;
        }

        public double getValue(double time) {
            ageValue(time);

            return value > 0 ? value : maxValueInWindow;
        }

        public double getValue() {
            return value > 0 ? value : maxValueInWindow;
        }
    }

    private void setLogger() {
        if (logger == null) {
            logger = Logger.getLogger("");
            LocalDateTime currentTime = LocalDateTime.now(ZoneId.systemDefault());
            DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("dd-MM-HH-mm-ss");
            logger.setLevel(Level.ALL);
            var handlers = logger.getHandlers();
            logger.removeHandler(handlers[0]);
            try {
                FileHandler fileHandler = new FileHandler(String.format("moving-average-updates-AW-%d-ASF-%.1f-t-%s.log",
                                                                        this.agingWindowSize,
                                                                        this.ageSmoothingFactor,
                                                                        currentTime.format(timeFormatter)));
                Formatter logFormatter = new Formatter() {
                    @Override
                    public String format(LogRecord record) {
                        return record.getMessage();
                    }
                };

                fileHandler.setFormatter(logFormatter);
                fileHandler.setLevel(Level.ALL);
                logger.setUseParentHandlers(false);
                logger.addHandler(fileHandler);
            } catch (IOException e) {
                System.err.println("Error creating the log file handler");
                e.printStackTrace();
                System.exit(1);
            }
        }
    }
}
