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
    final protected double smoothDownFactor;
    final protected double smoothUpFactor;
    final protected long agingWindowSize;
    final protected double ageSmoothingFactor;
    final protected int numOfPartitions;

    public MovingAverageBurstLatencyEstimator(double smoothUpFactor, double smoothDownFactor, long agingWindowSize, double ageSmoothingFactor, int numOfPartitions) {
        storedValues = new HashMap<>(INITIAL_SIZE, LOAD_FACTOR);

        checkState(smoothUpFactor >= 0 && smoothUpFactor <= 1,
                   "Illegal Smoothing-Factor, must be in the range [0, 1]");
        checkState(numOfPartitions > 0, "Illegal number of Partitions");
        this.smoothDownFactor = smoothDownFactor;
        this.smoothUpFactor = smoothUpFactor;
        this.agingWindowSize = agingWindowSize;
        this.ageSmoothingFactor = ageSmoothingFactor;
        this.numOfPartitions = numOfPartitions;

        System.out.println(String.format("DF: %.3f UP: %.3f aging-window: %d ASF: %.4f partitions: %d", smoothDownFactor, smoothUpFactor, agingWindowSize, ageSmoothingFactor, numOfPartitions));

        if (DEBUG) {
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
    public double getLatencyEstimation(KeyType key) {
        return storedValues.get(key).getMovingAverage();
    }

    @Override
    public double getLatencyEstimation(KeyType key, double time) {
        Entry entry = storedValues.get(key);
        if (entry == null) {
            throw new IllegalArgumentException(String.format("Key %s was not present during update attempt", key));
        }

        int numOfTicks = entry.getNumberOfTicksSinceUpdate(time);
        double estimation = entry.getMovingAverage();

        if (numOfTicks > numOfPartitions * agingWindowSize) {
            int numOfAgingDecays = (int) (numOfTicks / (numOfPartitions * agingWindowSize));

            estimation = estimation * Math.pow(1 - ageSmoothingFactor, numOfAgingDecays);
        }

        return estimation;
    }

    private class Entry {
        final KeyType key;
        final private int[] arrivalCounters;
        final private double latency;
        private double lastTickTime;
        private int tick;
        final private double tickDuration;
        private double maxValueInWindow;
        private double movingAverage;
        private long updateNum;


        public Entry(KeyType key, double latency, double currTime) {
            this.key = key;
            this.arrivalCounters = new int[numOfPartitions];
            arrivalCounters[0] = 1;
            this.lastTickTime = currTime;
            this.tick = 0;

            this.tickDuration = latency / numOfPartitions;

            this.latency = latency;
            this.maxValueInWindow = latency;
            this.movingAverage = 0;
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

        private void updateMovingAverage() {
            if (maxValueInWindow > latency) {
                if (DEBUG) {
                    logger.warning(String.format("%s, %d, %.2f, %.2f%n",
                                                 key.toString(),
                                                 updateNum++,
                                                 movingAverage,
                                                 maxValueInWindow));
                }

                final double smoothFactor = movingAverage < maxValueInWindow ? smoothUpFactor : smoothDownFactor;

                movingAverage = movingAverage > 0
                                ? (1 - smoothFactor) * movingAverage + smoothFactor * (maxValueInWindow - latency)
                                : maxValueInWindow;
            }
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

        private void updateCurrMax() {
            updateCurrMax(0);
        }

        public void onWindowEnd(double arrivalTime, int numOfAgingDecays) {
            if (numOfAgingDecays > 0) {
                ageMovingAverage(numOfAgingDecays);
            }

            updateMovingAverage();

            startNewWindow(arrivalTime);
        }

        public void addArrival(double value, double arrivalTime) {
            int numberOfTicksSinceLastUpdate = getNumberOfTicksSinceUpdate(arrivalTime);
            if (numberOfTicksSinceLastUpdate > 0) {
                for (int i = 0; i < Math.min(numberOfTicksSinceLastUpdate, numOfPartitions); ++i) {
                    moveCountersUp();
                    updateCurrMax();
                }

                tick += numberOfTicksSinceLastUpdate;

                if (tick >= numOfPartitions) {
                    int numOfAgingDecays = (int) (numberOfTicksSinceLastUpdate / (numOfPartitions * agingWindowSize));
                    onWindowEnd(arrivalTime, numOfAgingDecays);
                } else {
                    lastTickTime = lastTickTime + numberOfTicksSinceLastUpdate * tickDuration;
                }
            }

            arrivalCounters[0] += 1;
            updateCurrMax(value);
        }

        private void ageMovingAverage(int numOfAgingDecays) {
            movingAverage = movingAverage * Math.pow(1 - ageSmoothingFactor, numOfAgingDecays);
        }

        public int getNumberOfTicksSinceUpdate(double time) {
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

        public double getMovingAverage() {return movingAverage > 0 ? movingAverage : maxValueInWindow;}
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
                FileHandler fileHandler = new FileHandler(String.format("moving-average-updates-SD-%.1f-SU-%.1f-t-%s.log",
                                                                        this.smoothDownFactor,
                                                                        this.smoothUpFactor,
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
