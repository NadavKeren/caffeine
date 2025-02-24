package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline;

import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.policy.LatencyEstimator;
import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class MovingAverageBurstEstimator implements LatencyEstimator {
    final private static float LOAD_FACTOR = 0.875f;
    final protected Long2ObjectOpenHashMap<Entry> storedValues;
    final protected long agingWindowSize;
    final protected double ageSmoothingFactor;
    final protected int numOfPartitions;
    final protected int maxSize;

    private double hitPenalty;

    public MovingAverageBurstEstimator(long agingWindowSize,
                                       double ageSmoothingFactor,
                                       int numOfPartitions,
                                       int maxSize) {
        this.storedValues = new Long2ObjectOpenHashMap<>(maxSize, LOAD_FACTOR);
        this.agingWindowSize = agingWindowSize;
        this.ageSmoothingFactor = ageSmoothingFactor;
        this.numOfPartitions = numOfPartitions;
        this.maxSize = maxSize;
    }

    private MovingAverageBurstEstimator(MovingAverageBurstEstimator source) {
        this.agingWindowSize = source.agingWindowSize;
        this.ageSmoothingFactor = source.ageSmoothingFactor;
        this.numOfPartitions = source.numOfPartitions;
        this.maxSize = source.maxSize;

        this.storedValues = new Long2ObjectOpenHashMap<>(maxSize, LOAD_FACTOR);
        var itr = Long2ObjectMaps.fastIterator(source.storedValues);

        while (itr.hasNext()) {
            var item = itr.next();
            this.storedValues.put(item.getLongKey(), new Entry(item.getValue()));
        }
    }

    @Override
    public LatencyEstimator createDeepCopy() {
        return new MovingAverageBurstEstimator(this);
    }

    @Override
    public void record(long key, double value, double recordTime) {
        Assert.assertCondition(!storedValues.containsKey(key), () -> String.format("Found the key inside the estimator %d", key));
        Entry newEntry = new Entry(value);
        newEntry.recordArrival((long) recordTime);

        storedValues.put(key, newEntry);
    }

    @Override
    public void addValueToRecord(long key, double value, double recordTime) {
        Entry entry = storedValues.get(key);
        Assert.assertCondition(entry != null, () -> String.format("Trying to update a non-existing item: %d", key));

        entry.recordArrival((long) recordTime);
    }

    @Override
    public void recordHit(double value) {
        this.hitPenalty = value;
    }

    @Override
    public double getLatencyEstimation(long key) {
        Entry entry = storedValues.get(key);

        return entry != null ? entry.getValue() : 0;
    }

    @Override
    public double getLatencyEstimation(long key, double time) {
        Entry entry = storedValues.get(key);

        return entry != null ? entry.getValue((long) time) : 0;
    }

    @Override
    public double getCacheHitEstimation() {
        return hitPenalty;
    }

    @Override
    public void remove(long key) {
        Assert.assertCondition(storedValues.containsKey(key),
                               () -> String.format("Trying to remove a non-existing item: %d", key));

        storedValues.remove(key);
    }

    @Override
    public int size() {
        return this.storedValues.size();
    }

    protected class Entry {
        long[] virtualFetchTimestamps;
        double[] accumulators;
        int size = 0;

        final private double latency;
        private long lastUpdateTime = 0;
        final private double agingWindowDuration;
        final private long partitionLength;
        private double value = 0d;

        public Entry(double latency) {
            virtualFetchTimestamps = new long[numOfPartitions];
            accumulators = new double[numOfPartitions];
            this.latency = latency;
            agingWindowDuration = latency * agingWindowSize;
            partitionLength = (long) latency / numOfPartitions;
        }

        public Entry(Entry source) {
            this.virtualFetchTimestamps = new long[source.virtualFetchTimestamps.length];
            System.arraycopy(source.virtualFetchTimestamps, 0, this.virtualFetchTimestamps, 0, source.virtualFetchTimestamps.length);
            this.accumulators = new double[source.accumulators.length];
            System.arraycopy(source.accumulators, 0, this.accumulators, 0, source.accumulators.length);
            this.size = source.size;

            this.latency = source.latency;
            this.lastUpdateTime = source.lastUpdateTime;
            this.agingWindowDuration = source.agingWindowDuration;
            this.partitionLength = source.partitionLength;
            this.value = source.value;
        }

        private void ageValueIfNeeded(long timestamp) {
            int numOfAgingDecays = (int) ((timestamp - lastUpdateTime) / agingWindowDuration);

            if (numOfAgingDecays > 0) {
                value *= Math.pow(1 - ageSmoothingFactor, numOfAgingDecays);
                lastUpdateTime = timestamp;
            }
        }

        private void shiftAccumulators(int numOfIdxToShift) {
            for (int idx = 0; idx < size - numOfIdxToShift; ++idx) {
                virtualFetchTimestamps[idx] = virtualFetchTimestamps[idx + numOfIdxToShift];
                accumulators[idx] = accumulators[idx + numOfIdxToShift];
            }

            size = size - numOfIdxToShift;
        }

        public void recordArrival(long timestamp) {
            ageValueIfNeeded(timestamp);

            int numOfIdxToShift = 0;
            for (int idx = 0; idx < size; ++idx) {
                final long currVirtualFetchTimestamp = virtualFetchTimestamps[idx];
                final long timeSinceVirtualFetch = timestamp - currVirtualFetchTimestamp;

                if (timeSinceVirtualFetch > latency) {
                    ++numOfIdxToShift;

                } else {
                    Assert.assertCondition(timeSinceVirtualFetch >= 0, () -> String.format("Got a timestamp that is before an existing virtual fetch request; Timestamp: %d\tVirtual fetch timestamp: %d", timestamp, currVirtualFetchTimestamp));

                    accumulators[idx] += (latency - timeSinceVirtualFetch);

                    if (accumulators[idx] > value) {
                        value = accumulators[idx];
                    }
                }

            }

            shiftAccumulators(numOfIdxToShift);

            lastUpdateTime = timestamp;

            validateAccumulators(timestamp);

            final long timeSinceLastVirtualFetch = size > 0
                                                   ? timestamp - virtualFetchTimestamps[size - 1]
                                                   : Long.MAX_VALUE;
            if (timeSinceLastVirtualFetch > partitionLength) {
                virtualFetchTimestamps[size] = timestamp;
                accumulators[size] = latency;
                ++size;
            }
        }

        private void validateAccumulators(long timestamp) {
            for (int idx = 0; idx < size; ++idx) {
                if (timestamp - virtualFetchTimestamps[idx] > latency) {
                    throw new AssertionError("Error");
                }
            }
        }

        public double getValue(long timestamp) {
            Assert.assertCondition(timestamp >= lastUpdateTime, "Past timestamp given");
            ageValueIfNeeded(timestamp);

            return value;
        }

        public double getValue() {
            return getValue(lastUpdateTime);
        }
    }
}
