package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.rankmap;

import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.PipelinePolicy;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

/***
 * Approximates how much latency the cache would have saved on a sequence of requests to one key.
 * <p>
 * Charging a full miss penalty per request over-states the benefit: once the first request to a key
 * has started a fetch, the requests that arrive while it is in flight are delayed hits and only pay
 * the time left on that fetch. So the benefit is accumulated over a virtual fetch window the way
 * {@code MovingAverageBurstEstimator} does - the first arrival contributes the whole latency and each
 * later one contributes only the remainder - and it is emitted once, when the window closes.
 * <p>
 * The rank vector that comes out with it is the one read when the window <em>opened</em>, because
 * that is the moment whose outcome the window describes: whether the object was resident then is
 * what decides if any of this latency was paid at all.
 */
public final class SequenceBenefitEstimator {
    /*** Receives a closed window: the ranks when it opened, and the latency holding the key would have saved. */
    public interface Sink {
        void accept(int[] ranks, double benefit);
    }

    final private int numOfPartitions;
    final private Long2ObjectMap<Entry> entries;

    public SequenceBenefitEstimator(Config config, int expectedKeys) {
        var settings = new PipelinePolicy.PipelineSettings(config);

        this.numOfPartitions = settings.numOfPartitions();
        this.entries = new Long2ObjectOpenHashMap<>(expectedKeys * 2);
    }

    public void onArrival(AccessEvent event, int[] ranks, Sink sink) {
        final long key = event.key();
        final double latency = event.missPenalty();

        if (latency <= 0) {
            return;
        }

        Entry entry = entries.get(key);
        if (entry == null) {
            entry = new Entry(numOfPartitions);
            entries.put(key, entry);
        }

        entry.recordArrival(event.getRequestTime(), latency, ranks, sink);

        if (entry.isEmpty()) {
            entries.remove(key);
        }
    }

    /***
     * Closes every open window. Called at a decision point so that no window is scored against reach
     * depths other than the ones that were frozen while it was open.
     */
    public void flush(Sink sink) {
        var iterator = Long2ObjectMaps.fastIterator(entries);
        while (iterator.hasNext()) {
            iterator.next().getValue().closeAll(sink);
        }

        entries.clear();
    }

    private static final class Entry {
        final private double[] start;
        final private double[] latency;
        final private double[] benefit;
        final private int[][] ranks;
        private int size = 0;

        Entry(int numOfPartitions) {
            this.start = new double[numOfPartitions];
            this.latency = new double[numOfPartitions];
            this.benefit = new double[numOfPartitions];
            this.ranks = new int[numOfPartitions][];
        }

        boolean isEmpty() {
            return size == 0;
        }

        void recordArrival(double timestamp, double missPenalty, int[] currentRanks, Sink sink) {
            // Windows are opened in time order, so the expired ones are always a prefix.
            int expired = 0;
            while (expired < size && timestamp - start[expired] > latency[expired]) {
                sink.accept(ranks[expired], benefit[expired]);
                ++expired;
            }
            shiftOut(expired);

            for (int idx = 0; idx < size; ++idx) {
                benefit[idx] += latency[idx] - (timestamp - start[idx]);
            }

            final double partitionLength = Math.max(1d, missPenalty / ranksPartitions());
            final boolean withinLastPartition =
                    size > 0 && (timestamp - start[size - 1]) <= partitionLength;

            if (!withinLastPartition && size < start.length) {
                start[size] = timestamp;
                latency[size] = missPenalty;
                benefit[size] = missPenalty;
                ranks[size] = currentRanks;
                ++size;
            }
        }

        private int ranksPartitions() {
            return start.length;
        }

        void closeAll(Sink sink) {
            for (int idx = 0; idx < size; ++idx) {
                sink.accept(ranks[idx], benefit[idx]);
                ranks[idx] = null;
            }

            size = 0;
        }

        private void shiftOut(int count) {
            if (count == 0) {
                return;
            }

            for (int idx = 0; idx < size - count; ++idx) {
                start[idx] = start[idx + count];
                latency[idx] = latency[idx + count];
                benefit[idx] = benefit[idx + count];
                ranks[idx] = ranks[idx + count];
            }

            for (int idx = size - count; idx < size; ++idx) {
                ranks[idx] = null;
            }

            size -= count;
        }
    }
}
