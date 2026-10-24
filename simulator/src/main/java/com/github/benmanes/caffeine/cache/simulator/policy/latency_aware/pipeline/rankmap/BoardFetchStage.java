package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.rankmap;

import java.util.PriorityQueue;
import java.util.function.LongConsumer;

import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

/***
 * The keys one board has missed and is still fetching, the board's counterpart of the pipeline's
 * {@code FetchStage}.
 * <p>
 * The pipeline does not put a missed key into its blocks at the miss. The key waits in the fetch stage
 * for one miss penalty, requests to it meanwhile are delayed hits, and it is inserted only once a later
 * request finds the fetch complete. A board that inserted at the miss would rank the key at the top for
 * the whole fetch, crediting every candidate with hits the cache cannot have, and would time its recency
 * from the request instead of from the insertion.
 * <p>
 * Each board keeps its own, since a key can be on one board and missing from another.
 */
final class BoardFetchStage {
    private record Pending(double availabilityTime, long sequence, long key) {}

    final private PriorityQueue<Pending> arrivals = new PriorityQueue<>((a, b) -> {
        final int byTime = Double.compare(a.availabilityTime(), b.availabilityTime());
        return byTime != 0 ? byTime : Long.compare(a.sequence(), b.sequence());
    });
    final private Long2DoubleMap availability = new Long2DoubleOpenHashMap();
    private long sequence = 0;

    boolean contains(long key) {
        return availability.containsKey(key);
    }

    /*** When the key's fetch completes. Only meaningful for a key the stage {@link #contains}. */
    double availabilityTime(long key) {
        return availability.get(key);
    }

    void add(long key, double availabilityTime) {
        availability.put(key, availabilityTime);
        arrivals.add(new Pending(availabilityTime, sequence++, key));
    }

    /***
     * Hands out, in completion order, every key whose fetch completed strictly before {@code time}: the
     * same rule {@code PipelinePolicy.insertArrivals} applies before serving a request at that time.
     */
    void completeBefore(double time, LongConsumer arrived) {
        while (!arrivals.isEmpty() && arrivals.peek().availabilityTime() < time) {
            final long key = arrivals.poll().key();
            availability.remove(key);
            arrived.accept(key);
        }
    }

    void forEachKey(LongConsumer consumer) {
        availability.keySet().forEach(consumer);
    }

    int size() {
        return availability.size();
    }
}
