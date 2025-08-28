package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.FetchStage;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.ArrayDeque;
import java.util.Deque;

/***
 * A cache based on the work of G.Yan and J.Li published at ATC22:
 * <a href="https://www.usenix.org/system/files/atc22-yan-gang.pdf">Towards Latency Awareness for Content Delivery Network Caching</a>
 * This uses a list of all items, and picks the lowest ranked item as a victim.
 * The original code is available on <a herf="https://github.com/GYan58/la-cache">github</a> at the class <a herf="https://github.com/GYan58/la-cache/blob/main/Simulator/Delayed-Source-Code/caching/src/cache_la.cpp">cache_la</a>.
 */
@Policy.PolicySpec(name = "latency-aware.YanLi")
public class YanLi implements Policy {
    final protected static int MAX_INTER_ARRIVAL_TIMES = 20;
    final private static float DEFAULT_LOAD_FACTOR = 0.75f;

    private final int cacheSize;
    final private FetchStage fetchStage;

    final private PolicyStats stats;

    final private Long2ObjectMap<Entry> items;
    private int size = 0;

    public YanLi(Config config) {
        stats = new PolicyStats("Yan-Li Cache-LA");
        BasicSettings settings = new BasicSettings(config);
        this.cacheSize = (int) settings.maximumSize();
        items = new Long2ObjectOpenHashMap<>((int) (this.cacheSize / DEFAULT_LOAD_FACTOR), DEFAULT_LOAD_FACTOR);
        this.fetchStage = new FetchStage((int) Math.max(Math.min(cacheSize * 10, Integer.MAX_VALUE >> 10), 100000));
    }

    @Override
    public void record(AccessEvent event) {
        final long key = event.key();

        insertArrivals(event.getRequestTime());

        if (fetchStage.contains(key)) {
            onFetchStageHit(event);
        } else {
            Entry entry = items.get(key);
            if (entry != null) {
                entry.addArrival(event.getRequestTime());
                event.changeEventStatus(AccessEvent.EventStatus.HIT);
                stats.recordHit();
                stats.recordHitPenalty(event.hitPenalty());
            } else {
                stats.recordMiss();
                stats.recordMissPenalty(event.missPenalty());

                Entry newEntry = new Entry(event);
                items.put(key, newEntry);

                fetchStage.insert(event);
            }
        }
    }

    private void insertArrivals(double timestamp) {
        while (fetchStage.size() > 0 && fetchStage.getClosestArrival() < timestamp) {
            AccessEvent arrivedEvent = fetchStage.extractClosestArrival();
            Entry entry = items.get(arrivedEvent.key());
            entry.makeAvailable();
            ++size;

            if (size > cacheSize) {
                evict();
            }
        }
    }

    @SuppressWarnings("deprecation")
    private void evict() {
        stats.recordEviction();

        double minVal = Double.MAX_VALUE;
        long victim = Long.MAX_VALUE;

        for (var mapEntry : items.entrySet()) {
            if (mapEntry.getValue().isAvailable()) {
                double itemScore = mapEntry.getValue().score();
                if (itemScore < minVal) {
                    victim = mapEntry.getKey();
                    minVal = itemScore;
                }
            }
        }

        Assert.assertCondition(minVal < Double.MAX_VALUE && victim != Long.MAX_VALUE, "No victim chosen!");

        items.remove(victim);
        --size;
    }

    private void onFetchStageHit(AccessEvent pendingEvent) {
        AccessEvent fetchingEvent = fetchStage.get(pendingEvent.key());

        pendingEvent.changeEventStatus(AccessEvent.EventStatus.DELAYED_HIT);
        pendingEvent.setDelayedHitPenalty(fetchingEvent.getAvailabilityTime());

        stats.recordDelayedHit();
        stats.recordDelayedHitPenalty(pendingEvent.delayedHitPenalty());
    }

    @Override
    public void finished() {
        Assert.assertCondition(size <= cacheSize, "Cache overflow");
    }

    @Override
    public PolicyStats stats() {
        return stats;
    }

    private static class Entry {
        private boolean isAvailable;
        final private AccessEvent event;
        final private Deque<Double> interArrivalTimes;
        double estimate;
        double lastArrivalTime;

        public Entry(AccessEvent event) {
            this.isAvailable = false;
            this.event = event;
            this.interArrivalTimes = new ArrayDeque<>();
            this.lastArrivalTime = event.getRequestTime();
            this.estimate = 0.00001f; // Used in the original code
        }

        public void addArrival(double arrivalTime) {
            final double interArrivalTime = arrivalTime - lastArrivalTime;
            lastArrivalTime = arrivalTime;
            Assert.assertCondition(interArrivalTime >= 0, "negative inter-arrival time");
            interArrivalTimes.addLast(interArrivalTime);

            if (interArrivalTimes.size() > MAX_INTER_ARRIVAL_TIMES) {
                double removed = interArrivalTimes.removeFirst();
                estimate -= removed;
                estimate += interArrivalTime;
            } else {
                estimate += interArrivalTime;
            }

            Assert.assertCondition(interArrivalTimes.size() <= MAX_INTER_ARRIVAL_TIMES,
                                   () -> String.format("Exceeding the max saved inter-arrival times: %d",
                                                       interArrivalTimes.size()));
            Assert.assertCondition(estimate > 0, "Invalid rate estimate");
        }

        private double rate() { return interArrivalTimes.size() / estimate; }

        public double score() {
            final double rate = rate();
            final double rateLatencyMulti = rate * event.delta();
            final double numerator = rateLatencyMulti * (1 + rateLatencyMulti);
            final double denominator = 2 + rateLatencyMulti;

            return numerator / denominator;
        }

        public void makeAvailable() {
            this.isAvailable = true;
        }

        public boolean isAvailable() {
            return isAvailable;
        }
    }
}
