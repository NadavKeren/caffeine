package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.typesafe.config.Config;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/***
 * A cache based on the work of G.Yan and J.Li published at ATC22:
 * <a href="https://www.usenix.org/system/files/atc22-yan-gang.pdf">Towards Latency Awareness for Content Delivery Network Caching</a>
 * This uses a list of all items, and picks the lowest ranked item as a victim.
 * The original code is available on <a herf="https://github.com/GYan58/la-cache">github</a> at the class <a herf="https://github.com/GYan58/la-cache/blob/main/Simulator/Delayed-Source-Code/caching/src/cache_la.cpp">cache_la</a>.
 */
@Policy.PolicySpec(name = "latency-aware.YanLi")
public class YanLi implements Policy {
    final protected static int MAX_INTER_ARRIVAL_TIMES = 20;
    final private static int DEFAULT_SIZE = 1 << 15;
    final private static float DEFAULT_LOAD_FACTOR = 1.5f;

    private final int cacheSize;

    final private PolicyStats stats;

    final private Map<Long, Entry> items;

    public YanLi(Config config) {
        items = new HashMap<>(DEFAULT_SIZE, DEFAULT_LOAD_FACTOR);
        stats = new PolicyStats("Yan-Li Cache-LA");
        BasicSettings settings = new BasicSettings(config);
        this.cacheSize = (int) settings.maximumSize();
    }

    @Override
    public void record(AccessEvent event) {
        final long key = event.key();

        Entry entry = items.get(key);
        if (entry != null) {
            entry.addArrival(event.getArrivalTime());
            recordAccordingToAvailability(entry, event);
        } else {
            stats.recordMiss();
            stats.recordMissPenalty(event.missPenalty());

            Entry newEntry = new Entry(event);
            items.put(key, newEntry);

            if (items.size() > cacheSize) {
                evict();
            }
        }
    }

    private void evict() {
        stats.recordEviction();

        double minVal = Double.MAX_VALUE;
        long victim = Long.MAX_VALUE;

        for (var mapEntry : items.entrySet()) {
            double itemScore = mapEntry.getValue().score();
            if (itemScore < minVal) {
                victim = mapEntry.getKey();
                minVal = itemScore;
            }
        }

        Assert.assertCondition(minVal < Double.MAX_VALUE && victim != Long.MAX_VALUE, "No victim chosen!");

        items.remove(victim);
    }

    private void recordAccordingToAvailability(Entry entry, AccessEvent currEvent) {
        boolean isAvailable = entry.event().isAvailableAt(currEvent.getArrivalTime());
        if (isAvailable) {
            currEvent.changeEventStatus(AccessEvent.EventStatus.HIT);
            stats.recordHit();
            stats.recordHitPenalty(currEvent.hitPenalty());
        } else {
            currEvent.changeEventStatus(AccessEvent.EventStatus.DELAYED_HIT);
            currEvent.setDelayedHitPenalty(entry.event().getAvailabilityTime());
            stats.recordDelayedHitPenalty(currEvent.delayedHitPenalty());
            stats.recordDelayedHit();
        }
    }

    @Override
    public void finished() {
        Assert.assertCondition(items.size() <= cacheSize, "Cache overflow");
    }

    @Override
    public PolicyStats stats() {
        return stats;
    }

    private static class Entry {

        final private AccessEvent event;
        final private Deque<Double> interArrivalTimes;
        double estimate;
        double lastArrivalTime;

        public Entry(AccessEvent event) {
            this.event = event;
            this.interArrivalTimes = new ArrayDeque<>();
            this.lastArrivalTime = event.getArrivalTime();
            this.estimate = 0.00001f; // Used in the original code
        }

        public void addArrival(double arrivalTime) {
            final double interArrivalTime = arrivalTime - lastArrivalTime;
            Assert.assertCondition(interArrivalTime > 0, "negative inter-arrival time");
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

        public AccessEvent event() { return event; }
    }
}
