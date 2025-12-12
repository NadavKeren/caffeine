package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware;

import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.FetchStage;
import com.typesafe.config.Config;

@SuppressWarnings("NullAway")
@Policy.PolicySpec(name = "latency-aware.Mockup")
public class MockupLAPolicy implements Policy {
    private final PolicyStats stats;
    private final FetchStage fetchStage;

    public MockupLAPolicy(Config config) {
        final String policyName = config.getString("mockup.policy-name");
        stats = new PolicyStats("%s", policyName);
        this.fetchStage = new FetchStage((int) Math.max(Math.min(config.getLong("maximum-size") * 10, Integer.MAX_VALUE >> 10), 100000));
    }

    @Override
    public void record(AccessEvent event) {
        stats.recordOperation();

        while (fetchStage.size() > 0 && fetchStage.getClosestArrival() < event.getRequestTime()) {
            fetchStage.extractClosestArrival();
        }

        if (event.getStatus() == AccessEvent.EventStatus.MISS) {
            if (!fetchStage.contains(event.key())) {
                fetchStage.insert(event);
                stats.recordMiss();
                stats.recordMissPenalty(event.missPenalty());
            } else {
                event.setDelayedHitPenalty(fetchStage.get(event.key()).getAvailabilityTime());
                event.changeEventStatus(AccessEvent.EventStatus.DELAYED_HIT);
                stats.recordDelayedHit();
                stats.recordDelayedHitPenalty(event.delayedHitPenalty());
            }
        } else {
            if (fetchStage.contains(event.key())) {
                event.setDelayedHitPenalty(fetchStage.get(event.key()).getAvailabilityTime());
                event.changeEventStatus(AccessEvent.EventStatus.DELAYED_HIT);
                stats.recordDelayedHit();
                stats.recordDelayedHitPenalty(event.delayedHitPenalty());
            } else {
                stats.recordHit();
                stats.recordHitPenalty(event.hitPenalty());
            }
        }
    }

    @Override
    public String name() {
        return stats().name();
    }

    @Override
    public boolean isPenaltyAware() {
        return true;
    }

    @Override
    public PolicyStats stats() {
        return stats;
    }
}
