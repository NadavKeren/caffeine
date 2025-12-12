package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline;

public interface LongSampler {
    boolean shouldSample(long key);
}
