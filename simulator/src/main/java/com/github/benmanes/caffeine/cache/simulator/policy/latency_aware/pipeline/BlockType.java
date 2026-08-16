package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline;

public enum BlockType {
    LRU,
    LFU,
    LA_LRU,
    LA_LFU,
    LBU;

    @Override
    public String toString() {
        return name().replace('_', '-');
    }
}
