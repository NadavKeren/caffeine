package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline;
import com.dynatrace.hash4j.hashing.Hasher64;
import com.dynatrace.hash4j.hashing.XXH3_64;
import com.dynatrace.hash4j.hashing.HashFunnel;

public class XXH3Sampler implements LongSampler{
    private HashFunnel<Long> funnel = (o, sink) -> sink.putLong(o);
    private long mask;

    private Hasher64 hash;
    public XXH3Sampler(int order, long seed) {
        mask = createMask(order);
        hash = XXH3_64.create(seed);
    }

    @Override
    public boolean shouldSample(long key) {
        return (hash.hashToLong(key, funnel) & mask) == 0;
    }

    private static long createMask(int exp) {
        long mask = 0;
        for (int i = 0; i < exp; ++i) {
            mask = mask << 1 | 1;
        }

        return mask;
    }
}
