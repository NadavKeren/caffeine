package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class FarmHashSampler implements LongSampler {
    private final HashFunction hash;
    private final long mask;

    public FarmHashSampler(int order) {
        hash = Hashing.farmHashFingerprint64();
        mask = createMask(order);
    }

    @Override
    public boolean shouldSample(long key) {
        HashCode code = hash.hashLong(key);
        return (code.asLong() & mask) == 0;
    }

    private static long createMask(int exp) {
        long mask = 0;
        for (int i = 0; i < exp; ++i) {
            mask = mask << 1 | 1;
        }

        return mask;
    }
}
