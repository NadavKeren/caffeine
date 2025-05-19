package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline;

import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.Arrays;

public class FarmHashSampler implements LongSampler {
    private final HashFunction hash;
    private final long mask;
    private final int[] sampleDist;
    private int expectedRes;

    public FarmHashSampler(int order) {
        hash = Hashing.farmHashFingerprint64();
        mask = createMask(order);
        sampleDist = new int[1 << order];
        expectedRes = 0;
    }

    @Override
    public boolean shouldSample(long key) {
        HashCode code = hash.hashLong(key);
        int sampleRes = (int)(code.asLong() & mask);
        ++sampleDist[sampleRes];

        return (sampleRes) == expectedRes;
    }

    private static long createMask(int exp) {
        long mask = 0;
        for (int i = 0; i < exp; ++i) {
            mask = mask << 1 | 1;
        }

        return mask;
    }

    @Override
    public int[] sampleDist() {
        return sampleDist;
    }

    @Override
    public void resetSampleDist() {
        Arrays.fill(sampleDist, 0);
    }

    @Override
    public void changeSample(int i) {
        Assert.assertCondition(i >= 0 && i < sampleDist.length, "Illegal sample result provided");
        expectedRes = i;
    }

    @Override
    public int getCurrentExpectedResult() {
        return expectedRes;
    }
}
