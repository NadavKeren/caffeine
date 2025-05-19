package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline;
import com.dynatrace.hash4j.hashing.Hasher64;
import com.dynatrace.hash4j.hashing.XXH3_64;
import com.dynatrace.hash4j.hashing.HashFunnel;
import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;

import java.util.Arrays;

public class XXH3Sampler implements LongSampler{
    private HashFunnel<Long> funnel = (o, sink) -> sink.putLong(o);
    private long mask;

    private Hasher64 hash;
    private int expectedRes;
    private final int[] sampleDist;

    public XXH3Sampler(int order, long seed) {
        mask = createMask(order);
        hash = XXH3_64.create(seed);
        expectedRes = 0;
        sampleDist = new int[1 << order];
    }

    @Override
    public boolean shouldSample(long key) {
        long hashRes = hash.hashToLong(key, funnel);
        int sampleRes = (int)(hashRes & mask);
        ++sampleDist[sampleRes];

        return sampleRes == expectedRes;
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
