package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline;

import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.policy.EntryData;
import com.github.benmanes.caffeine.cache.simulator.policy.LatencyEstimator;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.BurstBlock;

import javax.annotation.Nullable;
import java.util.List;

public class BurstCache implements PipelineBlock {
    final private int quantumSize;
    final private BurstBlock block;

    public BurstCache(LatencyEstimator<Long> burstEstimator, int maximalCapacity, int quantumSize, int initialQuota) {
        this.quantumSize = quantumSize;

        block = new BurstBlock(maximalCapacity, initialQuota * quantumSize, burstEstimator);
    }

    private BurstCache(BurstCache other) {
        this.quantumSize = other.quantumSize;
        this.block = new BurstBlock(other.block, "BC copy");
    }

    @Override
    public void clear() {
        this.block.clear();
    }

    @Override
    public void setSize(int size) {
        this.block.setSize(size);
    }

    @Override
    public void increaseSize(List<EntryData> items) {
        block.increaseSize(quantumSize, items);
    }

    @Override
    public List<EntryData> decreaseSize() {
        List<EntryData> items = block.decreaseSize(quantumSize);

        return items;
    }

    @Override
    public PipelineBlock createCopy() {
        return new BurstCache(this);
    }

    private @Nullable EntryData addToCacheIfBetter(EntryData item) {
        EntryData evicted = null;

        if (!block.isFull()) {
            block.admit(item);
        } else if (block.compareToVictim(item) > 0) {
            evicted = block.removeVictim();
            block.admit(item);
        } else {
            evicted = item;
        }

        return evicted;
    }

    @Override
    public void copyInto(PipelineBlock other) {
        Assert.assertCondition(other instanceof BurstCache,
                               () -> String.format("Got wrong block type: expected: %s\tgot: %s",
                                                   this.getClass().getSimpleName(),
                                                   other.getClass().getSimpleName()));

        BurstCache casted = (BurstCache) other;

        Assert.assertCondition(casted.quantumSize == this.quantumSize, "copy fail: quantum size mismatch");
        this.block.copyInto(casted.block);
    }

    @Override
    public EntryData getEntry(long key) {
        return this.block.get(key);
    }

    @Override
    public @Nullable EntryData insert(EntryData data) {
        EntryData res = data;
        if (this.block.capacity() > 0) {
            res = addToCacheIfBetter(data);
        }

        return res;
    }

    @Override
    public EntryData getVictim() {
        return block.getVictim();
    }

    @Override
    public int size() {
        return block.size();
    }

    @Override
    public int capacity() {
        return block.capacity();
    }

    @Override
    public void validate() {
        block.validate();
    }
}
