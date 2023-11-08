package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.policy.EntryData;
import com.github.benmanes.caffeine.cache.simulator.policy.LatencyEstimator;
import com.github.benmanes.caffeine.cache.simulator.policy.linked.CraBlock;
import com.typesafe.config.Config;

import javax.annotation.Nullable;
import java.util.List;

public class LruBlock implements PipelineBlock {
    final private int quantumSize;
    final private CraBlock block;

    public LruBlock(Config config, LatencyEstimator<Long> latencyEstimator, int quantumSize, int initialQuota) {
        this.quantumSize = quantumSize;

        var settings = new LruBlockSettings(config);
        int maxLists = settings.maxLists();
        double decayFactor = settings.decayFactor();

        final int capacity = initialQuota * quantumSize;

        this.block = new CraBlock(decayFactor, maxLists, capacity, latencyEstimator, "LRU-Block");
    }

    @Override
    public void increaseSize(List<EntryData> items) {
        block.increaseSize(quantumSize, items);
    }

    @Override
    public List<EntryData> decreaseSize() {
        Assert.assertCondition(block.capacity() > 0, "Decreasing from empty block");

        return block.decreaseSize(quantumSize);
    }

//    private LruBlock(LruBlock other) {
//        this.quantumSize = other.quantumSize;
//        this.block = other.block.createGhostCopy("LRU copy");
//    }

    @Override
    public PipelineBlock createCopy() {
        return null;
    }

    @Nullable
    @Override
    public EntryData getEntry(long key) {
        EntryData entry = null;

        if (block.isHit(key)) {
            entry = block.get(key);
            block.moveToTail(key);
        }

        return entry;
    }

    @Nullable
    @Override
    public EntryData insert(EntryData data) {
        EntryData victim = null;
        block.addEntry(data);

        if (block.size() > block.capacity()) {
            victim = block.removeVictim();
        }

        return victim;
    }

    @Override
    public EntryData getVictim() {
        return block.findVictim();
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
        Assert.assertCondition(block.capacity() >= 0,
                () -> String.format("Illegal block capacity: %d",
                        block.capacity()));

        Assert.assertCondition(block.size() <= block.capacity(),
                () -> String.format("Block overflow: size: %d, capacity: %d", block.size(), block.capacity()));
    }

    private static class LruBlockSettings extends BasicSettings {
        public LruBlockSettings(Config config) {
            super(config);
        }

        public double decayFactor() { return config().getDouble("decay-factor"); }

        public int maxLists() { return config().getInt("max-lists"); }
    }
}
