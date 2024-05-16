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

    private LatencyEstimator<Long> latencyEstimator;

    protected double normalizationBias = 0;
    protected double normalizationFactor = 0;
    protected double maxDelta = 0;
    protected int maxDeltaCounts = 0;
    protected int samplesCount = 0;

    public LruBlock(Config config, LatencyEstimator<Long> latencyEstimator, int quantumSize, int initialQuota) {
        this.quantumSize = quantumSize;
        this.latencyEstimator = latencyEstimator;

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

    private LruBlock(LruBlock other) {
        this.quantumSize = other.quantumSize;
        this.block = other.block.createGhostCopy("LRU copy");
        this.latencyEstimator = other.latencyEstimator;
    }

    @Override
    public void clear() {
        this.block.clear();
    }

    @Override
    public PipelineBlock createCopy() {
        return new LruBlock(this);
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

    @Override
    public void bookkeeping(long key) {
        updateNormalization(key);
    }

    @Nullable
    @Override
    public EntryData insert(EntryData data) {
        EntryData victim = null;
        block.addEntry(data);

        if (block.size() > block.capacity()) {
            victim = block.removeVictim();
        }

        updateNormalization(data.key());

        return victim;
    }

    public void updateNormalization(long key) {
        double delta = latencyEstimator.getDelta(key);

        if (delta > normalizationFactor) {
            ++samplesCount;
            ++maxDeltaCounts;

            maxDelta = (maxDelta * maxDeltaCounts + delta) / maxDeltaCounts;
        }

        normalizationBias = normalizationBias > 0
                            ? Math.min(normalizationBias, Math.max(0, delta))
                            : Math.max(0, delta);

        if (samplesCount % 1000 == 0 || normalizationFactor == 0) {
            normalizationFactor = maxDelta;
            maxDeltaCounts = 1;
            samplesCount = 0;
        }

        this.block.setNormalization(normalizationBias, normalizationFactor);
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
