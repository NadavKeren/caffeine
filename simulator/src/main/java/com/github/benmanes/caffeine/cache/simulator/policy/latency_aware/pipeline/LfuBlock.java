package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.admission.Admittor;
import com.github.benmanes.caffeine.cache.simulator.admission.LATinyLfu;
import com.github.benmanes.caffeine.cache.simulator.admission.UneditableAdmittorProxy;
import com.github.benmanes.caffeine.cache.simulator.policy.EntryData;
import com.github.benmanes.caffeine.cache.simulator.policy.LatencyEstimator;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.linked.CraBlock;
import com.typesafe.config.Config;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class LfuBlock implements PipelineBlock {
    final private int quantumSize;
    final private Admittor admittor;
    final private CraBlock protectedBlock;
    final private CraBlock probationBlock;
    private int capacity;

    public LfuBlock(Config config,
                    Config blockConfig,
                    LatencyEstimator<Long> latencyEstimator,
                    int quantumSize,
                    int initialQuota) {
        this.quantumSize = quantumSize;
        var settings = new LfuBlockSettings(blockConfig);
        final double decayFactor = settings.decayFactor();
        final int maxLists = settings.maxLists();

        this.admittor = new LATinyLfu(config, new PolicyStats("LATinyLFU"), latencyEstimator);

        if (initialQuota >= 1) {
            this.probationBlock = new CraBlock(decayFactor, maxLists, quantumSize, latencyEstimator, "probation");
            this.protectedBlock = new CraBlock(decayFactor,
                                               maxLists,
                                               ((initialQuota - 1) * quantumSize),
                                               latencyEstimator,
                                               "protected");
        } else {
            this.probationBlock = new CraBlock(decayFactor, maxLists, 0, latencyEstimator, "probation");
            this.protectedBlock = new CraBlock(decayFactor, maxLists, 0, latencyEstimator, "protected");
        }

        this.capacity = quantumSize * initialQuota;
    }


    @Override
    public void increaseSize(List<EntryData> items) {
        Assert.assertCondition(items.size() <= quantumSize, "Offered too many items on increase");

        this.capacity += quantumSize;
        if (probationBlock.capacity() == 0) {
            probationBlock.increaseSize(quantumSize, items);
        } else {
            protectedBlock.increaseCapacity(quantumSize);
            probationBlock.appendItems(items);
        }
    }

    @Override
    public List<EntryData> decreaseSize() {
        Assert.assertCondition(protectedBlock.capacity() + probationBlock.capacity() > 0, "Decreasing from empty block");
        final int numOfItems = Math.min(size(), quantumSize);
        List<EntryData> items = new ArrayList<>(numOfItems);

        for (int i = 0; i < numOfItems; ++i) {
            if (probationBlock.size() <= probationBlock.capacity() && protectedBlock.size() > 0) {
                EntryData protectedVictim = protectedBlock.removeVictim();
                probationBlock.addEntry(protectedVictim);
            }

            EntryData victim = probationBlock.removeVictim();
            items.add(victim);
        }

        capacity -= quantumSize;

        if (protectedBlock.capacity() > 0) {
            protectedBlock.decreaseCapacity(quantumSize);
        } else {
            probationBlock.decreaseCapacity(quantumSize);
        }

        return items;
    }

    private LfuBlock(LfuBlock other) {
        this.quantumSize = other.quantumSize;
        this.capacity = other.capacity;
        this.admittor = new UneditableAdmittorProxy(other.admittor);

        this.probationBlock = other.probationBlock.createGhostCopy("Probation Copy");
        this.protectedBlock = other.protectedBlock.createGhostCopy("Protected Copy");
    }

    @Override
    public PipelineBlock createCopy() {
        return new LfuBlock(this);
    }

    /*
     * The cost of shrink will be (quantumSize / probationSize) * latency as an approximation
     * instead of doing additional part for the "end of cache"
     */
    @Nullable
    @Override
    public EntryData getEntry(long key) {
        final boolean isInProtected = protectedBlock.isHit(key);
        final boolean isInProbation = probationBlock.isHit(key);

        Assert.assertCondition((isInProtected ^ isInProbation) || (!isInProtected && !isInProbation),
                               () -> String.format("Found key %d in multiple parts: %b, %b", key, isInProtected, isInProbation));

        EntryData entry = null;
        if (isInProbation) {
            entry = probationBlock.get(key);
            promoteToProtected(entry);
        } else if (isInProtected) {
            entry = protectedBlock.get(key);
            protectedBlock.moveToTail(key);
        }

        return entry;
    }

    private void promoteToProtected(EntryData entry) {
        probationBlock.remove(entry.key());
        protectedBlock.addEntry(entry);

        if (protectedBlock.size() >= protectedBlock.capacity()) {
            EntryData demote = protectedBlock.findVictim();
            protectedBlock.remove(demote.key());
            probationBlock.addEntry(demote);
        }
    }

    @Nullable
    @Override
    public EntryData insert(EntryData data) {
        EntryData evicted = null;
        final int sizeBefore = size();

        if (capacity > 0) {
            if (probationBlock.size() + protectedBlock.size() >= capacity) {
                EntryData victim = probationBlock.findVictim();
                boolean shouldAdmit = admittor.admit(data.key(), victim.key());

                if (shouldAdmit) {
                    evicted = probationBlock.removeVictim();
                    probationBlock.addEntry(data);
                } else {
                    evicted = data;
                }
            } else {
                probationBlock.addEntry(data);
            }
        }

        admittor.record(data.key());

        Assert.assertCondition(protectedBlock.size() <= protectedBlock.capacity()
                               && probationBlock.size() + protectedBlock.size() <= this.capacity(),
                               "LFU: Size overflow");

        Assert.assertCondition(sizeBefore < capacity() || capacity() == 0 || evicted != null, "Got no evicted item when the cache is full");
        return evicted;
    }

    @Override
    public EntryData getVictim() {
        return probationBlock.findVictim();
    }

    @Override
    public int size() {
        return probationBlock.size() + protectedBlock.size();
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public void validate() {
        final int size = size();
        Assert.assertCondition(size <= capacity,
                               () -> String.format("Size overflow: size: %d, capacity: %d", size, capacity));
        Assert.assertCondition(capacity == probationBlock.capacity() + protectedBlock.capacity(),
                               () -> String.format("Capacity mismatch. Total: %d, probation: %d, protected: %d", capacity, probationBlock.capacity(), protectedBlock.capacity()));
    }

    private static class LfuBlockSettings extends BasicSettings {
        public LfuBlockSettings(Config config) {
            super(config);
        }

        public double decayFactor() { return config().getDouble("decay-factor"); }

        public int maxLists() { return config().getInt("max-lists"); }
    }
}
