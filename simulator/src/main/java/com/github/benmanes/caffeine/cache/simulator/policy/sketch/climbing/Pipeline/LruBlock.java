package com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing.Pipeline;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.policy.EntryData;
import com.github.benmanes.caffeine.cache.simulator.policy.LatencyEstimator;
import com.github.benmanes.caffeine.cache.simulator.policy.linked.CraBlock;
import com.typesafe.config.Config;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class LruBlock implements PipelineBlock {
    final private int quantumSize;
    final private CraBlock block;
    final private CraBlock endBlock;
    final private CraBlock ghostBlock;

    private double expansionBenefit = 0;
    private double shrinkCost = 0;

    public LruBlock(Config config, LatencyEstimator<Long> latencyEstimator, int quantumSize, int initialQuota, int ghostSize) {
        this.quantumSize = quantumSize;

        var settings = new LruBlockSettings(config);
        int maxLists = settings.maxLists();
        double decayFactor = settings.decayFactor();

        int sizeBlock = 0;
        int sizeEndBlock = 0;
        if (initialQuota >= 1) {
            sizeBlock = (initialQuota - 1) * quantumSize;
            sizeEndBlock = quantumSize;
        }

        this.block = new CraBlock(decayFactor, maxLists, sizeBlock, latencyEstimator, "LRU-Block");
        this.endBlock = new CraBlock(decayFactor, maxLists, sizeEndBlock, latencyEstimator, "LRU-End-of-Block");
        this.ghostBlock = new CraBlock(decayFactor, maxLists, ghostSize * quantumSize, latencyEstimator, "LRU-Ghost");
    }

    @Override
    public void increaseSize(List<EntryData> items) {
        if (endBlock.capacity() > 0) {
            block.increaseSize(quantumSize, items);
        } else {
            endBlock.increaseSize(quantumSize, items);
        }

        for (EntryData item: items) {
            if (ghostBlock.isHit(item.key())) {
                ghostBlock.remove(item.key());
            }
        }
    }

    @Override
    public List<EntryData> decreaseSize() {
        Assert.assertCondition(block.capacity() + endBlock.capacity() > 0, "Decreasing from empty block");

        List<EntryData> items = new ArrayList<>(quantumSize);
        if (block.capacity() > 0) {
            List<EntryData> blockItems = block.decreaseSize(quantumSize);

            for (EntryData item : blockItems) {
                EntryData victim = endBlock.removeVictim();
                endBlock.addEntry(item);
                items.add(victim);
            }
        } else {
            items = endBlock.decreaseSize(quantumSize);
        }


        for (EntryData item : items) {
            ghostBlock.removeVictim();
            ghostBlock.addEntry(item);
        }

        return items;
    }

    @Nullable
    @Override
    public EntryData getEntry(long key) {
//        final boolean isInBlock = block.isHit(key);
//        final boolean isInEnd = !isInBlock && endBlock.isHit(key);
//        final boolean isInGhost = !isInBlock && !isInEnd && ghostBlock.isHit(key);
        // This is for debugging only:

        final boolean isInBlock = block.isHit(key);
        final boolean isInEnd = endBlock.isHit(key);
        final boolean isInGhost = ghostBlock.isHit(key);

        Assert.assertCondition((isInBlock ^ isInEnd ^ isInGhost) || (!isInBlock && !isInEnd && !isInGhost),
                () -> String.format("Found key %d in multiple parts: %b, %b, %b", key, isInBlock, isInEnd, isInGhost));

        EntryData entry = null;

        if (isInBlock) {
            entry = block.get(key);
            block.moveToTail(key);
        } else if (isInEnd) {
            entry = endBlock.get(key);
            endBlock.moveToTail(key);
            shrinkCost += entry.event().missPenalty();
        } else if (isInGhost) {
            EntryData ghostEntry = ghostBlock.get(key);
            ghostBlock.moveToTail(key);
            expansionBenefit += ghostEntry.event().missPenalty();
        }

        return entry;
    }

    @Nullable
    @Override
    public EntryData insert(EntryData data) {
        EntryData victim = null;

        if (ghostBlock.isHit(data.key())) {
            ghostBlock.remove(data.key());
        }

        if (endBlock.size() < endBlock.capacity()) {
            Assert.assertCondition(block.size() == 0, "Should fill the end-of-block before the block itself");
            endBlock.addEntry(data);
        } else {
            block.addEntry(data);

            if (block.size() > block.capacity()) {
                victim = block.removeVictim();
                endBlock.addEntry(victim);

                victim = endBlock.removeVictim();
                ghostBlock.addEntry(victim);

                if (ghostBlock.isFull()) {
                    ghostBlock.removeVictim();
                }
            }
        }
        Assert.assertCondition(endBlock.size() <= endBlock.capacity()
                               && block.size() <= block.capacity()
                               && ghostBlock.size() <= ghostBlock.capacity(),
                               "LRU: Size overflow");

        return victim;
    }

    @Override
    public EntryData getVictim() {
        return endBlock.findVictim();
    }

    @Override
    public int size() {
        return block.size() + endBlock.size();
    }

    @Override
    public int capacity() {
        return block.capacity() + endBlock.capacity();
    }

    @Override
    public void validate() {
        Assert.assertCondition(endBlock.capacity() == 0 || endBlock.capacity() == quantumSize,
                () -> String.format("Illegal end-of-block capacity: %d", endBlock.capacity()));

        Assert.assertCondition(block.capacity() == 0 || endBlock.capacity() == quantumSize,
                () -> String.format("The block should have 0 capacity if end block is empty. Block capacity: %d, end-of-block capacity: %d",
                        block.capacity(),
                        endBlock.capacity()));

//        Assert.assertCondition(ghostBlock.capacity() == 2 * quantumSize,
//                () -> String.format("Illegal ghost block capacity: %d", ghostBlock.capacity()));

        Assert.assertCondition(endBlock.size() <= endBlock.capacity(),
                () -> String.format("End-of-block overflow: size: %d, capacity: %d", endBlock.size(), endBlock.capacity()));

        Assert.assertCondition(block.size() <= block.capacity(),
                () -> String.format("Block overflow: size: %d, capacity: %d", block.size(), block.capacity()));

        Assert.assertCondition(ghostBlock.size() <= ghostBlock.capacity(),
                () -> String.format("Ghost Block overflow: size: %d, capacity: %d", ghostBlock.size(), ghostBlock.capacity()));
    }

    @Override
    public boolean isGhostFull() {
        return ghostBlock.size() >= ghostBlock.capacity();
    }

    @Override
    public double getExpansionBenefit() {
        return this.expansionBenefit;
    }

    @Override
    public double getShrinkCost() {
        return this.shrinkCost;
    }

    @Override
    public void resetStats() {
        this.expansionBenefit = 0;
        this.shrinkCost = 0;
    }

    private static class LruBlockSettings extends BasicSettings {
        public LruBlockSettings(Config config) {
            super(config);
        }

        public double decayFactor() { return config().getDouble("decay-factor"); }

        public int maxLists() { return config().getInt("max-lists"); }
    }
}
