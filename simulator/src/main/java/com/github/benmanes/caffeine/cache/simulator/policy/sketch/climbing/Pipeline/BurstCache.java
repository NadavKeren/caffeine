package com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing.Pipeline;

import com.github.benmanes.caffeine.cache.simulator.policy.EntryData;
import com.github.benmanes.caffeine.cache.simulator.policy.LatencyEstimator;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.BurstBlock;

import javax.annotation.Nullable;
import java.util.List;

public class BurstCache implements PipelineBlock {
    final private int quantumSize;
    final private BurstBlock block;
    final private BurstBlock ghostBlock;

    private double expansionBenefit = 0;
    private double shrinkCost = 0;

    public BurstCache(LatencyEstimator<Long> burstEstimator, int quantumSize, int initialQuota) {
        this.quantumSize = quantumSize;

        block = new BurstBlock(initialQuota * quantumSize, burstEstimator);
        ghostBlock = new BurstBlock(quantumSize, burstEstimator);
    }

    @Override
    public void increaseSize(List<EntryData> items) {
        block.increaseSize(quantumSize, items);

        for (EntryData item: items) {
            if (ghostBlock.isHit(item.key())) {
                ghostBlock.remove(item.key());
            }
        }
    }

    @Override
    public List<EntryData> decreaseSize() {
        List<EntryData> items = block.decreaseSize(quantumSize);

        for (EntryData item: items) {
            addToGhostIfBetter(item);
        }

        return items;
    }

    private void addToGhostIfBetter(EntryData item) {
        if (!ghostBlock.isHit(item.key())) {
            if (!ghostBlock.isFull()) {
                ghostBlock.admit(item);
            } else if (ghostBlock.compareToVictim(item) > 0) {
                ghostBlock.removeVictim();
                ghostBlock.admit(item);
            }
        }
    }

    private @Nullable EntryData addToCacheIfBetter(EntryData item) {
        EntryData evicted = null;

        if (ghostBlock.isHit(item.key())) {
            ghostBlock.remove(item.key());
        }

        if (!block.isFull()) {
            block.admit(item);
        } else if (block.compareToVictim(item) > 0) {
            evicted = block.removeVictim();
            block.admit(item);
            addToGhostIfBetter(evicted);
        }

        return evicted;
    }

    @Override
    public EntryData getEntry(long key) {
        EntryData res = this.block.get(key);

        if (res != null) {
            if (this.block.getIndex(key) < quantumSize) {
                shrinkCost += res.event().missPenalty();
            }
        } else {
            EntryData ghostEntry = this.ghostBlock.get(key);
            if (ghostEntry != null){
                expansionBenefit += ghostEntry.event().missPenalty();
            }
        }

        return res;
    }

    @Override
    public @Nullable EntryData insert(EntryData data) {
        EntryData res = null;
        if (this.block.capacity() > 0) {
            res = addToCacheIfBetter(data);
        } else {
            addToGhostIfBetter(data);
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
        ghostBlock.validate();
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
}
