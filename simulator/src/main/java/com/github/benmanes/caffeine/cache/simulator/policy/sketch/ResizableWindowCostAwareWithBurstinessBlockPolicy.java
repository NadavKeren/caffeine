package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.ConsoleColors;
import com.github.benmanes.caffeine.cache.simulator.ResetablePolicyStats;
import com.github.benmanes.caffeine.cache.simulator.UneditableLatencyEstimatorProxy;
import com.github.benmanes.caffeine.cache.simulator.admission.UneditableAdmittorProxy;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.EntryData;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;

import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.List;

import java.lang.System.Logger;

public class ResizableWindowCostAwareWithBurstinessBlockPolicy extends WindowCostAwareWithBurstinessBlockPolicy {
    final private static boolean DEBUG = true;
    final private static Logger logger = System.getLogger(ResizableWindowCostAwareWithBurstinessBlockPolicy.class.getSimpleName());
    private final int quantumSize;
    private final String name;

    public ResizableWindowCostAwareWithBurstinessBlockPolicy(int windowQuota,
                                                             int probationQuota,
                                                             int protectedQuota,
                                                             int burstQuota,
                                                             int quantumSize,
                                                             Config config,
                                                             String name) {
        super(windowQuota * quantumSize,
              probationQuota * quantumSize,
              protectedQuota * quantumSize,
              burstQuota * quantumSize,
              config,
              new ResettableWCWithBBStats(
                      "sketch.ResizeableWindowCAWithBB (W:%d,LFU:%d,B:%d)",
                      windowQuota,
                      probationQuota + protectedQuota,
                      burstQuota));

        this.quantumSize = quantumSize;
        this.name = name;
    }

    private ResizableWindowCostAwareWithBurstinessBlockPolicy(ResizableWindowCostAwareWithBurstinessBlockPolicy other, String name) {
        super(new ResettableWCWithBBStats(
                "sketch.ResizeableWindowCAWithBB COPY (W:%d,LFU:%d,B:%d)",
                other.windowBlock.capacity(),
                other.probationBlock.capacity() + other.protectedBlock.capacity(),
                other.burstBlock.capacity()),
              other.windowBlock.capacity(),
              other.probationBlock.capacity(),
              other.protectedBlock.capacity(),
              other.burstBlock.capacity(),
              new UneditableLatencyEstimatorProxy<>(other.latencyEstimator),
              new UneditableLatencyEstimatorProxy<>(other.burstEstimator),
              new UneditableAdmittorProxy(other.admittor),
              other.windowBlock,
              other.protectedBlock,
              other.probationBlock,
              other.burstBlock,
              name);

        this.quantumSize = other.quantumSize;
        this.name = name;
    }

    protected ResizableWindowCostAwareWithBurstinessBlockPolicy() {
        super();
        this.quantumSize = 10;
        this.name = "Dummy";
    }

    public ResizableWindowCostAwareWithBurstinessBlockPolicy createGhostCopy(String name) {
        return new ResizableWindowCostAwareWithBurstinessBlockPolicy(this, name);
    }

    @Override
    public PolicyStats stats() {
        return super.stats();
    }

    public ResettableWCWithBBStats timeframeStats() {
        return (ResettableWCWithBBStats) super.stats();
    }

    public void changeSizes(BlockType increase, BlockType decrease) {
        if (DEBUG) {
            logger.log(Logger.Level.INFO,
                       ConsoleColors.infoString("%s: Performing change: Inc: %s, Dec: %s",
                                                name,
                                                increase.toString(),
                                                decrease.toString()));
        }

        Assert.assertCondition(increase != decrease, "Should not perform increment and decrement in the same block");

        List<EntryData> items;
        switch (decrease) {
            case LRU:
                items = this.windowBlock.decreaseSize(quantumSize);
                break;
            case LFU:
                final List<EntryData> protectedItems = this.protectedBlock.decreaseSize(quantumSize);
                for (EntryData item : protectedItems) {
                    this.probationBlock.addEntry(item);
                }

                items = new ArrayList<>(quantumSize);
                final int probationSize = this.probationBlock.size();
                for (int i = 0; i < Math.min(quantumSize, probationSize); ++i) {
                    EntryData victim = this.probationBlock.removeVictim();
                    items.add(victim);
                }
                break;
            case BC:
                items = this.burstBlock.decreaseSize(quantumSize);
                break;
            default:
                throw new IllegalStateException("Bad BlockType received");
        }

        switch (increase) {
            case LRU:
                this.windowBlock.increaseSize(quantumSize, items);
                break;
            case LFU:
                for (EntryData item : items) {
                    this.probationBlock.addEntry(item);
                }

                this.protectedBlock.increaseSize(quantumSize, new ArrayList<>());

                break;
            case BC:
                this.burstBlock.increaseSize(quantumSize, items);
                break;
            default:
                throw new IllegalStateException("Bad BlockType received");
        }

        if (DEBUG) {
            logger.log(Logger.Level.DEBUG,
                       ConsoleColors.minorInfoString("%s: Performed change: Inc: %s, Dec: %s;\tMoved %d items",
                                                     name,
                                                     increase.toString(),
                                                     decrease.toString(),
                                                     items.size()));
        }

        validateCapacity();
    }

    private int totalCapacity() {
        return this.probationBlock.capacity()
               + this.protectedBlock.capacity()
               + this.windowBlock.capacity()
               + this.burstBlock.capacity();
    }

    public void validateCapacity() {
        Assert.assertCondition((totalCapacity() == cacheCapacity),
                               () ->  String.format("%s: The sum of capacities != cache capacity: %s vs %s",
                                                    name,
                                                    totalCapacity(),
                                                    cacheCapacity));
    }

    public void validateSize() {
        final int windowSize = windowBlock.size();
        final int probationSize = probationBlock.size();
        final int protectedSize = protectedBlock.size();
        final int burstSize = burstBlock.size();
        final int capacity = totalCapacity();
        Assert.assertCondition(windowSize + probationSize + protectedSize + burstSize <= capacity,
                               () -> String.format("%s: size overflow: Capacity: %d\tWindow: %d, Probation: %d, Protected: %d, Burst: %d",
                                                   name, capacity, windowSize, probationSize, protectedSize, burstSize));


    }

    public void resetTimeframeStats() {
        timeframeStats().resetTimeframeStats();
    }

    public void resetStats() { super.policyStats = new ResettableWCWithBBStats(name); }

    @Override
    public void record(AccessEvent event) {
        super.record(event);
    }

    public boolean canExtendLFU() {
        return (windowBlock.capacity() + burstBlock.capacity() > 0)
               && (probationBlock.capacity() + protectedBlock.size() < cacheCapacity);
    }

    public boolean canShrinkLFU() {
        return protectedBlock.size() > 0;
    }

    public boolean canExtendLRU() {
        return (protectedBlock.capacity() + burstBlock.capacity() > 0)
               && (probationBlock.capacity() + windowBlock.capacity() < cacheCapacity);
    }

    public boolean canShrinkLRU() {
        return windowBlock.capacity() > 0;
    }

    public boolean canExtendBC() {
        return (windowBlock.capacity() + protectedBlock.capacity() + probationBlock.capacity() > 0)
               && (burstBlock.capacity() < cacheCapacity);
    }

    public boolean canShrinkBC() {
        return burstBlock.capacity() > 0;
    }

    @Override
    public boolean isPenaltyAware() {
        return true;
    }

    public boolean isFull() {
        return windowBlock.size() + probationBlock.size() + protectedBlock.size() + burstBlock.size() >= this.cacheCapacity;
    }

    public int lruCapacity() {
        return windowBlock.capacity();
    }

    public int lfuCapacity() {
        return protectedBlock.capacity() + probationBlock.capacity();
    }

    public int bcCapacity() {
        return burstBlock.capacity();
    }

    public enum BlockType {
        LRU("LRU"),
        LFU("LFU"),
        BC("BC"),
        NONE("NONE");

        final private String name;

        private BlockType(String name) {
            this.name = name;
        }


        @Override
        public String toString() {
            return this.name;
        }
    }

    public static class Dummy extends ResizableWindowCostAwareWithBurstinessBlockPolicy {
        public Dummy() {
            super();
        }

        @Override
        public PolicyStats stats() {
            return new ResetablePolicyStats("Dummy");
        }

        @Override
        public ResettableWCWithBBStats timeframeStats() {
            return new ResettableWCWithBBStats("Dummy");
        }

        @Override
        public void resetTimeframeStats() {}

        @Override
        public void record(AccessEvent event) {}

        @Override
        public boolean canExtendLFU() {
            return false;
        }

        @Override
        public boolean canShrinkLFU() {
            return false;
        }

        @Override
        public boolean canExtendLRU() {
            return false;
        }

        @Override
        public boolean canShrinkLRU() {
            return false;
        }

        @Override
        public boolean canExtendBC() {
            return false;
        }

        @Override
        public boolean canShrinkBC() {
            return false;
        }
    }

    public static class ResettableWCWithBBStats extends WindowCAWithBBStats {
        private int timeframeHitCount;
        private int timeframeMissCount;
        private int timeframeDelayedHitCount;

        private double timeframeHitPenalty;
        private double timeframeMissPenalty;
        private double timeframeDelayedHitPenalty;

        public ResettableWCWithBBStats(String format, Object... args) {
            super(format, args);
            resetTimeframeStats();
        }

        public void resetTimeframeStats() {
            this.timeframeHitCount = 0;
            this.timeframeMissCount = 0;
            this.timeframeDelayedHitCount = 0;
            this.timeframeHitPenalty = 0;
            this.timeframeMissPenalty = 0;
            this.timeframeDelayedHitPenalty = 0;
        }

        @Override
        public void recordHit() {
            super.recordHit();
            ++this.timeframeHitCount;
        }

        @Override
        public void recordDelayedHit() {
            super.recordDelayedHit();
            ++this.timeframeDelayedHitCount;
        }

        @Override
        public void recordMiss(double penalty) {
            super.recordMiss();
            ++this.timeframeMissCount;
        }

        @Override
        public void recordHitPenalty(double penalty) {
            super.recordHitPenalty(penalty);
            this.timeframeHitPenalty += penalty;
        }

        @Override
        public void recordMissPenalty(double penalty) {
            super.recordMissPenalty(penalty);
            this.timeframeMissPenalty += penalty;
        }

        @Override
        public void recordDelayedHitPenalty(double penalty) {
            super.recordDelayedHitPenalty(penalty);
            this.timeframeDelayedHitPenalty += penalty;
        }

        public int timeframeOperationNumber() {
            return this.timeframeHitCount + this.timeframeMissCount + this.timeframeDelayedHitCount;
        }

        public double timeframeHitRate() {
            return this.timeframeOperationNumber() > 0
                   ? (double) this.timeframeHitCount / this.timeframeOperationNumber()
                   : 0;
        }

        public double timeframeAveragePenalty() {
            return this.timeframeOperationNumber() > 0
                   ? (this.timeframeHitPenalty + this.timeframeMissPenalty + this.timeframeDelayedHitPenalty)
                     / this.timeframeOperationNumber()
                   : Double.MAX_VALUE;
        }
    }
}
