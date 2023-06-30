package com.github.benmanes.caffeine.cache.simulator;

import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;

public class ResetablePolicyStats extends PolicyStats {

    private int timeframeHitCount;
    private int timeframeMissCount;
    private double timeframeHitPenalty;
    private double timeframeMissPenalty;

    public ResetablePolicyStats(String format, Object... args) {
        super(format, args);
    }

    public void resetStats() {
        this.timeframeHitCount = 0;
        this.timeframeMissCount = 0;
        this.timeframeHitPenalty = 0;
        this.timeframeMissPenalty = 0;
    }

    @Override
    public void recordHit() {
        super.recordHit();
        ++this.timeframeHitCount;
    }

    @Override
    public void recordMiss() {
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

    public double timeframeHitRate() {
        return this.timeframeOperationNumber() > 0 ? (double) this.timeframeHitCount
                                                     / this.timeframeOperationNumber() : 0;
    }

    public int timeframeOperationNumber() {
        return this.timeframeHitCount + this.timeframeMissCount;
    }

    public double timeframeHitPenalty() {
        return this.timeframeOperationNumber() > 0 ? this.timeframeHitPenalty : Double.MAX_VALUE;
    }

    public double timeframeMissPenalty() {
        return this.timeframeOperationNumber() > 0 ? this.timeframeMissPenalty : Double.MAX_VALUE;
    }
}
