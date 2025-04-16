package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.objects.ObjectObjectImmutablePair;

import javax.annotation.Nullable;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.function.LongSupplier;

@Policy.PolicySpec(name = "latency-aware.SampledHillClimber")
public class SampledHillClimber implements Policy {
    private final static boolean DUMP = true;
    @Nullable private PrintWriter quotaDump = null;

    private final PipelinePolicy mainPipeline;
    private LongSampler sampler;

    private final PipelinePolicy sampledMainCache;
    private final List<Pair<PipelinePolicy, CacheDiff>> ghostCaches;
    private LongSampler alternativeSampler;
    private final PipelinePolicy alternativeSampledMain;
    private final LongSupplier seedSupplier;
    private final double samplingErrorTolerance;
    private final int adaptionHoldDuration;
    private int timeframesHoldingAdaption = 0;
    private final int samplingTestDuration;
    private int remainingIntervalsBeforeSamplingCheck;
    private int numOfAltBetterSampling = 0;

    private final PolicyStats stats;
    private final int blockCount;
    private final int adaptionTimeframe;
    private int opsSinceAdaption = 0;
    private final int sampleOrder;
    private final double samplingChangeThreshold;

    public SampledHillClimber(Config config) {
        var settings = new SampledHillClimberSettings(config);
        sampleOrder = settings.sampleOrderFactor();
        mainPipeline = new PipelinePolicy(config);
        blockCount = mainPipeline.blockCount();
        stats = new PolicyStats("Sampled " + sampleOrder + " " + mainPipeline.generatePipelineName());
        adaptionTimeframe = settings.adaptionMultiplier() * mainPipeline.cacheCapacity();

        sampler = new XXH3Sampler(sampleOrder, settings.randomSeed());
        sampledMainCache = new PipelinePolicy(config, sampleOrder);

        alternativeSampledMain = new PipelinePolicy(config, sampleOrder);
        Random seedRandom = new Random(settings.randomSeed());
        seedSupplier = seedRandom::nextLong;
        alternativeSampler = new XXH3Sampler(sampleOrder, seedSupplier.getAsLong());
        samplingErrorTolerance = settings.getSamplingErrorTolerance();
        adaptionHoldDuration = settings.getAdaptHoldDuration();
        samplingTestDuration = settings.getSamplingTestDuration();
        remainingIntervalsBeforeSamplingCheck = samplingTestDuration;
        samplingChangeThreshold = settings.getSamplingChangeThreshold();

        final int numOfCaches = blockCount * (blockCount - 1);
        ghostCaches = new ArrayList<>(numOfCaches);

        createGhostCaches();

        if (DUMP) {
            prepareQuotaDump();
        }
    }

    private void createGhostCaches() {
        for (var pair : ghostCaches) {
            pair.first().clear();
        }

        ghostCaches.clear();
        int idx = 0;
        for (int inc = 0; inc < blockCount; ++inc) {
            for (int dec = 0; dec < blockCount; ++dec) {
                if (inc != dec) {
                    var diff = new CacheDiff(inc, dec);
                    PipelinePolicy cache = PipelinePolicy.DUMMY;
                    if (sampledMainCache.canExtend(inc) && sampledMainCache.canShrink(dec)) {
                        cache = sampledMainCache.createCopy();
                        cache.moveQuantum(inc, dec);
                    }
                    ghostCaches.add(idx, new ObjectObjectImmutablePair<>(cache, diff));
                    ++idx;
                }
            }
        }
    }

    private void copyGhostCaches() {
        for (var pair : ghostCaches) {
            PipelinePolicy ghost = pair.first();
            CacheDiff diff = pair.second();

            ghost.clear();

            sampledMainCache.copyInto(ghost);

            if (ghost.canExtend(diff.incIdx) && ghost.canShrink(diff.decIdx)) {
                ghost.moveQuantum(diff.incIdx, diff.decIdx);
            } else {
                ghost.makeDummy();
            }
        }
    }

    @Override
    public void record(AccessEvent event) {
        this.mainPipeline.record(event);
        switch (event.getStatus()) {
            case HIT:
                stats.recordHit();
                stats.recordHitPenalty(event.hitPenalty());
                break;
            case DELAYED_HIT:
                stats.recordDelayedHit();
                stats.recordDelayedHitPenalty(event.delayedHitPenalty());
                break;
            case MISS:
                stats.recordMiss();
                stats.recordMissPenalty(event.missPenalty());
                break;
            default:
                throw new IllegalStateException("No such event status");
        }

        if (sampler.shouldSample(event.key())) {
            sampledMainCache.record(event);
            for (var pair : ghostCaches) {
                var cache = pair.first();
                if (cache != null) {
                    cache.record(event);
                }
            }
        }

        if (alternativeSampler.shouldSample(event.key())) {
            alternativeSampledMain.record(event);
        }

        ++opsSinceAdaption;

        if (opsSinceAdaption >= adaptionTimeframe) {
            if (this.timeframesHoldingAdaption == 0) {
                adapt(event.eventNum());
            } else {
                --this.timeframesHoldingAdaption;
            }

            opsSinceAdaption = 0;
            resetTimeframeStats();
        }
    }

    private void resetTimeframeStats() {
        this.mainPipeline.resetTimeframeStats();
        this.sampledMainCache.resetTimeframeStats();
        this.alternativeSampledMain.resetTimeframeStats();
        for (var pair : ghostCaches) {
            var cache = pair.first();
            cache.resetTimeframeStats();
        }
    }

    private void adapt(int eventNum) {
        final double currentAvg = this.mainPipeline.getTimeframeAveragePenalty();
        int minIdx = getBestPerformingCacheIndex(currentAvg);

        --this.remainingIntervalsBeforeSamplingCheck;
        checkIfAlternativeSamplingBetter(currentAvg);

        if (this.remainingIntervalsBeforeSamplingCheck == 0) {
            changeSamplingIfNeeded();
        }

        if (minIdx >= 0) {
            performQuantumShift(minIdx);
        }

        performDebugAssertionsAndDumps(eventNum, currentAvg);
    }

    private int getBestPerformingCacheIndex(double currentAvg) {
        double minAvg = currentAvg;
        int minIdx = -1;

        for (int idx = 0; idx < this.ghostCaches.size(); ++idx) {
            var currentGhostCache = this.ghostCaches.get(idx).first();
            double currGhostAvg = currentGhostCache.getTimeframeAveragePenalty();
            if (currGhostAvg < minAvg) {
                minAvg = currGhostAvg;
                minIdx = idx;
            }
        }

        return minIdx;
    }

    private void checkIfAlternativeSamplingBetter(double currentAvg) {
        final double sampledError = Math.abs(currentAvg - this.sampledMainCache.getTimeframeAveragePenalty());
        final double altSampledError = Math.abs(currentAvg - this.alternativeSampledMain.getTimeframeAveragePenalty());
        if (DUMP) {
            System.out.printf("Sampled Error: %.2f  Alternate: %.2f%n", sampledError / currentAvg * 100, altSampledError / currentAvg * 100);
        }

        if (sampledError / currentAvg > 0.05 && sampledError > altSampledError * samplingErrorTolerance) {
            ++this.numOfAltBetterSampling;
        }
    }

    private void changeSamplingIfNeeded() {
        double altSamplingWinPercent = (double) this.numOfAltBetterSampling / this.samplingTestDuration;
        if (altSamplingWinPercent >= this.samplingChangeThreshold) {
            this.sampler = this.alternativeSampler;
            if (DUMP) {
                System.out.printf("Performing sample change, %.2f%n", altSamplingWinPercent * 100);
            }
            this.timeframesHoldingAdaption = this.adaptionHoldDuration;
        }

        this.alternativeSampler = new XXH3Sampler(sampleOrder, seedSupplier.getAsLong());
        this.numOfAltBetterSampling = 0;
        this.remainingIntervalsBeforeSamplingCheck = 0;
    }

    private void performQuantumShift(int minIdx) {
        CacheDiff adaption = this.ghostCaches.get(minIdx).right();

        Assert.assertCondition(this.mainPipeline.canExtend(adaption.incIdx) && this.mainPipeline.canShrink(adaption.decIdx),
                               () -> String.format("Illegal adaption performed: increasing %s, decreasing %s",
                                                   this.mainPipeline.getType(adaption.incIdx),
                                                   this.mainPipeline.getType(adaption.decIdx)));

        this.mainPipeline.moveQuantum(adaption.incIdx, adaption.decIdx);
        this.sampledMainCache.moveQuantum(adaption.incIdx, adaption.decIdx);
        this.alternativeSampledMain.moveQuantum(adaption.incIdx, adaption.decIdx);

        copyGhostCaches();
    }

    private void performDebugAssertionsAndDumps(int eventNum, double currentAvg) {
        if (DUMP && quotaDump != null) {
            var currState = this.mainPipeline.getCurrentState();
            var sampledState = this.sampledMainCache.getCurrentState();

            Assert.assertCondition(Arrays.equals(currState.quotas, sampledState.quotas),
                                   () -> String.format("The sampled and Main configuration mismatch: main: %s sampled: %s",
                                                       Arrays.toString(currState.quotas),
                                                       Arrays.toString(sampledState.quotas)));

            quotaDump.println(printFormatState(eventNum, currState.quotas, currentAvg));
            quotaDump.flush();
        }
    }

    private String printFormatState(int eventNum, int[] quotas, double avgPen) {
        StringBuilder sb = new StringBuilder();

        sb.append(eventNum);
        sb.append(',');

        for (int quota : quotas) {
            sb.append(quota);
            sb.append(',');
        }

        sb.append(String.format("%.2f", avgPen));

        return sb.toString();
    }

    @Override
    public PolicyStats stats() {
        return stats;
    }

    private void prepareQuotaDump() {
        String currentDir = System.getProperty("user.dir");
        try {
            FileWriter fwriter = new FileWriter(currentDir + "/SHC-O" + sampleOrder + ".quota-dump", StandardCharsets.UTF_8);
            quotaDump = new PrintWriter(fwriter);
        } catch (IOException e) {
            System.err.println("Error creating the log file handler");
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public void dump() {
        if (DUMP && quotaDump != null) {
            quotaDump.close();
        }
    }

    public static class SampledHillClimberSettings extends BasicSettings {
        final static String BASE_PATH = "sampled-hill-climber";
        public SampledHillClimberSettings(Config config) {
            super(config);
        }

        public int sampleOrderFactor() { return config().getInt(BASE_PATH + ".sample-order-factor"); }

        public int adaptionMultiplier() { return config().getInt(BASE_PATH + ".adaption-multiplier"); }

        public double getSamplingErrorTolerance() {
            return config().getDouble(BASE_PATH + ".sampling-error-tolerance");
        }

        public int getAdaptHoldDuration() {
            return config().getInt(BASE_PATH + ".adaptation-hold-duration");
        }

        public int getSamplingTestDuration() { return config().getInt(BASE_PATH + ".sampling-test-duration"); }

        public double getSamplingChangeThreshold() {
            return config().getDouble(BASE_PATH + ".sampling-change-threshold");
        }
    }

    private static class CacheDiff {
        final public int incIdx;
        final public int decIdx;

        public CacheDiff(int inc, int dec) {
            this.incIdx = inc;
            this.decIdx = dec;
        }
    }
}
