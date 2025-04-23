package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.objects.ObjectObjectImmutablePair;

import javax.annotation.Nullable;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.function.Supplier;

@Policy.PolicySpec(name = "latency-aware.SampledHillClimber")
public class SampledHillClimber implements Policy {
    private final static boolean DUMP = true;
    @Nullable private PrintWriter quotaDump = null;

    private final PipelinePolicy mainPipeline;
    private final LongSampler sampler;

    private final PipelinePolicy sampledMainCache;
    private final List<Pair<PipelinePolicy, CacheDiff>> ghostCaches;

    private final SampledHillClimberStats stats;
    private final int blockCount;
    private final int adaptionTimeframe;
    private int opsSinceAdaption = 0;
    private final int sampleOrder;

    public SampledHillClimber(Config config) {
        var settings = new SampledHillClimberSettings(config);
        sampleOrder = settings.sampleOrderFactor();
        mainPipeline = new PipelinePolicy(config);
        blockCount = mainPipeline.blockCount();
        stats = new SampledHillClimberStats("Sampled " + sampleOrder + " " + mainPipeline.generatePipelineName());
        adaptionTimeframe = settings.adaptionMultiplier() * mainPipeline.cacheCapacity();

        sampler = new XXH3Sampler(sampleOrder, settings.randomSeed());
        sampledMainCache = new PipelinePolicy(config, sampleOrder);
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

        ++opsSinceAdaption;

        if (opsSinceAdaption >= adaptionTimeframe) {
            opsSinceAdaption = 0;
            adapt(event.eventNum());
        }
    }

    private void adapt(int eventNum) {
        final double currentAvg = this.mainPipeline.getTimeframeAveragePenalty();
        final double currentSampledAvg = this.sampledMainCache.getTimeframeAveragePenalty();
        stats.recordSampledAveragePenalty(currentSampledAvg);
        stats.recordSamplingError(currentAvg - currentSampledAvg);

        double minAvg = currentAvg;
        int minIdx = -1;

        for (int idx = 0; idx < this.ghostCaches.size(); ++idx) {
            double currGhostAvg = this.ghostCaches.get(idx).first().getTimeframeAveragePenalty();
            if (currGhostAvg < minAvg) {
                minAvg = currGhostAvg;
                minIdx = idx;
            }
        }

        if (minIdx >= 0) {
            CacheDiff adaption = this.ghostCaches.get(minIdx).right();

            Assert.assertCondition(this.mainPipeline.canExtend(adaption.incIdx) && this.mainPipeline.canShrink(adaption.decIdx),
                                   () -> String.format("Illegal adaption performed: increasing %s, decreasing %s",
                                                       this.mainPipeline.getType(adaption.incIdx),
                                                       this.mainPipeline.getType(adaption.decIdx)));

            this.mainPipeline.moveQuantum(adaption.incIdx, adaption.decIdx);
            this.sampledMainCache.moveQuantum(adaption.incIdx, adaption.decIdx);
            var currState = this.mainPipeline.getCurrentState();
            var sampledState = this.sampledMainCache.getCurrentState();

            Assert.assertCondition(Arrays.equals(currState.quotas, sampledState.quotas),
                                   () -> String.format("The sampled and Main configuration mismatch: main: %s sampled: %s",
                                                       Arrays.toString(currState.quotas),
                                                       Arrays.toString(sampledState.quotas)));

            if (DUMP && quotaDump != null) {
                quotaDump.println(printFormatState(eventNum, currState.quotas, currentAvg));
                quotaDump.flush();
            }

            copyGhostCaches();
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
    }

    private static class CacheDiff {
        final public int incIdx;
        final public int decIdx;

        public CacheDiff(int inc, int dec) {
            this.incIdx = inc;
            this.decIdx = dec;
        }
    }

    private static class SampledHillClimberStats extends PolicyStats {
        private double sampledPenalty = 0d;
        private int penaltyCount = 0;
        private DoubleList squaredErrors = new DoubleArrayList();

        SampledHillClimberStats(String format, Object... args) {
            super(format, args);
            addMetric(Metric.of("Sampled Average Penalty", (Supplier<Double>) this::sampledAveragePenalty, Metric.MetricType.NUMBER, true));
            addMetric(Metric.of("Sampled Average Penalty (MSE)", (Supplier<Double>) this::meanSquareError, Metric.MetricType.NUMBER, true));
            addMetric(Metric.of("Sampled Average Penalty (Max Squared Error)", (Supplier<Double>) this::maxSquaredError, Metric.MetricType.NUMBER, true));
            addMetric(Metric.of("Sampled Average Penalty (p90 Squared Error)", (Supplier<Double>) this::p90SquaredError, Metric.MetricType.NUMBER, true));
            addMetric(Metric.of("Sampled Average Penalty (p10 Squared Error)", (Supplier<Double>) this::p10SquaredError, Metric.MetricType.NUMBER, true));
        }

        public double sampledAveragePenalty() { return sampledPenalty / penaltyCount; }
        public double meanSquareError() {
            return squaredErrors.stream()
                                .mapToDouble(Double::doubleValue)
                                .average()
                                .orElseThrow(() -> new IllegalArgumentException("Error calculating the average"));
        }

        public void recordSampledAveragePenalty(double sampledAveragePenalty) {
            this.sampledPenalty += sampledAveragePenalty;
            penaltyCount++;
        }

        public double maxSquaredError() {
            return squaredErrors.stream()
                                .mapToDouble(Double::doubleValue)
                                .max()
                                .orElseThrow(() -> new IllegalArgumentException("Error calculating the maximum"));
        }

        private double getSquareErrorPercentile(int percentile) {
            Collections.sort(this.squaredErrors);

            int percentileIndex = percentile * squaredErrors.size() / 100;

            return squaredErrors.getDouble(percentileIndex);
        }

        public double p90SquaredError() {
            return getSquareErrorPercentile(90);
        }

        public double p10SquaredError() {
            return getSquareErrorPercentile(10);
        }

        public void recordSamplingError(double samplingError) {
            this.squaredErrors.add(samplingError * samplingError);
        }
    }
}
