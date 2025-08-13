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
import java.util.function.DoubleSupplier;

@Policy.PolicySpec(name = "latency-aware.SampledHillClimber")
public class SampledHillClimber implements Policy {
    private final static boolean DUMP = true;
    @Nullable private PrintWriter quotaDump = null;

    private final PipelinePolicy mainPipeline;
    private final LongSampler sampler;

    private final PipelinePolicy sampledMainCache;
    private final List<Pair<PipelinePolicy, CacheDiff>> ghostCaches;

    private final SHCStats stats;
    private final int blockCount;
    private final int adaptionTimeframe;
    private int opsSinceAdaption = 0;
    private final int sampleOrder;

    public SampledHillClimber(Config config) {
        var settings = new SampledHillClimberSettings(config);
        sampleOrder = settings.sampleOrderFactor();
        mainPipeline = new PipelinePolicy(config);
        blockCount = mainPipeline.blockCount();
        stats = new SHCStats("Sampled " + sampleOrder + " " + mainPipeline.generatePipelineName(), sampleOrder);
        adaptionTimeframe = settings.adaptionMultiplier() * mainPipeline.cacheCapacity();

        sampler = new XXH3Sampler(sampleOrder, settings.randomSeed());
        sampledMainCache = new PipelinePolicy(config, sampleOrder);
        final int numOfCaches = blockCount * (blockCount - 1);
        ghostCaches = new ArrayList<>(numOfCaches);

        createGhostCaches();

        if (DUMP) {
            prepareQuotaDump();
            quotaDump.println(printFormatState(0, this.mainPipeline.getCurrentState().quotas, 0, new double[ghostCaches.size()]));
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
            stats.recordSampledRequest();
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

        this.mainPipeline.resetTimeframeStats();

        double minAvg = currentAvg;
        int minIdx = -1;

        double[] timeframeResults = new double[this.ghostCaches.size()];
        for (int idx = 0; idx < this.ghostCaches.size(); ++idx) {
            var currGhostCache = this.ghostCaches.get(idx).first();
            double currGhostAvg = currGhostCache.getTimeframeAveragePenalty();
            if (currGhostAvg < minAvg) {
                minAvg = currGhostAvg;
                minIdx = idx;
            }
            timeframeResults[idx] = currGhostAvg < Double.MAX_VALUE ? currGhostAvg : -1;
            currGhostCache.resetTimeframeStats();
        }

        if (minIdx >= 0) {
            CacheDiff adaption = this.ghostCaches.get(minIdx).right();

            Assert.assertCondition(this.mainPipeline.canExtend(adaption.incIdx) && this.mainPipeline.canShrink(adaption.decIdx),
                                   () -> String.format("Illegal adaption performed: increasing %s, decreasing %s",
                                                       this.mainPipeline.getType(adaption.incIdx),
                                                       this.mainPipeline.getType(adaption.decIdx)));

            this.mainPipeline.moveQuantum(adaption.incIdx, adaption.decIdx);
            this.sampledMainCache.moveQuantum(adaption.incIdx, adaption.decIdx);
            final var currState = this.mainPipeline.getCurrentState();
            final var sampledState = this.sampledMainCache.getCurrentState();

            Assert.assertCondition(Arrays.equals(currState.quotas, sampledState.quotas),
                                   () -> String.format("The sampled and Main configuration mismatch: main: %s sampled: %s",
                                                       Arrays.toString(currState.quotas),
                                                       Arrays.toString(sampledState.quotas)));


            createGhostCaches();
        }

        if (DUMP && quotaDump != null) {
            quotaDump.println(printFormatState(eventNum, this.mainPipeline.getCurrentState().quotas, currentAvg, timeframeResults));
            quotaDump.flush();
        }

    }

    private String printFormatState(int eventNum, int[] quotas, double avgPen, double[] ghostResults) {
        StringBuilder sb = new StringBuilder();

        sb.append(eventNum);
        sb.append(',');

        for (int quota : quotas) {
            sb.append(quota);
            sb.append(',');
        }

        sb.append(String.format("%.2f", avgPen));
        for (double res : ghostResults) {
            sb.append(',');
            if (res >= 0) {
                sb.append(String.format("%.2f", res));
            } else {
                sb.append("NA");
            }
        }

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

    public static class SHCStats extends PolicyStats {
        private long sampledRequests = 0;
        public SHCStats(String format, int sampleOrder, Object... args) {
            super(format, args);
            addMetric(Metric.of("Sampling Percentage", (DoubleSupplier) this::samplingPercentage, Metric.MetricType.PERCENT, true));
            addMetric(Metric.of("Sample Order", sampleOrder, Metric.MetricType.NUMBER, true));
        }

        public void recordSampledRequest() {
            ++sampledRequests;
        }

        public double samplingPercentage() {
            return this.requestCount() > 0 ? (double) sampledRequests / this.requestCount() : 1d;
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
