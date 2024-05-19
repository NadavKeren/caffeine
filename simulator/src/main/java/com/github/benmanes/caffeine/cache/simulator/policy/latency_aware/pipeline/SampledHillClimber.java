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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

@Policy.PolicySpec(name = "latency-aware.SampledHillClimber")
public class SampledHillClimber implements Policy {
    private final static boolean DEBUG = true;
    @Nullable private PrintWriter writer = null;

    private final PipelinePolicy mainPipeline;
    private final LongSampler sampler;

    private final PipelinePolicy sampledMainCache;
    private final List<Pair<PipelinePolicy, CacheDiff>> ghostCaches;

    private final PolicyStats stats;
    private final int blockCount;
    private final int adaptionTimeframe;
    private int sampledOpsSinceAdaption = 0;
    private final int sampleOrder;

    public SampledHillClimber(Config config) {
        var settings = new SampledHillClimberSettings(config);
        sampleOrder = settings.sampleOrderFactor();
        mainPipeline = new PipelinePolicy(config);
        blockCount = mainPipeline.blockCount();
        stats = new PolicyStats("Sampled " + sampleOrder + " " + mainPipeline.generatePipelineName());
        adaptionTimeframe = settings.adaptionMultiplier() * mainPipeline.cacheCapacity();

        sampler = new FarmHashSampler(sampleOrder);
        sampledMainCache = new PipelinePolicy(config, sampleOrder);
        final int numOfCaches = blockCount * (blockCount - 1);
        ghostCaches = new ArrayList<>(numOfCaches);

        createGhostCaches();

        if (DEBUG) {
            writer = prepareFileWriter();
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
            sampledMainCache.record(event);
            for (var pair : ghostCaches) {
                var cache = pair.first();
                if (cache != null) {
                    cache.record(event);
                }
            }

            ++sampledOpsSinceAdaption;
        }

        if (sampledOpsSinceAdaption >= adaptionTimeframe) {
            sampledOpsSinceAdaption = 0;
            adapt(event.eventNum());
        }
    }

    private void adapt(int eventNum) {
        final double currentAvg = this.sampledMainCache.getTimeframeAveragePenalty();
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

            if (DEBUG && writer != null) {
                writer.println(printFormatState(eventNum, currState.quotas, currentAvg));
                writer.flush();
            }

            createGhostCaches();
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

    private PrintWriter prepareFileWriter() {
        LocalDateTime currentTime = LocalDateTime.now(ZoneId.systemDefault());
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("dd-MM-HH-mm-ss");
        PrintWriter writer = null;
        try {
            FileWriter fwriter = new FileWriter("/tmp/SHC-O" + sampleOrder + "-T-" + currentTime.format(timeFormatter) + ".dump", StandardCharsets.UTF_8);
            writer = new PrintWriter(fwriter);
        } catch (IOException e) {
            System.err.println("Error creating the log file handler");
            e.printStackTrace();
            System.exit(1);
        }

        return writer;
    }

    @Override
    public void dump() {
        if (DEBUG && writer != null) {
            writer.close();
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
}
