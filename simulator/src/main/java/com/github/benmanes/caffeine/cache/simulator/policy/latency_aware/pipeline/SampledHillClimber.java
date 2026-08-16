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

@SuppressWarnings("NullAway")
@Policy.PolicySpec(name = "latency-aware.SampledHillClimber")
public class SampledHillClimber implements Policy {
    private final static boolean DUMP_STATES = true;
    private final static boolean DUMP_RESULTS = true;
    @Nullable private PrintWriter quotaDump = null;
    @Nullable private PrintWriter resultsDump = null;

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

        if (DUMP_STATES) {
            quotaDump = prepareDump("quota_dump");
            quotaDump.println(buildHeaderRow());
            quotaDump.println(printFormatState(0, this.mainPipeline.getCurrentState().quotas, emptySnapshot(), emptySnapshots()));
        }
        if (DUMP_RESULTS) {
          resultsDump = prepareDump("results_dump");
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

        if (resultsDump != null) {
            int res = event.getStatus() == AccessEvent.EventStatus.MISS ? 0 : 1;
            resultsDump.println(event.getRequestTime()
                                + " " + event.key()
                                + " " + event.missPenalty()
                                + " " + res);
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

        GhostStatsSnapshot mainSnapshot = new GhostStatsSnapshot(currentAvg,
                                                                  this.mainPipeline.getTimeframeUniqueCount(),
                                                                  this.mainPipeline.getTimeframeHitCount(),
                                                                  this.mainPipeline.getTimeframeEvictionCount(),
                                                                  this.mainPipeline.getTimeframeSavedLatency(),
                                                                  this.mainPipeline.getTimeframeHitsPerBlock(),
                                                                  this.mainPipeline.getTimeframeEvictionsPerBlock(),
                                                                  this.mainPipeline.getTimeframeSavedLatencyPerBlock());

        this.mainPipeline.resetTimeframeStats();

        double minAvg = currentAvg;
        int minIdx = -1;

        GhostStatsSnapshot[] snapshots = new GhostStatsSnapshot[this.ghostCaches.size()];
        for (int idx = 0; idx < this.ghostCaches.size(); ++idx) {
            var currGhostCache = this.ghostCaches.get(idx).first();
            double currGhostAvg = currGhostCache.getTimeframeAveragePenalty();
            if (currGhostAvg < minAvg) {
                minAvg = currGhostAvg;
                minIdx = idx;
            }

            boolean isValid = currGhostAvg < Double.MAX_VALUE;
            snapshots[idx] = isValid
                ? new GhostStatsSnapshot(currGhostAvg,
                                         currGhostCache.getTimeframeUniqueCount(),
                                         currGhostCache.getTimeframeHitCount(),
                                         currGhostCache.getTimeframeEvictionCount(),
                                         currGhostCache.getTimeframeSavedLatency(),
                                         currGhostCache.getTimeframeHitsPerBlock(),
                                         currGhostCache.getTimeframeEvictionsPerBlock(),
                                         currGhostCache.getTimeframeSavedLatencyPerBlock())
                : GhostStatsSnapshot.na();

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

        if (DUMP_STATES && quotaDump != null) {
            quotaDump.println(printFormatState(eventNum, this.mainPipeline.getCurrentState().quotas, mainSnapshot, snapshots));
            quotaDump.flush();
        }

    }

    private String buildHeaderRow() {
        StringBuilder sb = new StringBuilder();

        sb.append("event_num");

        for (int idx = 0; idx < blockCount; ++idx) {
            sb.append(",quota_").append(idx);
        }

        appendSnapshotHeader(sb, "main");

        for (var pair : ghostCaches) {
            CacheDiff diff = pair.right();
            String prefix = "ghost_i" + diff.incIdx + "_d" + diff.decIdx;
            appendSnapshotHeader(sb, prefix);
        }

        return sb.toString();
    }

    private void appendSnapshotHeader(StringBuilder sb, String prefix) {
        sb.append(',').append(prefix).append("_avg_penalty");
        sb.append(',').append(prefix).append("_unique_requests");
        sb.append(',').append(prefix).append("_hits");
        sb.append(',').append(prefix).append("_evictions");
        sb.append(',').append(prefix).append("_saved_latency");

        for (int b = 0; b < blockCount; ++b) {
            sb.append(',').append(prefix).append("_hits_block").append(b);
        }
        for (int b = 0; b < blockCount; ++b) {
            sb.append(',').append(prefix).append("_evictions_block").append(b);
        }
        for (int b = 0; b < blockCount; ++b) {
            sb.append(',').append(prefix).append("_saved_latency_block").append(b);
        }
    }

    private GhostStatsSnapshot emptySnapshot() {
        return new GhostStatsSnapshot(0, 0, 0, 0, 0, new int[blockCount], new int[blockCount], new double[blockCount]);
    }

    private GhostStatsSnapshot[] emptySnapshots() {
        GhostStatsSnapshot[] snapshots = new GhostStatsSnapshot[ghostCaches.size()];
        for (int idx = 0; idx < snapshots.length; ++idx) {
            snapshots[idx] = emptySnapshot();
        }
        return snapshots;
    }

    private String printFormatState(int eventNum, int[] quotas, GhostStatsSnapshot mainSnapshot, GhostStatsSnapshot[] snapshots) {
        StringBuilder sb = new StringBuilder();

        sb.append(eventNum);

        for (int quota : quotas) {
            sb.append(',');
            sb.append(quota);
        }

        appendSnapshot(sb, mainSnapshot);

        for (GhostStatsSnapshot snapshot : snapshots) {
            appendSnapshot(sb, snapshot);
        }

        return sb.toString();
    }

    private void appendSnapshot(StringBuilder sb, GhostStatsSnapshot snapshot) {
        sb.append(',');
        if (snapshot.avgPenalty >= 0) {
            sb.append(String.format("%.2f", snapshot.avgPenalty));
        } else {
            sb.append("NA");
        }

        sb.append(',').append(snapshot.uniqueRequests >= 0 ? String.valueOf(snapshot.uniqueRequests) : "NA");
        sb.append(',').append(snapshot.hits >= 0 ? String.valueOf(snapshot.hits) : "NA");
        sb.append(',').append(snapshot.evictions >= 0 ? String.valueOf(snapshot.evictions) : "NA");
        sb.append(',').append(snapshot.savedLatency >= 0 ? String.format("%.2f", snapshot.savedLatency) : "NA");

        for (int b = 0; b < blockCount; ++b) {
            sb.append(',');
            sb.append(snapshot.hitsPerBlock != null ? String.valueOf(snapshot.hitsPerBlock[b]) : "NA");
        }
        for (int b = 0; b < blockCount; ++b) {
            sb.append(',');
            sb.append(snapshot.evictionsPerBlock != null ? String.valueOf(snapshot.evictionsPerBlock[b]) : "NA");
        }
        for (int b = 0; b < blockCount; ++b) {
            sb.append(',');
            sb.append(snapshot.savedLatencyPerBlock != null ? String.format("%.2f", snapshot.savedLatencyPerBlock[b]) : "NA");
        }
    }

    @Override
    public PolicyStats stats() {
        return stats;
    }

    private PrintWriter prepareDump(String suffix) {
        String currentDir = System.getProperty("user.dir");
        PrintWriter dump = null;
        try {
            FileWriter fwriter = new FileWriter(currentDir + "/SHC-O" + sampleOrder + "." + suffix, StandardCharsets.UTF_8);
            dump = new PrintWriter(fwriter);
        } catch (IOException e) {
            System.err.println("Error creating the log file handler");
            //noinspection CallToPrintStackTrace
            e.printStackTrace();
            System.exit(1);
        }

        return dump;
    }

    @Override
    public void dump() {
        if (DUMP_STATES && quotaDump != null) {
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

    @SuppressWarnings("this-escape")
    public static class SHCStats extends PolicyStats {
        private long sampledRequests = 0;
        public SHCStats(String format, int sampleOrder, Object... args) {
            super(format, args);
            addMetric(new Metric.Builder().name("Sampling Percentage")
                                          .value(this::samplingPercentage)
                                          .type(Metric.MetricType.PERCENT)
                                          .required(true));
            addMetric(new Metric.Builder().name("Sample Order")
                                          .value(sampleOrder)
                                          .type(Metric.MetricType.NUMBER)
                                          .required(true));
        }

        public void recordSampledRequest() {
            ++sampledRequests;
        }

        public double samplingPercentage() {
            return this.requestCount() > 0 ? (double) sampledRequests / this.requestCount() : 1d;
        }
    }

    private static class GhostStatsSnapshot {
        final double avgPenalty;
        final int uniqueRequests;
        final int hits;
        final int evictions;
        final double savedLatency;
        @Nullable final int[] hitsPerBlock;
        @Nullable final int[] evictionsPerBlock;
        @Nullable final double[] savedLatencyPerBlock;

        GhostStatsSnapshot(double avgPenalty, int uniqueRequests, int hits, int evictions, double savedLatency,
                           @Nullable int[] hitsPerBlock, @Nullable int[] evictionsPerBlock,
                           @Nullable double[] savedLatencyPerBlock) {
            this.avgPenalty = avgPenalty;
            this.uniqueRequests = uniqueRequests;
            this.hits = hits;
            this.evictions = evictions;
            this.savedLatency = savedLatency;
            this.hitsPerBlock = hitsPerBlock;
            this.evictionsPerBlock = evictionsPerBlock;
            this.savedLatencyPerBlock = savedLatencyPerBlock;
        }

        static GhostStatsSnapshot na() {
            return new GhostStatsSnapshot(-1, -1, -1, -1, -1, null, null, null);
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
