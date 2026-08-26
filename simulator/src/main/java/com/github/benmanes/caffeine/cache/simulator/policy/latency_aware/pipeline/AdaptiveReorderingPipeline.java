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
@Policy.PolicySpec(name = "latency-aware.AdaptiveReorderingPipeline")
public class AdaptiveReorderingPipeline implements Policy {
    private final static boolean DUMP_STATES = true;
    private final static boolean DUMP_RESULTS = true;
    @Nullable private PrintWriter orderDump = null;
    @Nullable private PrintWriter resultsDump = null;

    private final PipelinePolicy mainPipeline;
    private final LongSampler sampler;

    private final PipelinePolicy sampledMainCache;
    private final List<Pair<PipelinePolicy, BlockType[]>> ghostCaches;
    private final List<BlockType[]> candidateOrderings;

    private final ARPStats stats;
    private final int adaptionTimeframe;
    private int opsSinceAdaption = 0;
    private final int sampleOrder;

    public AdaptiveReorderingPipeline(Config config) {
        var settings = new AdaptiveReorderingPipelineSettings(config);
        sampleOrder = settings.sampleOrderFactor();
        mainPipeline = new PipelinePolicy(config);
        stats = new ARPStats("Adaptive Reordering " + sampleOrder + " " + mainPipeline.generatePipelineName(), sampleOrder);
        adaptionTimeframe = settings.adaptionMultiplier() * mainPipeline.cacheCapacity();

        sampler = new XXH3Sampler(sampleOrder, settings.randomSeed());
        sampledMainCache = new PipelinePolicy(config, sampleOrder);
        candidateOrderings = generateAllOrderings(mainPipeline.getCurrentState().types);
        ghostCaches = new ArrayList<>(candidateOrderings.size());

        createGhostCaches();

        if (DUMP_STATES) {
            orderDump = prepareDump("order_dump");
            orderDump.println(printFormatState(0, this.mainPipeline.getCurrentState().types, 0, List.of()));
        }
        if (DUMP_RESULTS) {
          resultsDump = prepareDump("results_dump");
        }
    }

    private static List<BlockType[]> generateAllOrderings(BlockType[] referenceOrder) {
        List<BlockType[]> orderings = new ArrayList<>();
        collectOrderings(Arrays.copyOf(referenceOrder, referenceOrder.length), 0, orderings);
        return orderings;
    }

    private static void collectOrderings(BlockType[] order, int fromIdx, List<BlockType[]> results) {
        if (fromIdx == order.length) {
            results.add(Arrays.copyOf(order, order.length));
            return;
        }

        for (int idx = fromIdx; idx < order.length; ++idx) {
            swapInPlace(order, fromIdx, idx);
            collectOrderings(order, fromIdx + 1, results);
            swapInPlace(order, fromIdx, idx);
        }
    }

    private static void swapInPlace(BlockType[] order, int i, int j) {
        BlockType temp = order[i];
        order[i] = order[j];
        order[j] = temp;
    }

    private void createGhostCaches() {
        for (var pair : ghostCaches) {
            pair.first().clear();
        }

        ghostCaches.clear();
        BlockType[] currentOrder = sampledMainCache.getCurrentState().types;
        for (BlockType[] targetOrder : candidateOrderings) {
            if (!Arrays.equals(targetOrder, currentOrder)) {
                PipelinePolicy cache = sampledMainCache.createCopy();
                cache.reorderTo(targetOrder);
                ghostCaches.add(new ObjectObjectImmutablePair<>(cache, targetOrder));
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

        this.mainPipeline.resetTimeframeStats();

        double minAvg = currentAvg;
        int minIdx = -1;

        List<String> timeframeResults = new ArrayList<>(this.ghostCaches.size());
        for (int idx = 0; idx < this.ghostCaches.size(); ++idx) {
            var ghostPair = this.ghostCaches.get(idx);
            var currGhostCache = ghostPair.first();
            double currGhostAvg = currGhostCache.getTimeframeAveragePenalty();
            if (currGhostAvg < minAvg) {
                minAvg = currGhostAvg;
                minIdx = idx;
            }
            timeframeResults.add(describeCandidateResult(ghostPair.right(), currGhostAvg));
            currGhostCache.resetTimeframeStats();
        }

        if (minIdx >= 0) {
            BlockType[] targetOrder = this.ghostCaches.get(minIdx).right();

            this.mainPipeline.reorderTo(targetOrder);
            this.sampledMainCache.reorderTo(targetOrder);

            final var currState = this.mainPipeline.getCurrentState();
            final var sampledState = this.sampledMainCache.getCurrentState();

            Assert.assertCondition(Arrays.equals(currState.types, sampledState.types),
                                   () -> String.format("The sampled and Main configuration mismatch: main: %s sampled: %s",
                                                       Arrays.toString(currState.types),
                                                       Arrays.toString(sampledState.types)));

            createGhostCaches();
        }

        if (DUMP_STATES && orderDump != null) {
            orderDump.println(printFormatState(eventNum, this.mainPipeline.getCurrentState().types, currentAvg, timeframeResults));
            orderDump.flush();
        }
    }

    private static String describeCandidateResult(BlockType[] candidateOrder, double avgPen) {
        String penaltyLabel = avgPen < Double.MAX_VALUE ? String.format("%.2f", avgPen) : "NA";
        return joinOrder(candidateOrder) + ":" + penaltyLabel;
    }

    private static String joinOrder(BlockType[] order) {
        StringBuilder sb = new StringBuilder();
        for (int idx = 0; idx < order.length; ++idx) {
            if (idx > 0) {
                sb.append('-');
            }
            sb.append(order[idx]);
        }
        return sb.toString();
    }

    private String printFormatState(int eventNum, BlockType[] currentOrder, double avgPen, List<String> ghostResults) {
        StringBuilder sb = new StringBuilder();

        sb.append(eventNum);
        sb.append(',');
        sb.append(joinOrder(currentOrder));
        sb.append(',');
        sb.append(String.format("%.2f", avgPen));

        for (String result : ghostResults) {
            sb.append(',');
            sb.append(result);
        }

        return sb.toString();
    }

    @Override
    public PolicyStats stats() {
        return stats;
    }

    private PrintWriter prepareDump(String suffix) {
        String currentDir = System.getProperty("user.dir");
        PrintWriter dump = null;
        try {
            FileWriter fwriter = new FileWriter(currentDir + "/ARP-O" + sampleOrder + "." + suffix, StandardCharsets.UTF_8);
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
        if (DUMP_STATES && orderDump != null) {
            orderDump.close();
        }
    }

    public static class AdaptiveReorderingPipelineSettings extends BasicSettings {
        final static String BASE_PATH = "adaptive-reordering-pipeline";
        public AdaptiveReorderingPipelineSettings(Config config) {
            super(config);
        }

        public int sampleOrderFactor() { return config().getInt(BASE_PATH + ".sample-order-factor"); }

        public int adaptionMultiplier() { return config().getInt(BASE_PATH + ".adaption-multiplier"); }
    }

    @SuppressWarnings("this-escape")
    public static class ARPStats extends PolicyStats {
        private long sampledRequests = 0;
        public ARPStats(String format, int sampleOrder, Object... args) {
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
}
