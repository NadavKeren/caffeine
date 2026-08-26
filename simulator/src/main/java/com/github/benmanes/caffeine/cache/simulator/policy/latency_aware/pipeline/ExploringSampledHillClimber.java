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

/***
 * A variant of the {@link SampledHillClimber} that, in addition to the ghost caches that are a
 * single quantum swap away from the current configuration, simulates one extra ghost cache placed
 * far away in the configuration space (at least {@code min-swap-distance} quantum swaps away).
 *
 * The far ghost cache lives in rounds: it is fed but ignored during the warmup timeframes, letting
 * its contents adjust to its very different quota allocation, and afterwards it competes for
 * adoption exactly like the single-step ghost caches. When the round ends, or when the far ghost
 * cache is adopted, a new random far point is drawn and the round restarts.
 */
@SuppressWarnings("NullAway")
@Policy.PolicySpec(name = "latency-aware.ESHC")
public class ExploringSampledHillClimber implements Policy {
    private final static boolean DUMP_STATES = true;
    private final static boolean DUMP_RESULTS = true;
    private final static int MAX_DRAW_ATTEMPTS = 1000;
    @Nullable private PrintWriter quotaDump = null;
    @Nullable private PrintWriter resultsDump = null;

    private final PipelinePolicy mainPipeline;
    private final LongSampler sampler;

    private final PipelinePolicy sampledMainCache;
    private final List<Pair<PipelinePolicy, CacheDiff>> ghostCaches;

    private final ESHCStats stats;
    private final int blockCount;
    private final int adaptionTimeframe;
    private int opsSinceAdaption = 0;
    private final int sampleOrder;

    private final Random random;
    private final int warmupTimeframes;
    private final int evaluationTimeframes;
    private final int roundLength;
    private final int minSwapDistance;
    private final int maxRounds;
    private final int totalQuanta;

    /* The far ghost cache is deliberately kept out of the ghostCaches list, as that list is wiped
     * and rebuilt on every accepted adaption, which would restart the warmup over and over. */
    @Nullable private PipelinePolicy farGhost = null;
    @Nullable private int[] farGhostQuotas = null;
    private int farGhostAge = 0;
    private int roundsStarted = 0;
    private long explorationJumps = 0;

    public ExploringSampledHillClimber(Config config) {
        var settings = new ExploringSampledHillClimberSettings(config);
        sampleOrder = settings.sampleOrderFactor();
        mainPipeline = new PipelinePolicy(config);
        blockCount = mainPipeline.blockCount();
        stats = new ESHCStats("Exploring " + sampleOrder + " " + mainPipeline.generatePipelineName(),
                              sampleOrder,
                              this::roundsStarted,
                              this::explorationJumps);
        adaptionTimeframe = settings.adaptionMultiplier() * mainPipeline.cacheCapacity();

        sampler = new XXH3Sampler(sampleOrder, settings.randomSeed());
        sampledMainCache = new PipelinePolicy(config, sampleOrder);
        final int numOfCaches = blockCount * (blockCount - 1);
        ghostCaches = new ArrayList<>(numOfCaches);

        createGhostCaches();

        random = new Random(settings.randomSeed());
        warmupTimeframes = settings.warmupTimeframes();
        evaluationTimeframes = settings.evaluationTimeframes();
        roundLength = warmupTimeframes + evaluationTimeframes;
        minSwapDistance = settings.minSwapDistance();
        maxRounds = settings.maxRounds();
        totalQuanta = Arrays.stream(mainPipeline.getQuota()).sum();

        Assert.assertCondition(warmupTimeframes >= 0,
                               () -> "The number of warmup timeframes must not be negative, got " + warmupTimeframes);
        Assert.assertCondition(evaluationTimeframes >= 1,
                               () -> "The number of evaluation timeframes must be positive, got " + evaluationTimeframes);
        Assert.assertCondition(minSwapDistance >= 1 && minSwapDistance <= totalQuanta,
                               () -> String.format("The minimal swap distance %d is out of the legal range [1, %d]",
                                                   minSwapDistance,
                                                   totalQuanta));

        startNewRound();

        if (DUMP_STATES) {
            quotaDump = prepareDump("quota_dump");
            quotaDump.println(buildHeaderRow());
            quotaDump.println(printFormatState(0,
                                               this.mainPipeline.getCurrentState().quotas,
                                               emptySnapshot(),
                                               emptySnapshots(),
                                               emptySnapshot(),
                                               false));
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

    /***
     * Starts a new exploration round: draws a random configuration that is at least
     * minSwapDistance quantum swaps away from the current one, and creates a ghost cache for it.
     * Once the configured number of rounds was exhausted the far ghost cache is dropped, and this
     * policy behaves exactly like the SampledHillClimber.
     */
    private void startNewRound() {
        if (maxRounds >= 0 && roundsStarted >= maxRounds) {
            farGhost = null;
            farGhostQuotas = null;
            return;
        }

        farGhostQuotas = drawFarPoint(sampledMainCache.getQuota());
        farGhost = sampledMainCache.createCopy();
        walkTo(farGhost, farGhostQuotas);
        farGhost.resetTimeframeStats();
        farGhostAge = 0;
        ++roundsStarted;
    }

    /***
     * Draws a random legal quota vector that is at least minSwapDistance quantum swaps away from
     * the given one, by rejection sampling uniformly drawn vectors.
     */
    private int[] drawFarPoint(int[] current) {
        int[] best = null;
        int bestDistance = -1;

        for (int attempt = 0; attempt < MAX_DRAW_ATTEMPTS; ++attempt) {
            int[] candidate = drawQuotaVector();
            int distance = swapDistance(current, candidate);

            if (distance >= minSwapDistance) {
                return candidate;
            }

            if (distance > bestDistance) {
                bestDistance = distance;
                best = candidate;
            }
        }

        final int reportedDistance = bestDistance;
        Assert.assertCondition(reportedDistance >= minSwapDistance,
                               () -> String.format("Could not draw a configuration at least %d swaps away from %s "
                                                   + "in %d attempts (best was %d swaps away); "
                                                   + "%d quanta over %d blocks may be too few",
                                                   minSwapDistance,
                                                   Arrays.toString(current),
                                                   MAX_DRAW_ATTEMPTS,
                                                   reportedDistance,
                                                   totalQuanta,
                                                   blockCount));

        return best;
    }

    /***
     * Draws a quota vector uniformly out of all the vectors of blockCount non-negative entries
     * summing up to totalQuanta, using the stars and bars method.
     */
    private int[] drawQuotaVector() {
        int[] cuts = new int[blockCount + 1];
        cuts[0] = 0;
        cuts[blockCount] = totalQuanta;

        for (int idx = 1; idx < blockCount; ++idx) {
            cuts[idx] = random.nextInt(totalQuanta + 1);
        }

        Arrays.sort(cuts);

        int[] quotas = new int[blockCount];
        for (int idx = 0; idx < blockCount; ++idx) {
            quotas[idx] = cuts[idx + 1] - cuts[idx];
        }

        return quotas;
    }

    /***
     * The number of quantum swaps needed in order to get from one configuration to the other,
     * which is half of the L1 distance between them.
     */
    private static int swapDistance(int[] from, int[] to) {
        int distance = 0;
        for (int idx = 0; idx < from.length; ++idx) {
            distance += Math.max(0, to[idx] - from[idx]);
        }

        return distance;
    }

    /***
     * Moves the given pipeline to the target configuration, one quantum at a time.
     * @return the number of quantum swaps performed.
     */
    private int walkTo(PipelinePolicy cache, int[] target) {
        Assert.assertCondition(Arrays.stream(target).sum() == totalQuanta,
                               () -> String.format("The target configuration %s does not sum up to %d quanta",
                                                   Arrays.toString(target),
                                                   totalQuanta));

        int[] current = cache.getQuota();
        int moves = 0;
        int incIdx = 0;
        int decIdx = 0;

        while (true) {
            while (incIdx < blockCount && current[incIdx] >= target[incIdx]) {
                ++incIdx;
            }
            while (decIdx < blockCount && current[decIdx] <= target[decIdx]) {
                ++decIdx;
            }

            if (incIdx >= blockCount || decIdx >= blockCount) {
                break;
            }

            final int inc = incIdx;
            final int dec = decIdx;
            Assert.assertCondition(cache.canExtend(inc) && cache.canShrink(dec),
                                   () -> String.format("Illegal walk step: increasing %d, decreasing %d, on %s towards %s",
                                                       inc,
                                                       dec,
                                                       Arrays.toString(cache.getQuota()),
                                                       Arrays.toString(target)));

            cache.moveQuantum(inc, dec);
            ++current[inc];
            --current[dec];
            ++moves;
        }

        Assert.assertCondition(Arrays.equals(current, target),
                               () -> String.format("Failed to walk to the target configuration: got %s, expected %s",
                                                   Arrays.toString(current),
                                                   Arrays.toString(target)));

        return moves;
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

            if (farGhost != null) {
                farGhost.record(event);
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

        boolean farIsBest = false;
        GhostStatsSnapshot farSnapshot = GhostStatsSnapshot.na();

        if (this.farGhost != null) {
            final double farAvg = this.farGhost.getTimeframeAveragePenalty();
            final boolean isEvaluated = this.farGhostAge >= this.warmupTimeframes;
            // An empty timeframe yields a NaN average penalty, which must not win the comparison.
            final boolean isValid = !Double.isNaN(farAvg) && farAvg < Double.MAX_VALUE;

            if (isEvaluated && isValid) {
                farSnapshot = new GhostStatsSnapshot(farAvg,
                                                     this.farGhost.getTimeframeUniqueCount(),
                                                     this.farGhost.getTimeframeHitCount(),
                                                     this.farGhost.getTimeframeEvictionCount(),
                                                     this.farGhost.getTimeframeSavedLatency(),
                                                     this.farGhost.getTimeframeHitsPerBlock(),
                                                     this.farGhost.getTimeframeEvictionsPerBlock(),
                                                     this.farGhost.getTimeframeSavedLatencyPerBlock());

                if (farAvg < minAvg) {
                    minAvg = farAvg;
                    farIsBest = true;
                }
            }

            this.farGhost.resetTimeframeStats();
            ++this.farGhostAge;
        }

        final int[] appliedFarQuotas = this.farGhostQuotas;
        final int appliedRound = this.roundsStarted;
        final int appliedAge = this.farGhostAge;

        if (farIsBest) {
            final var quotasBeforeJump = this.mainPipeline.getQuota();
            Assert.assertCondition(Arrays.equals(quotasBeforeJump, this.sampledMainCache.getQuota()),
                                   () -> String.format("The sampled and Main configuration mismatch: main: %s sampled: %s",
                                                       Arrays.toString(quotasBeforeJump),
                                                       Arrays.toString(this.sampledMainCache.getQuota())));

            walkTo(this.mainPipeline, appliedFarQuotas);
            walkTo(this.sampledMainCache, appliedFarQuotas);

            final var currState = this.mainPipeline.getCurrentState();
            final var sampledState = this.sampledMainCache.getCurrentState();

            Assert.assertCondition(Arrays.equals(currState.quotas, sampledState.quotas),
                                   () -> String.format("The sampled and Main configuration mismatch: main: %s sampled: %s",
                                                       Arrays.toString(currState.quotas),
                                                       Arrays.toString(sampledState.quotas)));

            ++explorationJumps;
            createGhostCaches();
            startNewRound();
        } else if (minIdx >= 0) {
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

        if (!farIsBest && this.farGhost != null && this.farGhostAge >= this.roundLength) {
            startNewRound();
        }

        if (DUMP_STATES && quotaDump != null) {
            quotaDump.println(printFormatState(eventNum,
                                               this.mainPipeline.getCurrentState().quotas,
                                               mainSnapshot,
                                               snapshots,
                                               farSnapshot,
                                               farIsBest,
                                               appliedRound,
                                               appliedAge,
                                               appliedFarQuotas));
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

        sb.append(",far_round");
        sb.append(",far_age");
        for (int idx = 0; idx < blockCount; ++idx) {
            sb.append(",far_quota_").append(idx);
        }
        sb.append(",far_applied");
        appendSnapshotHeader(sb, "far");

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

    private String printFormatState(int eventNum,
                                    int[] quotas,
                                    GhostStatsSnapshot mainSnapshot,
                                    GhostStatsSnapshot[] snapshots,
                                    GhostStatsSnapshot farSnapshot,
                                    boolean farApplied) {
        return printFormatState(eventNum,
                                quotas,
                                mainSnapshot,
                                snapshots,
                                farSnapshot,
                                farApplied,
                                this.roundsStarted,
                                this.farGhostAge,
                                this.farGhostQuotas);
    }

    private String printFormatState(int eventNum,
                                    int[] quotas,
                                    GhostStatsSnapshot mainSnapshot,
                                    GhostStatsSnapshot[] snapshots,
                                    GhostStatsSnapshot farSnapshot,
                                    boolean farApplied,
                                    int farRound,
                                    int farAge,
                                    @Nullable int[] farQuotas) {
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

        sb.append(',').append(farRound);
        sb.append(',').append(farQuotas != null ? String.valueOf(farAge) : "NA");
        for (int idx = 0; idx < blockCount; ++idx) {
            sb.append(',').append(farQuotas != null ? String.valueOf(farQuotas[idx]) : "NA");
        }
        sb.append(',').append(farApplied ? 1 : 0);
        appendSnapshot(sb, farSnapshot);

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

    private long roundsStarted() {
        return roundsStarted;
    }

    private long explorationJumps() {
        return explorationJumps;
    }

    private PrintWriter prepareDump(String suffix) {
        String currentDir = System.getProperty("user.dir");
        PrintWriter dump = null;
        try {
            FileWriter fwriter = new FileWriter(currentDir + "/ESHC-O" + sampleOrder + "." + suffix, StandardCharsets.UTF_8);
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

    public static class ExploringSampledHillClimberSettings extends BasicSettings {
        final static String BASE_PATH = "exploring-sampled-hill-climber";
        public ExploringSampledHillClimberSettings(Config config) {
            super(config);
        }

        public int sampleOrderFactor() { return config().getInt(BASE_PATH + ".sample-order-factor"); }

        public int adaptionMultiplier() { return config().getInt(BASE_PATH + ".adaption-multiplier"); }

        public int warmupTimeframes() { return config().getInt(BASE_PATH + ".warmup-timeframes"); }

        public int evaluationTimeframes() { return config().getInt(BASE_PATH + ".evaluation-timeframes"); }

        public int minSwapDistance() { return config().getInt(BASE_PATH + ".min-swap-distance"); }

        public int maxRounds() { return config().getInt(BASE_PATH + ".max-rounds"); }
    }

    @SuppressWarnings("this-escape")
    public static class ESHCStats extends PolicyStats {
        private long sampledRequests = 0;
        public ESHCStats(String format,
                         int sampleOrder,
                         LongSupplier roundsStarted,
                         LongSupplier explorationJumps,
                         Object... args) {
            super(format, args);
            addMetric(new Metric.Builder().name("Sampling Percentage")
                                          .value(this::samplingPercentage)
                                          .type(Metric.MetricType.PERCENT)
                                          .required(true));
            addMetric(new Metric.Builder().name("Sample Order")
                                          .value(sampleOrder)
                                          .type(Metric.MetricType.NUMBER)
                                          .required(true));
            addMetric(new Metric.Builder().name("Exploration Rounds")
                                          .value(roundsStarted)
                                          .type(Metric.MetricType.NUMBER)
                                          .required(true));
            addMetric(new Metric.Builder().name("Exploration Jumps")
                                          .value(explorationJumps)
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
