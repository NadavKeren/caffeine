package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.rankmap;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.AdaptationObjective;
import com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.PipelinePolicy;
import com.typesafe.config.Config;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/***
 * Adapts a pipeline's quanta allocation by estimating what every candidate allocation would have
 * achieved, instead of running the candidates.
 * <p>
 * One shadow ranking per stage holds the top-capacity objects under that stage's own eviction order.
 * The requested key's pre-access rank in each board answers "would a stage of this size have held
 * it", and since that is one comparison against a per-stage depth, a single rank vector answers the
 * question for every candidate allocation at once. Nothing about the candidates is materialized.
 * <p>
 * The real cache is an ordinary {@link PipelinePolicy}, so its statistics are exactly what that
 * policy would report on its own; only the allocation it runs is chosen differently.
 */
@SuppressWarnings("NullAway")
@Policy.PolicySpec(name = "latency-aware.RankMap")
public final class RankMapClimber implements Policy {
    final private PipelinePolicy mainPipeline;
    final private RankSource rankSource;
    final private AllocationTree tree;
    final private ReachDepthSolver solver;
    final private AdaptationObjective objective;
    @Nullable final private SequenceBenefitEstimator benefitEstimator;
    @Nullable final private BenefitDataWriter benefitWriter;
    @Nullable final private BenefitDataReader benefitReader;
    @Nullable final private OracleScorer oracle;
    @Nullable final private PrintWriter decisionLog;

    final private PolicyStats stats;
    final private int decisionInterval;
    final private int benefitFlushInterval;
    final private double[] weight;
    final private double intervalDecay;
    final private double moveThreshold;

    private double currentWeight = 0d;
    private int opsSinceDecision = 0;
    private int moves = 0;
    private long requests = 0;
    private boolean intervalUsable = false;

    public RankMapClimber(Config config) {
        var settings = new RankMapSettings(config);

        this.mainPipeline = new PipelinePolicy(config);
        this.objective = settings.objective();
        this.moveThreshold = settings.moveThreshold();

        final int capacity = mainPipeline.cacheCapacity();
        this.decisionInterval = settings.decisionMultiplier() * capacity;

        // Every refresh of the reach depths has to land on a recorded snapshot, otherwise a replay
        // cannot answer what the boards held at that moment. A replay checks this against the
        // recording it finds instead, so it is only a constraint on what is written.
        Assert.assertCondition(settings.precomputeMode() == RankDataFormat.Mode.OFF
                               || settings.precomputeMode() == RankDataFormat.Mode.READ
                               || (settings.snapshotInterval() > 0
                                   && decisionInterval % settings.snapshotInterval() == 0),
                               () -> String.format(
                                       "The decision interval %d is not a multiple of the snapshot interval %d",
                                       decisionInterval,
                                       settings.snapshotInterval()));

        this.rankSource = createRankSource(config, settings, decisionInterval);
        this.tree = new AllocationTree(mainPipeline.blockCount(), settings.totalQuanta(config));
        this.solver = new ReachDepthSolver(rankSource, tree, capacity / settings.totalQuanta(config));

        // A recording carries both objectives' data, so a recording run keeps the latency windows
        // whatever it optimizes, and a latency replay reads them instead of estimating them.
        if (rankSource instanceof RankDataWriter writer) {
            this.benefitFlushInterval = writer.snapshotInterval();
            this.benefitWriter = new BenefitDataWriter(config, RankDataFormat.benefitFileFor(writer.path()),
                                                       rankSource.stageCount(), capacity, benefitFlushInterval);
            this.benefitReader = null;
            this.benefitEstimator = new SequenceBenefitEstimator(config, capacity);
        } else if (rankSource instanceof RankDataReader reader && objective == AdaptationObjective.LATENCY) {
            this.benefitWriter = null;
            this.benefitReader = new BenefitDataReader(config, RankDataFormat.benefitFileFor(reader.path()),
                                                       rankSource.stageCount());
            this.benefitFlushInterval = benefitReader.flushInterval();
            this.benefitEstimator = null;
        } else {
            this.benefitWriter = null;
            this.benefitReader = null;
            this.benefitFlushInterval = decisionInterval;
            this.benefitEstimator = objective == AdaptationObjective.LATENCY
                                    ? new SequenceBenefitEstimator(config, capacity)
                                    : null;
        }

        // Windows are closed at least at every decision, so none is scored against depths other than
        // the ones frozen while it was open. A recording closes them at every snapshot instead, which
        // is what lets it serve every decision interval that is a multiple of it.
        Assert.assertCondition(decisionInterval % benefitFlushInterval == 0,
                               () -> String.format(
                                       "The decision interval %d is not a multiple of the benefit flush interval %d",
                                       decisionInterval,
                                       benefitFlushInterval));

        this.oracle = settings.validateOracle() ? new OracleScorer() : null;

        final double halfLife = (double) settings.halfLifeMultiplier() * capacity;
        final double decayPerRequest = Math.exp(Math.log(0.5) / halfLife);
        this.intervalDecay = Math.pow(decayPerRequest, decisionInterval);

        // The exact contribution a request at each position in the interval still has at its end.
        this.weight = new double[decisionInterval + 1];
        weight[decisionInterval] = 1 - decayPerRequest;
        for (int position = decisionInterval - 1; position >= 1; --position) {
            weight[position] = decayPerRequest * weight[position + 1];
        }

        this.stats = new PolicyStats("RankMap " + mainPipeline.generatePipelineName());
        this.decisionLog = openDecisionLog(settings, mainPipeline.blockCount());

        solver.refresh();
    }

    @Nullable
    private static PrintWriter openDecisionLog(RankMapSettings settings, int blockCount) {
        final String path = settings.decisionLog();
        if (path.isEmpty()) {
            return null;
        }

        try {
            Path target = Path.of(path);
            if (target.getParent() != null) {
                Files.createDirectories(target.getParent());
            }

            var writer = new PrintWriter(Files.newBufferedWriter(target, StandardCharsets.UTF_8));
            var header = new StringBuilder("request");
            for (int block = 0; block < blockCount; ++block) {
                header.append(",quota").append(block);
            }
            // Only the chosen allocation and the spread around it: with a fine quantum there are tens
            // of thousands of candidates, and a column each is of no use to anyone.
            header.append(",current_score,best_score,gain,moved");
            writer.println(header);
            writer.flush();

            return writer;
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot open the decision log at " + path, e);
        }
    }

    /***
     * The boards depend only on the trace and the cache size, so a run can either build them or
     * replay a recording an earlier run made at that size. A replay takes any recording of these
     * boards whose snapshots line up with its decisions, so runs with different decision intervals
     * and quanta all replay the one recording rather than each wanting its own.
     */
    private static RankSource createRankSource(Config config, RankMapSettings settings, int decisionInterval) {
        final var mode = settings.precomputeMode();

        if (mode == RankDataFormat.Mode.OFF) {
            return new RankedPipeline(config);
        }

        if (mode != RankDataFormat.Mode.WRITE) {
            final var recording = RankDataFormat.findRecording(config, settings.precomputeDirectory(), decisionInterval);

            if (recording.isPresent()) {
                return new RankDataReader(config, recording.get(), decisionInterval);
            }
            if (mode == RankDataFormat.Mode.READ) {
                throw new IllegalStateException(String.format(
                        "No recording of these boards in %s has snapshots that line up with a decision every %d requests (expected %s with S dividing %d)",
                        settings.precomputeDirectory(),
                        decisionInterval,
                        RankDataFormat.fileFor(config, settings.precomputeDirectory(), decisionInterval).getFileName(),
                        decisionInterval));
            }
        }

        final int snapshotInterval = settings.snapshotInterval();
        final Path path = RankDataFormat.fileFor(config, settings.precomputeDirectory(), snapshotInterval);

        return new RankDataWriter(config, new RankedPipeline(config), path, snapshotInterval);
    }

    @Override
    public void record(AccessEvent event) {
        final int[] ranks = rankSource.preAccessRanks(event.key());
        final boolean warm = rankSource.isWarm();

        if (warm) {
            intervalUsable = true;
            currentWeight = weight[opsSinceDecision + 1];

            if (objective == AdaptationObjective.HIT_RATIO) {
                creditCandidates(ranks, currentWeight);
            }

            if (benefitReader != null) {
                benefitReader.emit(requests, this::creditBenefit);
            } else if (benefitEstimator != null) {
                benefitEstimator.onArrival(event, ranks, this::emitBenefit);
            }
        }

        mainPipeline.record(event);
        recordStats(event);
        rankSource.update(event);

        // A window flushed here is credited with this request's weight, the same as one that closed
        // on its own during it, so a replay can hand both out at this request index.
        if (benefitEstimator != null && (requests + 1) % benefitFlushInterval == 0) {
            benefitEstimator.flush(this::emitBenefit);
        }

        ++requests;
        ++opsSinceDecision;
        if (opsSinceDecision >= decisionInterval) {
            decide();
        }
    }

    private void emitBenefit(int[] ranks, double benefit) {
        if (benefitWriter != null) {
            benefitWriter.write(requests, ranks, benefit);
        }

        if (objective == AdaptationObjective.LATENCY) {
            creditBenefit(ranks, benefit);
        }
    }

    private void creditBenefit(int[] ranks, double benefit) {
        creditCandidates(ranks, currentWeight * benefit);
    }

    private void creditCandidates(int[] ranks, double gain) {
        if (gain == 0d) {
            return;
        }

        // An object outside every board misses under every candidate, which on a real trace is most
        // requests. Cutting those out here is what keeps the walk off the hot path.
        for (int rank : ranks) {
            if (rank <= rankSource.capacity()) {
                tree.credit(ranks, gain);

                if (oracle != null) {
                    oracle.credit(ranks, gain);
                }
                return;
            }
        }
    }

    private void decide() {
        if (intervalUsable) {
            tree.foldInterval(intervalDecay);

            if (oracle != null) {
                oracle.foldAndVerify();
            }

            final int current = tree.indexOf(mainPipeline.getQuota());
            final int best = tree.best();
            final double gain = tree.score(best) - tree.score(current);
            boolean moved = false;

            if (best != current && gain > moveThreshold) {
                mainPipeline.moveTo(tree.quotaOf(best));
                ++moves;
                moved = true;
            }

            logDecision(tree.score(current), tree.score(best), gain, moved);
        } else {
            tree.discardInterval();

            if (oracle != null) {
                oracle.discard();
            }
        }

        solver.refresh();

        if (oracle != null) {
            oracle.verifyDepths();
        }

        opsSinceDecision = 0;
    }

    private void logDecision(double currentScore, double bestScore, double gain, boolean moved) {
        if (decisionLog == null) {
            return;
        }

        var row = new StringBuilder();
        row.append(requests);
        for (int quota : mainPipeline.getQuota()) {
            row.append(',').append(quota);
        }
        row.append(',').append(currentScore)
           .append(',').append(bestScore)
           .append(',').append(gain)
           .append(',').append(moved ? 1 : 0);

        decisionLog.println(row);
        decisionLog.flush();
    }

    private void recordStats(AccessEvent event) {
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
    }

    @Override
    public PolicyStats stats() {
        return stats;
    }

    @Override
    public void finished() {
        stats.setPercentAdaption(moves);
        // The harness calls finished() but never dump(), and a recording is only usable once its
        // header has been patched with the warm-up point and the file renamed into place.
        rankSource.close();

        if (benefitWriter != null) {
            benefitWriter.close();
        }
        if (benefitReader != null) {
            benefitReader.close();
        }

        if (decisionLog != null) {
            decisionLog.close();
        }
    }

    /***
     * The correctness oracle: the same projection done the obvious way, with no work shared between
     * candidates. Far too slow for a real sweep, and the only thing that stays obviously correct as
     * the stage count grows.
     */
    private final class OracleScorer {
        private double[] accumulated = new double[tree.candidateCount()];
        private double[] score = new double[tree.candidateCount()];
        private int[][] depths = null;

        void credit(int[] ranks, double gain) {
            final int[][] candidateDepths = depths();

            for (int candidate = 0; candidate < candidateDepths.length; ++candidate) {
                final int[] stageDepths = candidateDepths[candidate];

                for (int stage = 0; stage < stageDepths.length; ++stage) {
                    if (ranks[stage] <= stageDepths[stage]) {
                        accumulated[candidate] += gain;
                        break;
                    }
                }
            }
        }

        void foldAndVerify() {
            for (int candidate = 0; candidate < score.length; ++candidate) {
                score[candidate] = intervalDecay * score[candidate] + accumulated[candidate];
                accumulated[candidate] = 0d;
            }

            for (int candidate = 0; candidate < score.length; ++candidate) {
                final double projected = tree.score(candidate);
                final double reference = score[candidate];
                final int finalCandidate = candidate;

                Assert.assertCondition(Math.abs(projected - reference) <= 1e-6 * (1 + Math.abs(reference)),
                                       () -> String.format("Candidate %s: subtree scoring gave %f, the naive loop gave %f",
                                                           Arrays.toString(tree.quotaOf(finalCandidate)),
                                                           projected,
                                                           reference));
            }
        }

        void discard() {
            Arrays.fill(accumulated, 0d);
        }

        void verifyDepths() {
            final int[][] fromTree = solver.treeDepths();
            final int[][] naive = solver.naiveDepths();

            for (int candidate = 0; candidate < fromTree.length; ++candidate) {
                final int finalCandidate = candidate;
                Assert.assertCondition(Arrays.equals(fromTree[candidate], naive[candidate]),
                                       () -> String.format("Candidate %s: tree depths %s, reference depths %s",
                                                           Arrays.toString(tree.quotaOf(finalCandidate)),
                                                           Arrays.toString(fromTree[finalCandidate]),
                                                           Arrays.toString(naive[finalCandidate])));
            }

            depths = fromTree;
        }

        private int[][] depths() {
            if (depths == null) {
                depths = solver.treeDepths();
            }

            return depths;
        }
    }

    public static final class RankMapSettings extends BasicSettings {
        final static String BASE_PATH = "rank-map";

        public RankMapSettings(Config config) {
            super(config);
        }

        public AdaptationObjective objective() {
            return AdaptationObjective.parse(config().getString(BASE_PATH + ".objective"));
        }

        public int decisionMultiplier() { return config().getInt(BASE_PATH + ".decision-multiplier"); }

        public int halfLifeMultiplier() { return config().getInt(BASE_PATH + ".half-life-multiplier"); }

        public double moveThreshold() { return config().getDouble(BASE_PATH + ".move-threshold"); }

        public boolean validateOracle() { return config().getBoolean(BASE_PATH + ".validate-oracle"); }

        public RankDataFormat.Mode precomputeMode() {
            return RankDataFormat.Mode.parse(config().getString(BASE_PATH + ".precompute.mode"));
        }

        public String precomputeDirectory() { return config().getString(BASE_PATH + ".precompute.directory"); }

        public int snapshotInterval() { return config().getInt(BASE_PATH + ".precompute.snapshot-interval"); }

        public String decisionLog() { return config().getString(BASE_PATH + ".decision-log"); }

        public int totalQuanta(Config config) {
            return new PipelinePolicy.PipelineSettings(config).numOfQuanta();
        }
    }
}
