package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.typesafe.config.Config;

import javax.annotation.Nullable;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

/***
 * An adaptation mechanism that decides on the quota of each block out of the statistics of the live
 * pipeline, instead of simulating the candidate configurations on ghost caches as the
 * {@link SampledHillClimber} does.
 * <p>
 * At the end of each timeframe the hits of every block are spread over its quanta, from the most hit
 * quantum to the least hit one. The last band, h_lowest, is what the block would lose by giving a
 * quantum away, and h_lowest^2 / h_second_lowest extrapolates the bands one step further down, thus
 * estimating what the block would gain from an additional quantum. A single quantum is moved from the
 * block with the lowest h_lowest to the block with the highest potential.
 * TODO: nkeren - add citation when available
 */
@SuppressWarnings("NullAway")
@Policy.PolicySpec(name = "latency-aware.SBC")
public class StatisticsBasedClimber implements Policy {
    private final static boolean DUMP_STATES = true;
    /***
     * The minimal quota of a block. A block of a single quantum has no h_second_lowest to extrapolate
     * from, and an empty block would have no statistics at all, hence could never be scored back up.
     */
    private final static int MINIMAL_QUOTA = 1;

    @Nullable private PrintWriter quotaDump = null;

    private final PipelinePolicy mainPipeline;

    private final PolicyStats stats;
    private final int blockCount;
    private final int adaptionTimeframe;
    private int opsSinceAdaption = 0;

    public StatisticsBasedClimber(Config config) {
        var settings = new StatisticsBasedClimberSettings(config);
        mainPipeline = new PipelinePolicy(config);
        mainPipeline.enableHitStatistics();
        blockCount = mainPipeline.blockCount();
        stats = new PolicyStats("SBC " + mainPipeline.generatePipelineName());
        adaptionTimeframe = settings.adaptionMultiplier() * mainPipeline.cacheCapacity();

        if (DUMP_STATES) {
            quotaDump = prepareDump("quota_dump");

            final var initialState = mainPipeline.getCurrentState();
            int[][] emptyBands = new int[blockCount][];
            for (int idx = 0; idx < blockCount; ++idx) {
                emptyBands[idx] = new int[initialState.quotas[idx]];
            }

            quotaDump.println(printFormatState(0, initialState, initialState.quotas, emptyBands));
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

        ++opsSinceAdaption;

        if (opsSinceAdaption >= adaptionTimeframe) {
            opsSinceAdaption = 0;
            adapt(event.eventNum());
        }
    }

    private void adapt(int eventNum) {
        final var currentState = this.mainPipeline.getCurrentState();
        final int[][] bands = this.mainPipeline.getTimeframeHitDistribution();

        int decIdx = -1;
        int incIdx = -1;
        int minLowest = Integer.MAX_VALUE;
        double maxPotential = 0d;

        for (int idx = 0; idx < blockCount; ++idx) {
            final int currQuota = currentState.quotas[idx];
            final int hLowest = bands[idx][currQuota - 1];
            /*
             * A block of a single quantum has no band to extrapolate from, thus its bands are assumed
             * flat and its potential collapses into h_lowest.
             */
            final int hSecondLowest = currQuota > 1 ? bands[idx][currQuota - 2] : hLowest;
            final double potential = hSecondLowest > 0
                                   ? (double) hLowest * hLowest / hSecondLowest
                                   : 0d;

            if (currQuota > MINIMAL_QUOTA && hLowest < minLowest) {
                minLowest = hLowest;
                decIdx = idx;
            }

            if (this.mainPipeline.canExtend(idx) && potential > maxPotential) {
                maxPotential = potential;
                incIdx = idx;
            }
        }

        int[] chosenQuotas = currentState.quotas;

        if (incIdx >= 0 && decIdx >= 0 && incIdx != decIdx) {
            final int finalIncIdx = incIdx;
            final int finalDecIdx = decIdx;

            Assert.assertCondition(this.mainPipeline.canExtend(incIdx) && this.mainPipeline.canShrink(decIdx),
                                   () -> String.format("Illegal adaption performed: increasing %s, decreasing %s",
                                                       this.mainPipeline.getType(finalIncIdx),
                                                       this.mainPipeline.getType(finalDecIdx)));

            this.mainPipeline.moveQuantum(incIdx, decIdx);
            chosenQuotas = this.mainPipeline.getCurrentState().quotas;
        }

        if (DUMP_STATES && quotaDump != null) {
            quotaDump.println(printFormatState(eventNum, currentState, chosenQuotas, bands));
            quotaDump.flush();
        }

        this.mainPipeline.resetTimeframeStats();
    }

    private String printFormatState(int eventNum,
                                    PipelinePolicy.PipelineState currentState,
                                    int[] chosenQuotas,
                                    int[][] bands) {
        StringBuilder sb = new StringBuilder();

        sb.append(eventNum);

        for (int quota : currentState.quotas) {
            sb.append(',');
            sb.append(quota);
        }

        for (int quota : chosenQuotas) {
            sb.append(',');
            sb.append(quota);
        }

        for (int idx = 0; idx < blockCount; ++idx) {
            sb.append(',');
            sb.append(currentState.types[idx]);

            for (int band : bands[idx]) {
                sb.append(',');
                sb.append(band);
            }
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
            FileWriter fwriter = new FileWriter(currentDir + "/SBC." + suffix, StandardCharsets.UTF_8);
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

    public static class StatisticsBasedClimberSettings extends BasicSettings {
        final static String BASE_PATH = "statistics-based-climber";

        public StatisticsBasedClimberSettings(Config config) {
            super(config);
        }

        public int adaptionMultiplier() { return config().getInt(BASE_PATH + ".adaption-multiplier"); }
    }
}
