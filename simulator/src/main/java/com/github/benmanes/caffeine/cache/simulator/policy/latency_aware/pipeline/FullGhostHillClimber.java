package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.ConsoleColors;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Policy.PolicySpec(name = "latency-aware.FGHC")
public class FullGhostHillClimber implements Policy {
    private final static boolean DUMP_QUOTAS = true;
    private final static boolean DEBUG = false;
    @Nullable private PrintWriter quotaDump = null;
    @Nullable private PrintWriter logger = null;

    private final PipelinePolicy mainPipeline;
    private final List<Pair<PipelinePolicy, CacheDiff>> ghostCaches;

    private final PolicyStats stats;
    private final int blockCount;

    private final int adaptionTimeframe;
    private int opsSinceAdaption = 0;

    public FullGhostHillClimber(Config config) {
        var hillClimberSettings = new FGHCSettings(config);
        mainPipeline = new PipelinePolicy(config);
        blockCount = mainPipeline.blockCount();
        stats = new PolicyStats("FGHC " + mainPipeline.generatePipelineName());
        adaptionTimeframe = (int) (hillClimberSettings.adaptionMultiplier()
                                   * mainPipeline.cacheCapacity());

        final int numOfCaches = blockCount * (blockCount - 1);
        ghostCaches = new ArrayList<>(numOfCaches);


        if (DEBUG) {
            prepareLog();
        }

        createGhostCaches(0);

        if (DUMP_QUOTAS) {
            prepareQuotaDump();
            dumpState(0, 0, 0,  new double[this.ghostCaches.size()]);
        }
    }

    private void createGhostCaches(int eventNum) {
        for (var pair : ghostCaches) {
            pair.first().clear();
        }

        ghostCaches.clear();

        if (logger != null) {
            logger.println(ConsoleColors.colorString(eventNum + ":\tEmptying the ghost caches", ConsoleColors.PURPLE_BOLD));
            logger.println(ConsoleColors.colorString(eventNum + ":\tCurrent Cache configuration: " + Arrays.toString(mainPipeline.getQuota()), ConsoleColors.CYAN_BOLD));
        }

        int idx = 0;
        for (int inc = 0; inc < blockCount; ++inc) {
            for (int dec = 0; dec < blockCount; ++dec) {
                if (inc != dec) {
                    var diff = new CacheDiff(inc, dec);
                    PipelinePolicy cache = PipelinePolicy.DUMMY;
                    if (mainPipeline.canExtend(inc) && mainPipeline.canShrink(dec)) {
                        cache = mainPipeline.createCopy();
                        cache.moveQuantum(inc, dec);

                        if (logger != null) {
                            logger.printf("%d:\tCreated ghost cache %d: %s\n", eventNum, idx, Arrays.toString(cache.getQuota()));
                        }
                    }
                    ghostCaches.add(idx, new ObjectObjectImmutablePair<>(cache, diff));
                    ++idx;
                }
            }
        }

        if (logger != null) {
            logger.println(ConsoleColors.colorString(eventNum + ":\tGhost creation ended", ConsoleColors.PURPLE_BOLD));
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

        for (var pair : ghostCaches) {
            var cache = pair.first();
            if (cache != null) {
                cache.record(event);
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
        final double currentHitRatio = this.mainPipeline.getTimeframeHitRatio();
        if (logger != null) {
            logger.println(ConsoleColors.colorString(eventNum + ":\tmain cache: " + this.mainPipeline.getTimeframeStats(), ConsoleColors.YELLOW));
        }

        this.mainPipeline.resetTimeframeStats();

        double minAvg = currentAvg;
        int minIdx = -1;

        double[] timeframeResults = new double[this.ghostCaches.size()];

        for (int idx = 0; idx < this.ghostCaches.size(); ++idx) {
            var currGhostCache = this.ghostCaches.get(idx).first();
            double currGhostAvg = currGhostCache.getTimeframeAveragePenalty();
            if (logger != null && !currGhostCache.isDummy()) {
                logger.println(ConsoleColors.colorString(eventNum + ":\tghost cache " + idx + ": " + currGhostCache.getTimeframeStats(), ConsoleColors.BLUE));
            }

            currGhostCache.resetTimeframeStats();


            timeframeResults[idx] = currGhostAvg;
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

            if (logger != null) {
                logger.println(ConsoleColors.colorString(eventNum + ":\tIncreasing: " + adaption.incIdx + "\tDecreasing: " + adaption.decIdx, ConsoleColors.GREEN_BOLD));
            }


            createGhostCaches(eventNum);
        }

        if (DUMP_QUOTAS && quotaDump != null) {
            dumpState(eventNum, currentAvg, currentHitRatio, timeframeResults);
        }
    }

    private void dumpState(int eventNum, double currentAvg, double currentHitRatio, double[] timeframeResults) {
        var currState = this.mainPipeline.getCurrentState();
        var currCacheState = new CacheState(eventNum, currState.quotas, currentAvg, currentHitRatio);
        quotaDump.print(currCacheState);
        quotaDump.print(",");
        for (int idx = 0; idx < this.ghostCaches.size(); ++idx) {
            if (!ghostCaches.get(idx).first().isDummy()) {
                quotaDump.print(String.format("%.2f", timeframeResults[idx]));
            } else {
                quotaDump.print("NA");
            }
            if (idx != this.ghostCaches.size() - 1) {
                quotaDump.print(",");
            }
        }
        quotaDump.println();
        quotaDump.flush();
    }

    @Override
    public PolicyStats stats() {
        return stats;
    }

    private void prepareQuotaDump() {
        String currentDir = System.getProperty("user.dir");
        try {
            FileWriter fwriter = new FileWriter(currentDir + "/FGHC.quota-dump", StandardCharsets.UTF_8);
            quotaDump = new PrintWriter(fwriter);
        } catch (IOException e) {
            System.err.println("Error creating the log file handler");
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void prepareLog() {
        String currentDir = System.getProperty("user.dir");
        try {
            FileWriter fwriter = new FileWriter(currentDir + "/FGHC.log", StandardCharsets.UTF_8);
             logger = new PrintWriter(fwriter);
        } catch (IOException e) {
            System.err.println("Error creating the log file handler");
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public void dump() {
        if (DUMP_QUOTAS && quotaDump != null) {
            quotaDump.close();
        }
    }

    protected static class FGHCSettings extends BasicSettings {
        private final static String BASE_PATH = "full-ghost-hill-climber";

        public FGHCSettings(Config config) { super(config); }

        public double adaptionMultiplier() { return config().getDouble(BASE_PATH + ".adaption-multiplier"); }
    }

    private static class CacheDiff {
        final public int incIdx;
        final public int decIdx;

        public CacheDiff(int inc, int dec) {
            this.incIdx = inc;
            this.decIdx = dec;
        }
    }

    private static class CacheState {
        private final int eventNum;
        private final int[] quotas;
        private final double avgPen;
        private final double hitRatio;

        private CacheState(int eventNum,
                           int[] quotas,
                           double avgPen,
                           double hitRatio) {
            this.eventNum = eventNum;
            this.quotas = Arrays.copyOf(quotas, quotas.length);
            this.avgPen = avgPen;
            this.hitRatio = hitRatio;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();

            sb.append(eventNum);
            sb.append(',');

            for (int quota : quotas) {
                sb.append(quota);
                sb.append(',');
            }

            sb.append(String.format("%.2f,", avgPen));
            sb.append(String.format("%.2f", hitRatio));

            return sb.toString();
        }
    }
}
