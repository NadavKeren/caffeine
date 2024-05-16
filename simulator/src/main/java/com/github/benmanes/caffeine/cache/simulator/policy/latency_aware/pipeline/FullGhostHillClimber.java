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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Policy.PolicySpec(name = "latency-aware.FGHC")
public class FullGhostHillClimber implements Policy {
    private final static boolean DEBUG = true;
    @Nullable private PrintWriter writer = null;

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
        stats = new PolicyStats(mainPipeline.generatePipelineName());
        adaptionTimeframe = (int) (hillClimberSettings.adaptionMultiplier()
                                   * mainPipeline.cacheCapacity());

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
                    if (mainPipeline.canExtend(inc) && mainPipeline.canShrink(dec)) {
                        cache = mainPipeline.createCopy();
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
            var currState = this.mainPipeline.getCurrentState();
            var currCacheState = new CacheState(eventNum, currState.quotas, currentAvg);

            if (DEBUG && writer != null) {
                writer.println(currCacheState.toString());
                writer.flush();
            }

            createGhostCaches();
        }
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
            FileWriter fwriter = new FileWriter("/tmp/FGHC-" + currentTime.format(timeFormatter) + ".dump", StandardCharsets.UTF_8);
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

        private CacheState(int eventNum,
                           int[] quotas,
                           double avgPen) {
            this.eventNum = eventNum;
            this.quotas = Arrays.copyOf(quotas, quotas.length);
            this.avgPen = avgPen;
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

            sb.append(String.format("%.2f", avgPen));

            return sb.toString();
        }
    }
}
