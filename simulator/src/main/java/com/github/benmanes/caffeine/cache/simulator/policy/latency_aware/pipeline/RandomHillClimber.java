package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.typesafe.config.Config;

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
import java.util.Collections;
import java.util.List;

@Policy.PolicySpec(name = "latency-aware.RHC")
public class RandomHillClimber implements Policy {
    private final static boolean DEBUG = true;
    @Nullable private PrintWriter writer = null;

    private final PipelinePolicy mainPipeline;

    private final PolicyStats stats;
    private final int blockCount;

    private final int adaptionTimeframe;
    private int opsSinceAdaption = 0;

    public RandomHillClimber(Config config) {
        var hillClimberSettings = new RandomHillSettings(config);
        mainPipeline = new PipelinePolicy(config);
        blockCount = mainPipeline.blockCount();
        stats = new PolicyStats("RHC " + mainPipeline.generatePipelineName());
        adaptionTimeframe = (int) (hillClimberSettings.adaptionMultiplier()
                                   * mainPipeline.cacheCapacity());

        if (DEBUG) {
             writer = prepareFileWriter();
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
        List<Integer> idxes = new ArrayList<>(blockCount);
        for (int idx = 0; idx < blockCount; ++idx) {
            idxes.add(idx);
        }
        Collections.shuffle(idxes);

        int incIdx = idxes.get(0);
        int decIdx = idxes.get(1);

        if (this.mainPipeline.canExtend(incIdx) && this.mainPipeline.canShrink(decIdx)) {
            this.mainPipeline.moveQuantum(incIdx, decIdx);
            var currState = this.mainPipeline.getCurrentState();
            var currCacheState = new CacheState(eventNum, currState.quotas, this.mainPipeline.stats().averagePenalty());

            if (DEBUG && writer != null) {
                writer.println(currCacheState.toString());
                writer.flush();
            }
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
            FileWriter fwriter = new FileWriter("/tmp/RHC-" + currentTime.format(timeFormatter) + ".dump", StandardCharsets.UTF_8);
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

    protected static class RandomHillSettings extends BasicSettings {
        private final static String BASE_PATH = "full-ghost-hill-climber";

        public RandomHillSettings(Config config) { super(config); }

        public double adaptionMultiplier() { return config().getDouble(BASE_PATH + ".adaption-multiplier"); }
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
