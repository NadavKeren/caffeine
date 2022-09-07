package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.ConsoleColors;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.google.common.base.Stopwatch;
import com.typesafe.config.Config;

import java.util.List;
import java.util.Map;
import java.util.Set;

import java.lang.System.Logger;

import static java.util.stream.Collectors.toSet;

@Policy.PolicySpec(name = "sketch.GhostClimberTinyLFU")
public class GhostHillClimberTinyLfuPolicy implements Policy {
    private final static boolean DEBUG = true;
    private final static Logger logger = System.getLogger(GhostHillClimberTinyLfuPolicy.class.getSimpleName());

    private final static ResizeableWindowTinyLfuPolicy.Dummy DUMMY = new ResizeableWindowTinyLfuPolicy.Dummy();
    private final GhostClimberTinyLfuStats policyStats;
    private final ResizeableWindowTinyLfuPolicy mainCache;
    private ResizeableWindowTinyLfuPolicy biggerLFUCache;
    private ResizeableWindowTinyLfuPolicy smallerLFUCache;

    final private int adaptionTimeframe;
    private int opsSinceAdaption;

    public GhostHillClimberTinyLfuPolicy(int initialLfuQuota, Config config) {
        policyStats = new GhostClimberTinyLfuStats(name() + " (LFU: %d)", initialLfuQuota);

        var settings = new GhostTinyLfuSettings(config);
        int quantaSize = (int) settings.maximumSize() / settings.numOfQuanta();
        int maximumSize = (int) settings.maximumSize();

        this.mainCache = new ResizeableWindowTinyLfuPolicy(initialLfuQuota,
                                                           quantaSize,
                                                           maximumSize,
                                                           new WindowTinyLfuPolicy.WindowTinyLfuSettings(config),
                                                           "main");

        policyStats.attachMainStats(this.mainCache.stats());

        createGhostCaches();

        this.adaptionTimeframe = (int) (settings.adaptionMultiplier() * maximumSize);
        this.opsSinceAdaption = 0;

        if (DEBUG) {
            logger.log(Logger.Level.INFO,
                       "Created cache with %d, %d, %d",
                       this.mainCache.maxWindow,
                       this.mainCache.maxProbation
                       + this.mainCache.maxProtected,
                       this.mainCache.maximumSize);
        }
    }

    private void createGhostCaches() {
        if (mainCache.canExtendLFU()) {
            this.biggerLFUCache = ResizeableWindowTinyLfuPolicy.ghostCopyOf(mainCache, "BiggerLFU");
            this.biggerLFUCache.increaseLFU();
        } else {
            this.biggerLFUCache = DUMMY;
        }

        if (mainCache.canExtendLRU()) {
            this.smallerLFUCache = ResizeableWindowTinyLfuPolicy.ghostCopyOf(mainCache, "SmallerLFU");
            this.smallerLFUCache.decreaseLFU();
        } else {
            this.smallerLFUCache = DUMMY;
        }
    }

    @Override
    public void record(AccessEvent event) {
        this.mainCache.record(event);
        this.biggerLFUCache.record(event);
        this.smallerLFUCache.record(event);
        ++this.opsSinceAdaption;

        if (this.opsSinceAdaption >= adaptionTimeframe) {
            this.adapt();
            this.mainCache.finished();
        }
    }

    private void adapt() {
        final double smallerLFUHitRate = this.smallerLFUCache.getWindowStats().timeframeHitRate();
        final double higherLFUHitRate = this.biggerLFUCache.getWindowStats().timeframeHitRate();
        final double currentHitRate = this.mainCache.getWindowStats().timeframeHitRate();
        final double bestHitRate = Math.max(Math.max(smallerLFUHitRate, higherLFUHitRate), currentHitRate);

        final double delta = bestHitRate - currentHitRate;
        if (DEBUG) {
            logger.log(Logger.Level.DEBUG, ConsoleColors.colorString(String.format(
                    "Smaller LFU: %.2f%% Bigger LFU: %.2f%% Main: %.2f%%",
                    smallerLFUHitRate * 100,
                    higherLFUHitRate * 100,
                    currentHitRate * 100), ConsoleColors.WHITE_BOLD));
        }

        if (delta > 0) {
            if (bestHitRate == smallerLFUHitRate) {
                if (DEBUG) {
                    logger.log(Logger.Level.DEBUG,
                               ConsoleColors.colorString("Decreasing LFU", ConsoleColors.YELLOW_BOLD));
                }

                this.mainCache.decreaseLFU();
            } else {
                if (DEBUG) {
                    logger.log(Logger.Level.DEBUG,
                               ConsoleColors.colorString("Increasing LFU", ConsoleColors.YELLOW_BOLD));
                }

                this.mainCache.increaseLFU();
            }

            if (DEBUG) {
                logger.log(Logger.Level.INFO,
                           "%d, %d, %d",
                           this.mainCache.maxWindow,
                           this.mainCache.maxProbation + this.mainCache.maxProtected,
                           this.mainCache.maximumSize);
            }

            createGhostCaches();
        } else {
            this.smallerLFUCache.resetStats();
            this.biggerLFUCache.resetStats();
        }

        this.opsSinceAdaption = 0;
        this.mainCache.resetStats();
    }

    @Override
    public void finished() {
        mainCache.finished();
        biggerLFUCache.finished();
        smallerLFUCache.finished();
    }

    @Override
    public PolicyStats stats() {
        return this.policyStats;
    }

    public static Set<Policy> policies(Config config) {
        GhostTinyLfuSettings settings = new GhostTinyLfuSettings(config);

        return settings.initialLFUQuota().stream()
                       .map(initialLfuQuota -> new GhostHillClimberTinyLfuPolicy(initialLfuQuota, config))
                       .collect(toSet());
    }

    public static final class GhostTinyLfuSettings extends BasicSettings {
        final static String BASE_PATH = "ghost-hill-climber-tiny-lfu";

        public GhostTinyLfuSettings(Config config) {
            super(config);
        }

        public int numOfQuanta() {
            return config().getInt(BASE_PATH + ".num-of-quanta");
        }

        public List<Integer> initialLFUQuota() {
            return config().getIntList(BASE_PATH + ".initial-lfu-quota");
        }

        public double adaptionMultiplier() {
            return config().getDouble(BASE_PATH + ".adaption-multiplier");
        }
    }

    protected static class GhostClimberTinyLfuStats extends PolicyStats {
        private PolicyStats mainStats = null;

        public GhostClimberTinyLfuStats(String format, Object... args) {
            super(format, args);
        }

        public void attachMainStats(PolicyStats mainStats) {
            this.mainStats = mainStats;
        }

        @Override
        public Map<String, Metric> metrics() {
            return mainStats.metrics();
        }

        @Override
        public Stopwatch stopwatch() {
            return mainStats.stopwatch();
        }

        @Override
        public String name() {
            return mainStats.name();
        }

        @Override
        public long operationCount() {
            return mainStats.operationCount();
        }

        @Override
        public long hitsWeight() {
            return mainStats.hitsWeight();
        }

        @Override
        public double hitPenalty() {
            return mainStats.hitPenalty();
        }

        @Override
        public long missCount() {
            return mainStats.missCount();
        }

        @Override
        public long missesWeight() {
            return mainStats.missesWeight();
        }

        @Override
        public double missPenalty() {
            return mainStats.missPenalty();
        }

        @Override
        public long evictionCount() {
            return mainStats.evictionCount();
        }

        @Override
        public long requestCount() {
            return mainStats.requestCount();
        }

        @Override
        public long requestsWeight() {
            return mainStats.requestsWeight();
        }

        @Override
        public long admissionCount() {
            return mainStats.admissionCount();
        }

        @Override
        public long rejectionCount() {
            return mainStats.rejectionCount();
        }

        @Override
        public double totalPenalty() {
            return mainStats.totalPenalty();
        }

        @Override
        public double percentAdaption() {
            return mainStats.percentAdaption();
        }

        @Override
        public double hitRate() {
            return mainStats.hitRate();
        }

        @Override
        public double weightedHitRate() {
            return mainStats.weightedHitRate();
        }

        @Override
        public double missRate() {
            return mainStats.missRate();
        }

        @Override
        public double weightedMissRate() {
            return mainStats.weightedMissRate();
        }

        @Override
        public double admissionRate() {
            return mainStats.admissionRate();
        }

        @Override
        public double complexity() {
            return mainStats.complexity();
        }

        @Override
        public double averagePenalty() {
            return mainStats.averagePenalty();
        }

        @Override
        public double averageHitPenalty() {
            return mainStats.averageHitPenalty();
        }

        @Override
        public double averageMissPenalty() {
            return mainStats.averageMissPenalty();
        }
    }
}
