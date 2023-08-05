package com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.ConsoleColors;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.GhostHillClimberTinyLfuPolicy;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.ResizableWindowCostAwareWithBurstinessBlockPolicy;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.ResizableWindowCostAwareWithBurstinessBlockPolicy.BlockType;
import com.google.common.base.Stopwatch;
import com.typesafe.config.Config;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.System.Logger;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Policy.PolicySpec(name = "sketch.AdaptiveCAWithBB")
public class AdaptiveCAWithBurstBlockPolicy implements Policy {
    private final static boolean DEBUG = true;
    private final static Logger logger = System.getLogger(GhostHillClimberTinyLfuPolicy.class.getSimpleName());
    private final static int NUM_OF_POSSIBLE_CACHES = 6;

    private final static ResizableWindowCostAwareWithBurstinessBlockPolicy DUMMY = new ResizableWindowCostAwareWithBurstinessBlockPolicy.Dummy();
    private final AdaptiveCAWithBBStats stats;
    private CacheState currCapacityState;

    final private int adaptionTimeframe;
    private int opsSinceAdaption;
    private int adaptionNumber = 0;
    private List<CacheState> capacityHistory;

    private final ResizableWindowCostAwareWithBurstinessBlockPolicy[] ghostCaches;

    private final ResizableWindowCostAwareWithBurstinessBlockPolicy mainCache;

    public AdaptiveCAWithBurstBlockPolicy(Config config) {
        try {
            var settings = new AdaptiveCAWithBBSettings(config);
            int windowQuota = settings.lruQuota();
            int protectedQuota = settings.protectedQuota();
            int probationQuota = settings.probationQuota();
            int burstQuota = settings.bcQuota();
            int quantaCount = settings.numOfQuanta();


            Assert.assertCondition(windowQuota >= 0 && probationQuota >= 0 && protectedQuota >= 0 && burstQuota >= 0,
                                   () -> String.format("Quotas must be non-negative : Window: %d, Probation: %d, Protected: %d, Burst: %d",
                                                       windowQuota,
                                                       probationQuota,
                                                       protectedQuota,
                                                       burstQuota));
            Assert.assertCondition(windowQuota + probationQuota + protectedQuota + burstQuota == quantaCount,
                                   () -> String.format("Invalid settings - sum of quota mismatch %d vs %d",
                                                       windowQuota + probationQuota + protectedQuota + burstQuota,
                                                       quantaCount));

            int quantumSize = (int) (settings.maximumSize() / quantaCount);
            this.adaptionTimeframe = (int) (settings.adaptionMultiplier() * settings.maximumSize());

            stats = new AdaptiveCAWithBBStats("sketch.FullGhostHillClimber (LRU:%d, LFU: %d, BC:%d)",
                                              windowQuota,
                                              protectedQuota + probationQuota,
                                              burstQuota);

            mainCache = new ResizableWindowCostAwareWithBurstinessBlockPolicy(windowQuota,
                                                                              probationQuota,
                                                                              protectedQuota,
                                                                              burstQuota,
                                                                              quantumSize,
                                                                              config,
                                                                              "main");

            stats.attachMainStats(mainCache.stats());

            ghostCaches = new ResizableWindowCostAwareWithBurstinessBlockPolicy[NUM_OF_POSSIBLE_CACHES];
            createGhostCaches();

            currCapacityState = new CacheState(this, 0, windowQuota, probationQuota + protectedQuota, burstQuota);
            capacityHistory = new ArrayList<>();
            capacityHistory.add(currCapacityState);

            if (DEBUG) {
                logger.log(Logger.Level.INFO,
                           ConsoleColors.majorInfoString(
                                   "Created Adaptive CA with BB; Starting quota: LRU: %d LFU: %d BC: %d\tQuantum Size: %d",
                                   windowQuota,
                                   probationQuota + protectedQuota,
                                   burstQuota,
                                   quantumSize));
            }

            finished();
        } catch (RuntimeException e) {
            e.printStackTrace();
            System.exit(1);
            throw new IllegalStateException(e.getMessage());
        }
    }

    private void createGhostCaches() {
        if (DEBUG) {
            logger.log(Logger.Level.INFO,
                       ConsoleColors.infoString("creating ghost caches, adaption number %d", adaptionNumber));
        }

        for (int i = 0; i < NUM_OF_POSSIBLE_CACHES; ++i) {
            CacheConfiguration configuration = CacheConfiguration.fromIndex(i);
            setGhostCache(i, configuration.increment(), configuration.decrement(), configuration.getName());
        }
    }

    private boolean checkIfChangePossible(BlockType increase, BlockType decrease) {
        boolean condition;
        switch (increase) {
            case BC:
                condition = mainCache.canExtendBC();
                break;
            case LFU:
                condition = mainCache.canExtendLFU();
                break;
            case LRU:
                condition = mainCache.canExtendLRU();
                break;
            default:
                throw new IllegalStateException("No such block type");
        }

        if (condition) {
            switch (decrease) {
                case BC:
                    condition = mainCache.canShrinkBC();
                    break;
                case LFU:
                    condition = mainCache.canShrinkLFU();
                    break;
                case LRU:
                    condition = mainCache.canShrinkLRU();
                    break;
                default:
                    throw new IllegalStateException("No such block type");
            }
        }

        return condition;
    }

    private void setGhostCache(int i, BlockType increase, BlockType decrease, String name) {
        if (checkIfChangePossible(increase, decrease)) {
            this.ghostCaches[i] = mainCache.createGhostCopy(name);
            this.ghostCaches[i].changeSizes(increase, decrease);
        } else {
            this.ghostCaches[i] = DUMMY;
        }
    }

    @Override
    public void record(AccessEvent event) {
        final double hitPenaltyBefore = stats().hitPenalty();
        final double delayedHitPenaltyBefore = stats().delayedHitPenalty();
        final double missPenaltyBefore = stats().missPenalty();
        this.mainCache.record(event);
        for (int i = 0; i < NUM_OF_POSSIBLE_CACHES; ++i) {
            this.ghostCaches[i].record(event);
        }

        ++opsSinceAdaption;

        if (opsSinceAdaption >= adaptionTimeframe && mainCache.isFull()) {
            opsSinceAdaption = 0;
            adapt(event.eventNum());
        }

        final double hitPenaltyAfter = stats().hitPenalty();
        final double delayedHitPenaltyAfter = stats().delayedHitPenalty();
        final double missPenaltyAfter = stats().missPenalty();

        Assert.assertCondition((hitPenaltyAfter > hitPenaltyBefore && delayedHitPenaltyAfter == delayedHitPenaltyBefore && missPenaltyAfter == missPenaltyBefore)
                               || (hitPenaltyAfter == hitPenaltyBefore && delayedHitPenaltyAfter > delayedHitPenaltyBefore && missPenaltyAfter == missPenaltyBefore)
                               || (hitPenaltyAfter == hitPenaltyBefore && delayedHitPenaltyAfter == delayedHitPenaltyBefore && missPenaltyAfter > missPenaltyBefore),
                               () -> String.format("No stats update: Before: %.2f %.2f %.2f After: %.2f %.2f %.2f",
                                                   hitPenaltyBefore,
                                                   delayedHitPenaltyBefore,
                                                   missPenaltyBefore,
                                                   hitPenaltyAfter,
                                                   delayedHitPenaltyAfter,
                                                   missPenaltyAfter));
        if (opsSinceAdaption > 0) {
            validateStats();
        }

        finished();
    }

    private void validateStats() {
        for (int i = 0; i < NUM_OF_POSSIBLE_CACHES; ++i) {
            if (this.ghostCaches[i] != DUMMY) {
                final int idx = i;
                final double genAvgPen = this.ghostCaches[i].stats().averagePenalty();
                final double timeframeAvgPen = this.ghostCaches[i].timeframeStats().timeframeAveragePenalty();
                Assert.assertCondition(this.ghostCaches[i].timeframeStats().timeframeOperationNumber() > 0,
                                       () -> String.format("zero operations on cache %d: %s", idx, CacheConfiguration.fromIndex(idx)));
                Assert.assertCondition(genAvgPen == timeframeAvgPen,
                                       () -> String.format("penalty mismatch on cache %d: %s: total: %.2f timeframe: %.2f",
                                                           idx,
                                                           CacheConfiguration.fromIndex(idx),
                                                           genAvgPen,
                                                           timeframeAvgPen));
            }
        }
    }

    private void adapt(int eventNum) {
        validateStats();
        final var mainCacheTimeframeStats = this.mainCache.timeframeStats();
        final double currLruBenefit = mainCacheTimeframeStats.timeframeLruBenefit();
        final double currLfuBenefit = mainCacheTimeframeStats.timeframeLfuBenefit();
        final double currBcBenefit = mainCacheTimeframeStats.timeframeBcBenefit();

        Adaption adaption = new Adaption(CacheConfiguration.MAIN,
                                         this.mainCache.timeframeStats().timeframeAveragePenalty());

        for (int i = 0; i < NUM_OF_POSSIBLE_CACHES; ++i) {
            final var ghostTimeframeStats = this.ghostCaches[i].timeframeStats();
            final double avgPenalty = ghostTimeframeStats.timeframeAveragePenalty();
            final CacheConfiguration currConf = CacheConfiguration.fromIndex(i);

            final int idx = i;
            if (DEBUG && this.ghostCaches[i] != DUMMY) {

                StringBuilder sb = new StringBuilder();
                sb.append(currConf.name());
                sb.append(':');
                double expansionBenefit = 0;
                double shrinkCost = 0;
                switch (currConf.increment) {
                    case LRU:
                        expansionBenefit = ghostTimeframeStats.timeframeLruBenefit() - currLruBenefit;
                        break;
                    case LFU:
                        expansionBenefit = ghostTimeframeStats.timeframeLfuBenefit() - currLfuBenefit;
                        break;
                    case BC:
                        expansionBenefit = ghostTimeframeStats.timeframeBcBenefit() - currBcBenefit;
                        break;
                    case NONE:
                    default:
                        throw new IllegalStateException("Bad increment type");

                }

                switch (currConf.decrement) {
                    case LRU:
                        shrinkCost = currLruBenefit - ghostTimeframeStats.timeframeLruBenefit();
                        break;
                    case LFU:
                        shrinkCost = currLfuBenefit - ghostTimeframeStats.timeframeLfuBenefit();
                        break;
                    case BC:
                        shrinkCost = currBcBenefit - ghostTimeframeStats.timeframeBcBenefit();
                        break;
                    case NONE:
                    default:
                        throw new IllegalStateException("Bad decrement type");
                }

                sb.append('\t');
                sb.append("EB inc: ");
                sb.append(currConf.increment);
                sb.append(": ");
                sb.append(String.format("%.2e", expansionBenefit));
                sb.append('\t');
                sb.append("SC dec: ");
                sb.append(currConf.decrement);
                sb.append(": ");
                sb.append(String.format("%.2e", shrinkCost));
                logger.log(Logger.Level.INFO, ConsoleColors.minorInfoString("%s: avg Pen. %.2f, %s",
                                                                            CacheConfiguration.fromIndex(idx),
                                                                            avgPenalty,
                                                                            sb.toString()));
            }

            if (avgPenalty < adaption.avgPenalty()) {
                adaption = new Adaption(currConf, avgPenalty);
            }

            this.ghostCaches[i].resetStats();
        }

        if (DEBUG) {
            logger.log(Logger.Level.DEBUG,
                       ConsoleColors.colorString("===========================", ConsoleColors.YELLOW));
        }

        if (adaption.increment() != BlockType.NONE && adaption.decrement() != BlockType.NONE) {
            if (DEBUG) {
                logger.log(Logger.Level.INFO, ConsoleColors.majorInfoString("main: increasing %s over %s in adaption number %d, avg. before: %.2f, lowest: %.2f total: %.2f",
                                                                            adaption.increment().name(),
                                                                            adaption.decrement().name(),
                                                                            adaptionNumber,
                                                                            this.mainCache.timeframeStats()
                                                                                          .timeframeAveragePenalty(),
                                                                            adaption.avgPenalty,
                                                                            this.stats.averagePenalty()));
            }

            var newCapacityState = CacheState.fromStateAndAdaptation(this,
                                                                     eventNum,
                                                                     currCapacityState,
                                                                     adaption);

            capacityHistory.add(newCapacityState);
            currCapacityState = newCapacityState;

            this.mainCache.changeSizes(adaption.increment(), adaption.decrement());
            this.mainCache.resetTimeframeStats();

            if (DEBUG) {
                logger.log(Logger.Level.INFO,
                           ConsoleColors.majorInfoString(
                                   "State after adaption num %d: LRU: %d LFU: %d BC: %d",
                                   this.adaptionNumber,
                                   mainCache.lruCapacity(),
                                   mainCache.lfuCapacity(),
                                   mainCache.bcCapacity()));
            }

            createGhostCaches();
        } else {
            var newCapacityState = new CacheState(this,
                                                  eventNum,
                                                  currCapacityState.lruCapacity,
                                                  currCapacityState.lfuCapacity,
                                                  currCapacityState.bcCapacity);

            capacityHistory.add(newCapacityState);
            currCapacityState = newCapacityState;
        }

        ++this.adaptionNumber;
    }

    @Override
    public void finished() {
        mainCache.validateCapacity();
        mainCache.validateSize();

        for (int i = 0; i < NUM_OF_POSSIBLE_CACHES; ++i) {
            if (ghostCaches[i] != DUMMY) {
                ghostCaches[i].validateCapacity();
                ghostCaches[i].validateSize();
            }
        }
    }

    private PrintWriter prepareFileWriter() {
        LocalDateTime currentTime = LocalDateTime.now(ZoneId.systemDefault());
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("dd-MM-HH-mm-ss");
        PrintWriter writer = null;
        try {
            FileWriter fwriter = new FileWriter("/tmp/naive-adaptive-adaptions-" + currentTime.format(timeFormatter) + ".dump", StandardCharsets.UTF_8);
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
        if (DEBUG) {
            PrintWriter writer = prepareFileWriter();

            for (var state: capacityHistory) {
                writer.println(state.toString());
            }

            writer.close();
        }
    }

    @Override
    public PolicyStats stats() {
        return stats;
    }

    @Override
    public boolean isPenaltyAware() {
        return true;
    }

    public static Policy policy(Config config) {
        return new AdaptiveCAWithBurstBlockPolicy(config);
    }

    protected static class AdaptiveCAWithBBStats extends PolicyStats {
        private PolicyStats mainStats = null;

        public AdaptiveCAWithBBStats(String format, Object... args) {super(format, args);}

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
            return this.name;
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
        public double delayedHitPenalty() {
            return mainStats.delayedHitPenalty();
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
        public double delayedHitRate() {return mainStats.delayedHitRate();}

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

    protected static final class AdaptiveCAWithBBSettings extends BasicSettings {
        final static String BASE_PATH = "adaptive-ca-bb";

        public AdaptiveCAWithBBSettings(Config config) {
            super(config);
        }

        public int probationQuota() {
            return config().getInt(BASE_PATH + ".quota-probation");
        }

        public int protectedQuota() {
            return config().getInt(BASE_PATH + ".quota-protected");
        }

        public int lruQuota() {
            return config().getInt(BASE_PATH + ".quota-window");
        }

        public int bcQuota() {
            return config().getInt(BASE_PATH + ".quota-bc");
        }

        public int numOfQuanta() {return config().getInt(BASE_PATH + ".num-of-quanta");}

        public double adaptionMultiplier() {
            return config().getDouble(BASE_PATH + ".adaption-multiplier");
        }
    }

    private static class Adaption {
        private final CacheConfiguration configuration;
        private final double avgPenalty;

        private Adaption(CacheConfiguration configuration, double avgPenalty) {
            this.configuration = configuration;
            this.avgPenalty = avgPenalty;
        }

        private BlockType decrement() {
            return configuration.decrement();
        }

        private BlockType increment() {
            return configuration.increment();
        }

        private double avgPenalty() {
            return avgPenalty;
        }
    }

    private enum CacheConfiguration {
        BIGGER_LFU_SMALLER_BC(0, "+LFU-BC", BlockType.LFU, BlockType.BC),
        BIGGER_LFU_SMALLER_LRU(1, "+LFU-LRU", BlockType.LFU, BlockType.LRU),
        BIGGER_LRU_SMALLER_BC(2, "+LRU-BC", BlockType.LRU, BlockType.BC),
        BIGGER_LRU_SMALLER_LFU(3, "+LRU-LFU", BlockType.LRU, BlockType.LFU),
        BIGGER_BC_SMALLER_LRU(4, "+BC-LRU", BlockType.BC, BlockType.LRU),
        BIGGER_BC_SMALLER_LFU(5, "+BC-LFU", BlockType.BC, BlockType.LFU),
        MAIN(Integer.MAX_VALUE, "main", BlockType.NONE, BlockType.NONE);

        final private int index;
        final private String name;
        final private BlockType increment;
        final private BlockType decrement;

        private CacheConfiguration(int num, String name, BlockType increment, BlockType decrement) {
            this.index = num;
            this.name = name;
            this.increment = increment;
            this.decrement = decrement;
        }

        @SuppressWarnings("unused")
        public int getIndex() {return index;}

        public String getName() {return name;}

        private BlockType increment() {
            return increment;
        }

        private BlockType decrement() {
            return decrement;
        }

        public static CacheConfiguration fromIndex(int index) {
            switch (index) {
                case 0:
                    return BIGGER_LFU_SMALLER_BC;
                case 1:
                    return BIGGER_LFU_SMALLER_LRU;
                case 2:
                    return BIGGER_LRU_SMALLER_BC;
                case 3:
                    return BIGGER_LRU_SMALLER_LFU;
                case 4:
                    return BIGGER_BC_SMALLER_LRU;
                case 5:
                    return BIGGER_BC_SMALLER_LFU;
                default:
                    throw new IllegalStateException("No such configuration");
            }
        }
    }

    private static class CacheState {
        final private int eventNum;
        final private int lruCapacity;
        final private int lfuCapacity;
        final private int bcCapacity;
        final private double hitRate;
        final private double avgPenalty;

        private CacheState(AdaptiveCAWithBurstBlockPolicy policy,
                           int eventNum,
                           int lruCapacity,
                           int lfuCapacity,
                           int bcCapacity) {
            this.eventNum = eventNum;
            this.lruCapacity = lruCapacity;
            this.lfuCapacity = lfuCapacity;
            this.bcCapacity = bcCapacity;
            this.hitRate = policy.stats.hitRate();
            this.avgPenalty = policy.stats.averagePenalty();
        }

        protected static CacheState fromStateAndAdaptation(AdaptiveCAWithBurstBlockPolicy policy,
                                                    int eventNum,
                                                    CacheState state,
                                                    Adaption adaption) {
            int currentLRU = state.lruCapacity();
            int currentLFU = state.lfuCapacity();
            int currentBC = state.bcCapacity();

            switch (adaption.increment()) {
                case LRU:
                    ++currentLRU;
                    break;
                case LFU:
                    ++currentLFU;
                    break;
                case BC:
                    ++currentBC;
                    break;
                case NONE:
                default:
                    throw new IllegalStateException("Bad adaption");
            }

            switch (adaption.decrement()) {
                case LRU:
                    --currentLRU;
                    break;
                case LFU:
                    --currentLFU;
                    break;
                case BC:
                    --currentBC;
                    break;
                case NONE:
                default:
                    throw new IllegalStateException("Bad adaption");
            }

            return new CacheState(policy, eventNum, currentLRU, currentLFU, currentBC);
        }

        private int lruCapacity() {
            return lruCapacity;
        }

        private int lfuCapacity() {
            return lfuCapacity;
        }

        private int bcCapacity() {
            return bcCapacity;
        }

        @Override
        public String toString() {
            return String.format("%d,%d,%d,%d,%.2f,%.2f",
                                 eventNum,
                                 lruCapacity,
                                 lfuCapacity,
                                 bcCapacity,
                                 hitRate * 100,
                                 avgPenalty);
        }
    }
}
