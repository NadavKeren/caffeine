package com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing.Pipeline;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.ConsoleColors;
import com.github.benmanes.caffeine.cache.simulator.UneditableLatencyEstimatorProxy;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.EntryData;
import com.github.benmanes.caffeine.cache.simulator.policy.LatencyEstimator;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.LatestLatencyEstimator;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.MovingAverageBurstLatencyEstimator;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.doubles.DoubleIntImmutablePair;

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
import java.util.Comparator;
import java.util.List;
import java.lang.System.Logger;

@Policy.PolicySpec(name = "sketch.AdaptivePipeline")
public class AdaptivePipelineCache implements Policy {
    private static boolean DEBUG = true;

    private static Logger logger = DEBUG ? System.getLogger(AdaptivePipelineCache.class.getName()) : null;

    final private int totalQuota;
    final private int quantumSize;
    final private int cacheCapacity;

    final private int adaptionTimeframe;
    private int opsSinceAdaption = 0;
    private int adaptionNumber = 0;

    final int blockCount;
    final private LatencyEstimator<Long> latencyEstimator;
    final private LatencyEstimator<Long> burstEstimator;
    final PipelineBlock[] blocks;
    final int[] blockQuotas;
    final String[] types;
    final private AdaptivePipelineStats stats;

    // TODO: find a way to share these without losing the generic value of this class


    final private List<StateSnapshot> blockQuotaHistory;

    public AdaptivePipelineCache(Config config) {
        var settings = new AdaptivePipelineSettings(config);


        totalQuota = settings.numOfQuanta();
        quantumSize = settings.quantumSize();
        cacheCapacity = totalQuota * quantumSize;

        adaptionTimeframe = (int) (settings.adaptionMultiplier() * cacheCapacity);

        blockCount = settings.numOfBlocks();
        blocks = new PipelineBlock[blockCount];
        blockQuotas = new int[blockCount];

        final var blockConfigs = settings.blocksConfigs();
        types = new String[blockCount];
        latencyEstimator = new LatestLatencyEstimator<>();
        burstEstimator = new MovingAverageBurstLatencyEstimator<>(settings.agingWindowSize(),
                                                                  settings.ageSmoothFactor(),
                                                                  settings.numOfPartitions());

        for (int idx = 0; idx < blockCount; ++idx) {
            final Config currConfig = blockConfigs.get(idx);
            final PipelineBlockSettings blockSettings = new PipelineBlockSettings(currConfig);
            final int quota = blockSettings.quota();
            final String type = blockSettings.type();

            blocks[idx] = createBlock(type, quota, config, currConfig);
            blockQuotas[idx] = quota;
            types[idx] = type;
        }

        final int accumulatedQuota = Arrays.stream(blockQuotas).sum();

        Assert.assertCondition(accumulatedQuota == totalQuota,
                               () -> String.format("Quota mismatch, expected: %d, accumulated %d",
                                                   totalQuota,
                                                   accumulatedQuota));


        String name = buildName(types);
        stats = new AdaptivePipelineStats(name);

        if (DEBUG) {
            logger.log(Logger.Level.INFO, ConsoleColors.majorInfoString("Created: %s", name));
            Assert.assertCondition(cacheCapacity == settings.maximumSize(), () -> String.format("Size mismatch: expected: %d got: %d", settings.maximumSize(), cacheCapacity));
        }

        blockQuotaHistory = new ArrayList<>();
        blockQuotaHistory.add(new StateSnapshot(0));
    }

    private String buildName(String[] types) {
        final StringBuilder nameBuilder = new StringBuilder();

        nameBuilder.append("Adaptive Pipeline: Quantum Size: ");
        nameBuilder.append(quantumSize);
        nameBuilder.append(" blocks: (");
        for (int idx = 0; idx < blockCount; ++idx) {
            nameBuilder.append("[");
            nameBuilder.append(types[idx]);
            nameBuilder.append(": ");
            nameBuilder.append(blockQuotas[idx]);
            nameBuilder.append("]");

            if (idx < blockCount - 1) {
                nameBuilder.append(", ");
            }
        }
        nameBuilder.append(")");

        return nameBuilder.toString();
    }

    private PipelineBlock createBlock(String type, int quota, Config generalConfig, Config blockConfig) {
        PipelineBlock block = null;

        switch (type) {
            case "LRU":
                block = new LruBlock(blockConfig,
                        new UneditableLatencyEstimatorProxy<>(latencyEstimator),
                        quantumSize,
                        quota);
                break;
            case "BC":
                block = new BurstCache(new UneditableLatencyEstimatorProxy<>(burstEstimator), quantumSize, quota);
                break;
            case "LFU":
                block = new LfuBlock(generalConfig, blockConfig, latencyEstimator, quantumSize, quota);
                break;
            default:
                throw new IllegalStateException("No such type: " + type);
        }

        return block;
    }

    @Override
    public void record(AccessEvent event) {
        EntryData entry = null;

        for (int idx = 0; idx < blockCount; ++idx) {
            EntryData blockRes = blocks[idx].getEntry(event.key());

            if (entry == null && blockRes != null) {
                entry = blockRes;
                stats.recordHitAtBlock(idx);
            }
        }

        if (entry == null) {
            onMiss(event);
        } else {
            recordAccordingToAvailability(entry, event);
        }

        ++opsSinceAdaption;

        if (opsSinceAdaption >= adaptionTimeframe && size() >= cacheCapacity) {
            opsSinceAdaption = 0;
            adapt(event.eventNum());
        }

//        validate();
    }

    private void onMiss(AccessEvent event) {
        EntryData newItem = new EntryData(event);

        latencyEstimator.record(event.key(), event.missPenalty(), event.getArrivalTime());
        burstEstimator.record(event.key(), event.missPenalty(), event.getArrivalTime());

        for (int idx = 0; idx < blockCount; ++idx) {
            if (newItem != null) {
                newItem = blocks[idx].insert(newItem);
            }
        }

        if (newItem != null) {
            stats.recordEviction();
        }

        stats.recordMiss();
        stats.recordMissPenalty(event.missPenalty());
        event.changeEventStatus(AccessEvent.EventStatus.MISS);
    }

    private void recordAccordingToAvailability(EntryData entry, AccessEvent currEvent) {
        boolean isAvailable = entry.event().isAvailableAt(currEvent.getArrivalTime());

        if (isAvailable) {
            currEvent.changeEventStatus(AccessEvent.EventStatus.HIT);
            stats.recordHit();
            stats.recordHitPenalty(currEvent.hitPenalty());
            burstEstimator.addValueToRecord(currEvent.key(), 0, currEvent.getArrivalTime());

            latencyEstimator.recordHit(currEvent.hitPenalty());
            burstEstimator.recordHit(currEvent.hitPenalty());
        } else {
            currEvent.changeEventStatus(AccessEvent.EventStatus.DELAYED_HIT);
            currEvent.setDelayedHitPenalty(entry.event().getAvailabilityTime());
            stats.recordDelayedHitPenalty(currEvent.delayedHitPenalty());
            stats.recordDelayedHit();
            latencyEstimator.addValueToRecord(currEvent.key(),
                                              currEvent.delayedHitPenalty(),
                                              currEvent.getArrivalTime());
            burstEstimator.addValueToRecord(currEvent.key(), currEvent.delayedHitPenalty(), currEvent.getArrivalTime());
        }
    }

    private void adapt(int eventNum) {
        List<Pair<Double, Integer>> sortedByExpansionBenefit = new ArrayList<>(blockCount);
        List<Pair<Double, Integer>> sortedByShrinkCost = new ArrayList<>(blockCount);

        for (int idx = 0; idx < blockCount; ++idx) {

            double currentBenefit = blockQuotas[idx] < totalQuota ? blocks[idx].getExpansionBenefit() : 0;
            double currentCost = blockQuotas[idx] > 0 ? blocks[idx].getShrinkCost() : Double.MAX_VALUE;

            sortedByExpansionBenefit.add(new DoubleIntImmutablePair(currentBenefit, idx));
            sortedByShrinkCost.add(new DoubleIntImmutablePair(currentCost, idx));


            if (DEBUG) {
                logger.log(Logger.Level.INFO, ConsoleColors.minorInfoString("%s: EB: %.2f SC: %.2f", this.types[idx], currentBenefit, currentCost));
            }
        }

        Collections.sort(sortedByExpansionBenefit, Comparator.comparingInt(Pair::right));
        Collections.sort(sortedByShrinkCost, (l1, l2) -> (l2.right() - l1.right()));

        int incIdx = 0;
        int decIdx = 0;
        boolean wasFound = false;

        for (int i = 0; i < blockCount && !wasFound; ++i) {
            for (int j = 0; j < blockCount && !wasFound; ++j) {
                var currMaxExp = sortedByExpansionBenefit.get(i);
                var currMinCost = sortedByShrinkCost.get(j);
                if (!currMaxExp.right().equals(currMinCost.right())
                    && currMaxExp.left().compareTo(currMinCost.left()) > 0) {
                    incIdx = currMaxExp.right();
                    decIdx = currMinCost.right();
                    wasFound = true;
                }
            }
        }


        if (wasFound) {
            if (DEBUG) {
                logger.log(Logger.Level.INFO, ConsoleColors.infoString("max difference at: %s and %s", types[incIdx], types[decIdx]));
            }

            Assert.assertCondition(blockQuotas[incIdx] < totalQuota, "Illegal Increment requested");
            Assert.assertCondition(blockQuotas[decIdx] > 0, "Illegal Decrement requested");

            List<EntryData> items = blocks[decIdx].decreaseSize();

            blocks[incIdx].increaseSize(items);

            ++blockQuotas[incIdx];
            --blockQuotas[decIdx];

            if (DEBUG) {
                logger.log(System.Logger.Level.INFO,
                           ConsoleColors.majorInfoString("Event: %d, Adaption %d: Inc: %s\tDec: %s",
                                                         eventNum,
                                                         adaptionNumber,
                                                         types[incIdx],
                                                         types[decIdx]));
            }
        }

        blockQuotaHistory.add(new StateSnapshot(eventNum));

        for (int idx = 0; idx < blockCount; ++idx) {
            blocks[idx].resetStats();
        }

        ++adaptionNumber;

        validate();
    }

    @Override
    public void finished() {
        validate();
    }

    public int size() {
        int accumulatedSize = 0;
        for (PipelineBlock block : blocks) {
            accumulatedSize += block.size();
        }

        return accumulatedSize;
    }

    private void validate() {
        int accumulatedSize = 0;
        for (int idx = 0; idx < blockCount; ++idx) {
            PipelineBlock block = blocks[idx];
            block.validate();
            accumulatedSize += block.size();
            final int blockIdx = idx;
            Assert.assertCondition(block.size() <= block.capacity(),
                                   () -> String.format("block %s exceeding its capacity", types[blockIdx]));
            Assert.assertCondition(block.capacity() == blockQuotas[idx] * quantumSize,
                                   () -> String.format("block %s has wrong capacity", types[blockIdx]));

        }

        final int finalAccumulatedSize = accumulatedSize;
        Assert.assertCondition(accumulatedSize <= cacheCapacity,
                               () -> String.format("Size overflow: Expected at most: %d, Got: %d",
                                                   cacheCapacity,
                                                   finalAccumulatedSize));
    }

    @Override
    public PolicyStats stats() {
        return this.stats;
    }

    @Override
    public boolean isPenaltyAware() {
        return true;
    }

    private PrintWriter prepareFileWriter() {
        LocalDateTime currentTime = LocalDateTime.now(ZoneId.systemDefault());
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("dd-MM-HH-mm-ss");
        PrintWriter writer = null;
        try {
            FileWriter fwriter = new FileWriter("/tmp/pipeline-cache-" + currentTime.format(timeFormatter) + ".dump", StandardCharsets.UTF_8);
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

            for (StateSnapshot state: blockQuotaHistory) {
                writer.println(state);
            }

            writer.close();
        }
    }

    protected static final class AdaptivePipelineSettings extends BasicSettings {
        final static String BASE_PATH = "adaptive-pipeline";
        final static String CONFIGS_PATH = BASE_PATH + ".blocks";

        public AdaptivePipelineSettings(Config config) {
            super(config);
        }

        public int numOfBlocks() { return config().getInt(BASE_PATH + ".num-of-blocks"); }

        public int numOfQuanta() { return config().getInt(BASE_PATH + ".num-of-quanta"); }

        public int quantumSize() { return config().getInt(BASE_PATH + ".quantum-size"); }

        public int agingWindowSize() {return config().getInt(BASE_PATH + ".burst.aging-window-size");}

        public double ageSmoothFactor() {return config().getDouble(BASE_PATH + ".burst.age-smoothing");}

        public int numOfPartitions() {return config().getInt(BASE_PATH + ".burst.number-of-partitions");}

        public List<Config> blocksConfigs() {
            final int numOfBlocks = numOfBlocks();
            final List<Config> configs = new ArrayList<>(numOfBlocks);

            for (int i = 0; i < numOfBlocks; ++i) {
                try {
                    configs.add(config().getConfig(CONFIGS_PATH + "." + i));
                } catch (ConfigException e) {
                    System.err.println(e.getMessage());
                    e.printStackTrace();
                    System.exit(1);
                }
            }

            return configs;
        }

        public double adaptionMultiplier() {
            return config().getDouble(BASE_PATH + ".adaption-multiplier");
        }
    }

    public static class PipelineBlockSettings {
        final private Config config;

        public PipelineBlockSettings(Config config) {
            this.config = config;
        }

        public String type() {
            return config.getString("type");
        }

        public int quota() {
            return config.getInt("quota");
        }
    }

    public class AdaptivePipelineStats extends PolicyStats {
        int[] hitCountsPerBlock;

        public AdaptivePipelineStats(String format, Object... args) {
            super(format, args);
            hitCountsPerBlock = new int[blockCount];
        }

        public void recordHitAtBlock(int idx) {
            ++hitCountsPerBlock[idx];
        }
    }

    private class StateSnapshot {
        final int eventNum;
        final int[] quotas;
        final double hitRate;
        final double avgPenalty;

        public StateSnapshot(int eventNum) {
            this.eventNum = eventNum;
            this.quotas = getQuotaSnapshot();
            this.hitRate = stats.hitRate();
            this.avgPenalty = stats.averagePenalty();
        }

        private int[] getQuotaSnapshot() {
            int[] snapshot = new int[blockCount];

            System.arraycopy(blockQuotas, 0, snapshot, 0, blockCount);

            return snapshot;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(eventNum);
            sb.append(",");
            for (int idx = 0; idx < blockCount; ++idx) {
                sb.append(quotas[idx]);
                sb.append(",");
            }

            sb.append(String.format("%.2f,", hitRate * 100));
            sb.append(String.format("%.2f", avgPenalty));

            return sb.toString();
        }
    }
}
