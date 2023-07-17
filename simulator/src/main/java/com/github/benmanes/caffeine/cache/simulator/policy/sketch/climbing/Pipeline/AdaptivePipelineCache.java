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
    final private AdaptivePipelineStats stats;

    // TODO: find a way to share these without losing the generic value of this class


    final private List<int[]> blockQuotaHistory;

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
        final String[] types = new String[blockCount];
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


        stats = new AdaptivePipelineStats(buildName(types));

        blockQuotaHistory = new ArrayList<>();
        blockQuotaHistory.add(getQuotaSnapshot());
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

        if (opsSinceAdaption >= adaptionTimeframe) {
            opsSinceAdaption = 0;
            adapt();
        }

        validate();
    }

    private void onMiss(AccessEvent event) {
        EntryData newItem = new EntryData(event);

        for (int idx = 0; idx < blockCount; ++idx) {
            if (newItem != null) {
                newItem = blocks[idx].insert(newItem);
            }
        }

        stats.recordMiss();
        stats.recordMissPenalty(event.missPenalty());
        event.changeEventStatus(AccessEvent.EventStatus.MISS);

        latencyEstimator.record(event.key(), event.missPenalty(), event.getArrivalTime());
        burstEstimator.record(event.key(), event.missPenalty(), event.getArrivalTime());
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

    private void adapt() {
        double maxBenefit = 0;
        double minCost = Double.MAX_VALUE;
        int maxBenefitIdx = -1;
        int minCostIdx = -1;

        for (int idx = 0; idx < blockCount; ++idx) {
            double currentBenefit = blocks[idx].getExpansionBenefit();

            if (currentBenefit > maxBenefit) {
                maxBenefit = currentBenefit;
                maxBenefitIdx = idx;
            }

            double currentCost = blocks[idx].getShrinkCost();

            if (currentCost < minCost) {
                minCost = currentCost;
                minCostIdx = idx;
            }
        }

        if (maxBenefit > minCost && maxBenefitIdx != minCostIdx) {
            List<EntryData> items = blocks[minCostIdx].decreaseSize();

            blocks[maxBenefitIdx].increaseSize(items);

            ++blockQuotas[maxBenefitIdx];
            --blockQuotas[minCostIdx];

            if (DEBUG) {
                logger.log(System.Logger.Level.INFO,
                           ConsoleColors.infoString("creating ghost caches, adaption number %d", adaptionNumber));

                blockQuotaHistory.add(getQuotaSnapshot());
            }
        }

        ++adaptionNumber;
    }

    @Override
    public void finished() {
        validate();
    }

    private void validate() {
        int accumulatedSize = 0;
        for (PipelineBlock block : blocks) {
            block.validate();
            accumulatedSize += block.size();
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

    private int[] getQuotaSnapshot() {
        int[] snapshot = new int[blockCount];

        System.arraycopy(blockQuotas, 0, snapshot, 0, blockCount);

        return snapshot;
    }

    @Override
    public void dump() {
        if (DEBUG) {
            PrintWriter writer = prepareFileWriter();

            for (int[] state: blockQuotaHistory) {
                for (int i = 0; i < state.length; ++i) {
                    writer.print(state[i]);
                    if (i < state.length - 1) {
                        writer.print(',');
                    }
                }
                writer.println();
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

        public int agingWindowSize() {return config().getInt(BASE_PATH + "burst.aging-window-size");}

        public double ageSmoothFactor() {return config().getDouble(BASE_PATH + "burst.age-smoothing");}

        public int numOfPartitions() {return config().getInt(BASE_PATH + "burst.number-of-partitions");}

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
}
