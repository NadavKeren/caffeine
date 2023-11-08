package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline;

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
import java.util.function.DoubleSupplier;

@Policy.PolicySpec(name = "sketch.AdaptivePipeline")
public class AdaptivePipelineCache implements Policy {
    private static boolean DEBUG = true;

    private static Logger logger = DEBUG ? System.getLogger(AdaptivePipelineCache.class.getName()) : null;

    final private int totalQuota;
    final private int quantumSize;
    final private int cacheCapacity;

    final private int adaptionTimeframe;
    private int opsSinceAdaption = 0;
    final int blockCount;
    final private LatencyEstimator<Long> latencyEstimator;
    final private LatencyEstimator<Long> burstEstimator;
    final PipelineBlock[] blocks;
    final int[] blockQuotas;
    final String[] types;
    final private AdaptivePipelineStats stats;

    final private int[] opPerType = new int[3];
    final private int[] hitPerType = new int[3];

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

//        final int ghostSize = settings.ghostSize();

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
                block = new BurstCache(new UneditableLatencyEstimatorProxy<>(burstEstimator),
                                       quantumSize,
                                       quota);
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

        int idx = (int) event.key() / 100;
        ++opPerType[idx];

        if (entry == null) {
            onMiss(event);
        } else {
            recordAccordingToAvailability(entry, event);
        }

        if (event.getStatus() == AccessEvent.EventStatus.HIT) {
            ++hitPerType[idx];
        }

        ++opsSinceAdaption;

        if (opsSinceAdaption >= adaptionTimeframe && size() >= cacheCapacity) { //) && areGhostsFull()) {
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

        stats.recordMissAt(event.key());
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

            stats.recordHitAt(currEvent.key());
        } else {
            currEvent.changeEventStatus(AccessEvent.EventStatus.DELAYED_HIT);
            currEvent.setDelayedHitPenalty(entry.event().getAvailabilityTime());
            stats.recordDelayedHitPenalty(currEvent.delayedHitPenalty());
            stats.recordDelayedHit();
            latencyEstimator.addValueToRecord(currEvent.key(),
                                              currEvent.delayedHitPenalty(),
                                              currEvent.getArrivalTime());
            burstEstimator.addValueToRecord(currEvent.key(), currEvent.delayedHitPenalty(), currEvent.getArrivalTime());

            stats.recordMissAt(currEvent.key());
        }
    }

    @SuppressWarnings("unused")
    private void adapt(int eventNum) {
        validate();
    }

    @Override
    public void finished() {
        validate();

        System.out.println(this.getClass().getSimpleName());
        for (int i = 0; i < 3; ++i) {
            System.out.println(i + ": " + (100 * (double) hitPerType[i] / opPerType[i]));
        }
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

        public int ghostSize() { return config().getInt(BASE_PATH + ".ghost-size"); }

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
        int[] opCountPerType;
        int[] hitCountPerType;

        public AdaptivePipelineStats(String format, Object... args) {
            super(format, args);
            hitCountsPerBlock = new int[blockCount];
            opCountPerType = new int[3];
            hitCountPerType = new int[3];
            addMetric(Metric.of("Recency Hit Ratio", (DoubleSupplier) this::recencyHitRate, Metric.MetricType.PERCENT, true));
            addMetric(Metric.of("Frequency Hit Ratio", (DoubleSupplier) this::frequecnyHitRate, Metric.MetricType.PERCENT, true));
            addMetric(Metric.of("Bursty Hit Ratio", (DoubleSupplier) this::burstyHitRate, Metric.MetricType.PERCENT, true));
        }

        public void recordHitAtBlock(int idx) {
            ++hitCountsPerBlock[idx];
        }

        public void recordHitAt(long key) {
            int idx = (int) key / 100;
            ++opCountPerType[idx];
            ++hitCountPerType[idx];
        }

        public void recordMissAt(long key) {
            int idx = (int) key / 100;
            ++opCountPerType[idx];
        }

        private double getHitRate(int idx) {
            return (double) hitCountPerType[idx] / opCountPerType[idx];
        }

        public double recencyHitRate() {
            return getHitRate(0);
        }

        public double frequecnyHitRate() {
            return getHitRate(1);
        }

        public double burstyHitRate() {
            return getHitRate(2);
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
