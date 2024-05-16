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
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.MovingAverageWithSketchBurstEstimator;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

import javax.annotation.Nullable;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/***
 * This class represents a static configuration pipeline,
 * and may be used either as a standalone policy,
 * or as part of the Full-Ghost Hill-Climber (FGHC) algorithm.
 * TODO: nkeren - add citation when available
 */

@Policy.PolicySpec(name = "latency-aware.Pipeline")
public class PipelinePolicy implements Policy {
    final private static boolean DEBUG = false;

    final public static PipelinePolicy DUMMY = new DummyPipeline();

    final private PolicyStats stats;
    final private PipelineBlock[] blocks;
    final private int[] quota;
    final private BlockType[] types;
    final private int totalQuanta;
    final private int blockCount;
    final private int quantumSize;
    final private int cacheCapacity;

    private double timeframePenalty = 0;
    private int timeframeOpCount = 0;

    @Nullable private PrintWriter dumper = null;
    @Nullable private PrintWriter opDumpWriter = null;
    @Nullable private PrintWriter evictionDumpWriter = null;

    /*
     * TODO: nkeren: consult Ben regarding how to share these with only one party making the updates.
     */
    final private LatencyEstimator<Long> latencyEstimator;
    private LatencyEstimator<Long> burstEstimator;

    private PipelinePolicy() {
        this.stats = null;
        this.blocks = null;
        this.quota = null;
        this.types = null;
        this.totalQuanta = 0;
        this.blockCount = 0;
        this.quantumSize = 0;
        this.cacheCapacity = 0;
        this.latencyEstimator = null;
        this.burstEstimator = null;
    }

    /***
     * The standalone constructor, gets the configuration of the pipeline and its block.
     * @param config - Configuration containing the general pipeline configuration,
     *               and the configuration of each block in the pipeline.
     */
    public PipelinePolicy(Config config) {
        this(config, 0);
    }

    public PipelinePolicy(Config config, int shrinkOrder) {
        var settings = new PipelineSettings(config);

        totalQuanta = settings.numOfQuanta();
        quantumSize = settings.quantumSize() >> shrinkOrder;
        cacheCapacity = totalQuanta * quantumSize;

        blockCount = settings.numOfBlocks();
        types = new BlockType[blockCount];
        quota = new int[blockCount];

        blocks = new PipelineBlock[blockCount];

        latencyEstimator = new LatestLatencyEstimator<>();
        createBurstEstimator(settings);
        Assert.assertCondition(burstEstimator != null, "The burst estimator should have been initialized");

        final var blockConfigs = settings.blocksConfigs();

        for (int idx = 0; idx < blockCount; ++idx) {
            final Config currConfig = blockConfigs.get(idx);
            final PipelineBlockSettings blockSettings = new PipelineBlockSettings(currConfig);
            final int currQuota = blockSettings.quota();
            final String type = blockSettings.type();


            blocks[idx] = createBlock(type, currQuota, config, currConfig);
            quota[idx] = currQuota;
            types[idx] = BlockType.fromString(type);
        }

        stats = new PolicyStats(generatePipelineName());

        try {
            FileWriter fileWriter = new FileWriter("/tmp/pipeline-ops.dump", Charset.defaultCharset());
            opDumpWriter = new PrintWriter(fileWriter);
            fileWriter = new FileWriter("/tmp/pipeline-evictions.dump", Charset.defaultCharset());
            evictionDumpWriter = new PrintWriter(fileWriter);

            if (DEBUG) {
                FileWriter file = new FileWriter("/tmp/pipeline.dump", Charset.defaultCharset());
                dumper = new PrintWriter(file);
            }
        } catch (IOException exception) {
            Assert.assertCondition(false, "Got an I/O error on opening the dumpfiles: " + exception.getCause());
        }
    }

    private void createBurstEstimator(PipelineSettings settings) {
        String type = settings.burstEstimationType();

        switch (type) {
            case "normal" :
                burstEstimator = new MovingAverageBurstLatencyEstimator<>(settings.agingWindowSize(),
                                                                          settings.ageSmoothFactor(),
                                                                          settings.numOfPartitions());
                break;
            case "sketch":
                burstEstimator = new MovingAverageWithSketchBurstEstimator(settings.agingWindowSize(),
                                                                           settings.ageSmoothFactor(),
                                                                           settings.numOfPartitions(),
                                                                           settings.eps(),
                                                                           settings.confidence(),
                                                                           settings.randomSeed(),
                                                                           settings.agingWindowSize() * this.cacheCapacity,
                                                                           settings.ageSmoothFactor());
                break;
            default:
                Assert.assertCondition(false, "No such estimation type");
                break;
        }
    }

    public static Policy policy(Config config) {
         return new PipelinePolicy(config);
    }

    public void clear() {
        for (int i = 0; i < blockCount; ++i) {
            this.blocks[i].clear();
        }
    }

    public String generatePipelineName() {
        StringBuilder sb = new StringBuilder();
        sb.append("Pipeline (");
        sb.append(this.cacheCapacity);
        sb.append(") [");

        for (int i = 0; i < blockCount; ++i) {
            sb.append(types[i].toString());
            sb.append(String.format(": %.1f <%d>", 100.0 * this.quota[i] / totalQuanta, this.blocks[i].capacity()));

            if (i < blockCount - 1) {
                sb.append(", ");
            }
        }

        sb.append(']');

        return sb.toString();
    }

    private PipelineBlock createBlock(String type,
                                      int quota,
                                      Config generalConfig,
                                      Config blockConfig) {
        PipelineBlock block;

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
                block = new LfuBlock(generalConfig,
                                     blockConfig,
                                     new UneditableLatencyEstimatorProxy<>(latencyEstimator),
                                     quantumSize,
                                     quota);

                break;
            default:
                throw new IllegalStateException("No such type: " + type);
        }

        return block;
    }

    /***
     * This creates a copy of the pipeline, that should be used as shadow cache.
     * This does not allow updates to the estimators, thus should not be used as a stand-alone cache.
     * @param source - the pipeline to copy, using proxies to the estimators.
     */
    private PipelinePolicy(PipelinePolicy source) {
        this.totalQuanta = source.totalQuanta;
        this.blockCount = source.blockCount;
        this.quantumSize = source.quantumSize;
        this.cacheCapacity = source.cacheCapacity;
        this.timeframePenalty = 0;
        this.timeframeOpCount = 0;
        this.dumper = null;
        this.opDumpWriter = null;
        this.evictionDumpWriter = null;

        this.blocks = new PipelineBlock[blockCount];
        this.types = new BlockType[blockCount];
        this.quota = new int[blockCount];

        this.latencyEstimator = new UneditableLatencyEstimatorProxy<>(source.latencyEstimator);
        this.burstEstimator = new UneditableLatencyEstimatorProxy<>(source.burstEstimator);

        for (int i = 0; i < blockCount; ++i) {
            blocks[i] = source.blocks[i].createCopy();
            types[i] = source.types[i];
            quota[i] = source.quota[i];

            Assert.assertCondition(blocks[i] != null, "Created null copy at: " + i);
        }

        stats = new PolicyStats("Copy of " + generatePipelineName());
    }

    public PipelinePolicy createCopy() {
        return new PipelinePolicy(this);
    }

    @Override
    public void record(AccessEvent event) {
        EntryData entry = null;

        if (opDumpWriter != null) {
            opDumpWriter.println(ConsoleColors.colorString("event: " + event.eventNum(), ConsoleColors.WHITE_BOLD));
        }

        for (int idx = 0; idx < blockCount; ++idx) {
            // Not stopping after item is found in order to let all blocks perform bookkeeping
            blocks[idx].bookkeeping(event.key());

            EntryData blockRes = null;
            if (entry == null) {
                blockRes = blocks[idx].getEntry(event.key());
            }


            if (entry == null && blockRes != null) {
                entry = blockRes;

                if (DEBUG) {
                    opDumpWriter.println(event.key() + " in " + types[idx]);
                }
            }
        }

        if (entry == null) {
            onMiss(event);
        } else {
            recordAccordingToAvailability(entry, event);
        }
    }

    @Override
    public PolicyStats stats() {
        return stats;
    }

    private void onMiss(AccessEvent event) {
        event.changeEventStatus(AccessEvent.EventStatus.MISS);

        latencyEstimator.record(event.key(), event.missPenalty(), event.getArrivalTime());
        burstEstimator.record(event.key(), event.missPenalty(), event.getArrivalTime());

        stats.recordMiss();
        stats.recordMissPenalty(event.missPenalty());

        ++this.timeframeOpCount;
        this.timeframePenalty += event.missPenalty();

        EntryData newItem = new EntryData(event);
        insertionProcess(newItem);
        for (int idx = 0; idx < blockCount; ++idx) {
            blocks[idx].onMiss(event.key());
        }
    }

    private void insertionProcess(EntryData newItem) {
        var eventNum = newItem.event().eventNum();
        if (opDumpWriter != null) {
            opDumpWriter.println(ConsoleColors.minorInfoString("Inserting %d", newItem.event().key()));
        }

        for (int idx = 0; idx < blockCount; ++idx) {
            if (newItem != null) {
                if (dumper != null) {
                    dumper.print(eventNum + " "
                                 + types[idx].toString() + ": "
                                 + newItem.key() + " -> ");
                }

                newItem = blocks[idx].insert(newItem);

                if (DEBUG && dumper == null) {
                    debugPrint(idx, newItem, eventNum);
                }

                if (dumper != null) {
                    if (newItem != null) {
                        dumper.println(newItem.key());
                    } else {
                        dumper.println("null");
                        dumper.println("---------------");
                    }
                }
            }
        }

        if (newItem != null) {
            stats.recordEviction();
            latencyEstimator.remove(newItem.key());
            burstEstimator.remove(newItem.key());

            if (dumper != null) {
                dumper.println("---------------");
            }
        }
    }

    private void debugPrint(int idx, @Nullable EntryData item, int eventNum) {
        if (opDumpWriter == null || evictionDumpWriter == null) {
            throw new RuntimeException("Should not get to debugPrint with empty dumpers");
        }

        if (item != null) {
            opDumpWriter.println(ConsoleColors.colorString(types[idx] + " -> ", ConsoleColors.YELLOW)
                                 + ConsoleColors.colorString(String.valueOf(item.key()),
                                                             ConsoleColors.CYAN));
            evictionDumpWriter.println(ConsoleColors.colorString(String.valueOf(eventNum), ConsoleColors.PURPLE) + " " +
                                       ConsoleColors.colorString(types[idx] + " -> ", ConsoleColors.YELLOW)
                                       + ConsoleColors.colorString(String.valueOf(item),
                                                                   ConsoleColors.CYAN));
        } else {
            opDumpWriter.println(ConsoleColors.colorString(types[idx] + " -> ", ConsoleColors.YELLOW)
                                 + ConsoleColors.colorString("null", ConsoleColors.CYAN));
            evictionDumpWriter.println(ConsoleColors.colorString(String.valueOf(eventNum), ConsoleColors.PURPLE) + " " +
                                       ConsoleColors.colorString(types[idx] + " -> ", ConsoleColors.YELLOW)
                                       + ConsoleColors.colorString("null", ConsoleColors.CYAN));
        }
    }

    private void recordAccordingToAvailability(EntryData entry, AccessEvent currEvent) {
        boolean isAvailable = entry.event().isAvailableAt(currEvent.getArrivalTime());

        if (isAvailable) {
            currEvent.changeEventStatus(AccessEvent.EventStatus.HIT);
            stats.recordHit();
            stats.recordHitPenalty(currEvent.hitPenalty());
            burstEstimator.addValueToRecord(currEvent.key(), 0, currEvent.getArrivalTime());
            this.timeframePenalty += currEvent.hitPenalty();

            latencyEstimator.recordHit(currEvent.hitPenalty());
            burstEstimator.recordHit(currEvent.hitPenalty());
        } else {
            currEvent.changeEventStatus(AccessEvent.EventStatus.DELAYED_HIT);
            currEvent.setDelayedHitPenalty(entry.event().getAvailabilityTime());
            stats.recordDelayedHitPenalty(currEvent.delayedHitPenalty());
            stats.recordDelayedHit();
            this.timeframePenalty += currEvent.delayedHitPenalty();
            latencyEstimator.addValueToRecord(currEvent.key(),
                                              currEvent.delayedHitPenalty(),
                                              currEvent.getArrivalTime());
            burstEstimator.addValueToRecord(currEvent.key(),
                                            currEvent.delayedHitPenalty(),
                                            currEvent.getArrivalTime());
        }

        ++this.timeframeOpCount;
    }

    public double getTimeframeAveragePenalty() {
        final double res = this.timeframePenalty / this.timeframeOpCount;
        this.timeframePenalty = 0;
        this.timeframeOpCount = 0;

        return res;
    }

    public void moveQuantum(int incIdx, int decIdx) {
        Assert.assertCondition(incIdx != decIdx, "should not perform move into the same block");

        Assert.assertCondition(quota[incIdx] < totalQuanta, "Illegal Increment requested");
        Assert.assertCondition(quota[decIdx] > 0, "Illegal Decrement requested");

        List<EntryData> items = blocks[decIdx].decreaseSize();

        blocks[incIdx].increaseSize(items);

        ++quota[incIdx];
        --quota[decIdx];
    }

    @Override
    public void dump() {
        if (opDumpWriter != null) {
            opDumpWriter.flush();
            opDumpWriter.close();
        }
    }

    public PipelineState getCurrentState() {
        return new PipelineState(this.types, this.quota);
    }

    public int blockCount() {
        return blockCount;
    }

    public int cacheCapacity() {
        return cacheCapacity;
    }

    public boolean canExtend(int idx) {
        Assert.assertCondition(idx < blockCount && idx >= 0, () -> "Illegal block idx: " + idx);
        return quota[idx] < totalQuanta;
    }

    public boolean canShrink(int idx) {
        Assert.assertCondition(idx < blockCount && idx >= 0, () -> "Illegal block idx: " + idx);
        return quota[idx] > 0;
    }

    public String getType(int idx) { return types[idx].toString(); }

    public static final class PipelineState {
        final public BlockType[] types;

        final public int[] quotas;

        public PipelineState(BlockType[] types, int[] quotas) {
            this.types = Arrays.copyOf(types, types.length);
            this.quotas = Arrays.copyOf(quotas, quotas.length);
        }
    }

    public static final class PipelineSettings extends BasicSettings {
        final static String BASE_PATH = "pipeline";
        final static String CONFIGS_PATH = BASE_PATH + ".blocks";

        public PipelineSettings(Config config) {
            super(config);
        }

        public int numOfBlocks() { return config().getInt(BASE_PATH + ".num-of-blocks"); }

        public int numOfQuanta() { return config().getInt(BASE_PATH + ".num-of-quanta"); }

        public int quantumSize() { return config().getInt(BASE_PATH + ".quantum-size"); }

        public String burstEstimationType() { return config().getString(BASE_PATH + ".burst.type"); }

        public int agingWindowSize() {return config().getInt(BASE_PATH + ".burst.aging-window-size"); }

        public double ageSmoothFactor() {return config().getDouble(BASE_PATH + ".burst.age-smoothing"); }

        public int numOfPartitions() {return config().getInt(BASE_PATH + ".burst.number-of-partitions"); }

        public double eps() { return config().getDouble(BASE_PATH + ".burst.sketch.eps"); }

        public double confidence() { return config().getDouble(BASE_PATH + ".burst.sketch.confidence"); }

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

    private static class DummyPipeline extends PipelinePolicy {
        public DummyPipeline() {
            super();
        }

        @Override
        public PipelinePolicy createCopy() {
            return this;
        }

        @Override
        public void record(AccessEvent event) {
            // Not doing anything
        }

        @Override
        public double getTimeframeAveragePenalty() {
            return Double.MAX_VALUE;
        }

        @Override
        public void moveQuantum(int incIdx, int decIdx) {
            // Not doing anything
        }

        @Override
        public boolean canExtend(int idx) {
            return false;
        }

        @Override
        public boolean canShrink(int idx) {
            return false;
        }
    }

    public enum BlockType {
        LRU("LRU"),
        LFU("LFU"),
        BC("BC");

        BlockType(String str) {
            this.str = str;
        }

        final private String str;

        public static BlockType fromString(String type) {
            switch (type) {
                case "LRU":
                    return LRU;
                case "LFU":
                    return LFU;
                case "BC":
                    return BC;
                default:
                    throw new IllegalStateException("No such type defined " + type);
            }
        }


        @Override
        public String toString() {
            return this.str;
        }
    }
}
