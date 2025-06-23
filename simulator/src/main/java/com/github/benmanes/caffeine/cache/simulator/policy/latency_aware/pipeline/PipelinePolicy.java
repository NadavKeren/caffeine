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
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.ApproximateBurstLatencyEstimator;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.ApproximateWithSketchBurstEstimator;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

import javax.annotation.Nonnull;
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

    private PolicyStats stats;
    private FetchStage fetchingStage;
    final private PipelineBlock[] blocks;
    final private int[] quota;
    final private int totalQuanta;
    final private int blockCount;
    final private int quantumSize;
    final private int cacheCapacity;
    private int opsSinceLastAging = 0;
    final private int agingWindowSize;

    private boolean isDummy;
    final private boolean isCopy;

    @Nonnull final private TimeframeStats timeframeStats;

//    private double timeframePenalty = 0;
//    private int timeframeOpCount = 0;

    @Nullable private PrintWriter dumper = null;
    @Nullable private PrintWriter opDumpWriter = null;

    /*
     * TODO: nkeren: consult Ben regarding how to share these with only one party making the updates.
     */
    private LatencyEstimator latencyEstimator;
    private LatencyEstimator burstEstimator;

    private PipelinePolicy() {
        this.fetchingStage = null;
        this.stats = null;
        this.blocks = null;
        this.quota = null;
        this.totalQuanta = 0;
        this.blockCount = 0;
        this.quantumSize = 0;
        this.cacheCapacity = 0;
        this.latencyEstimator = null;
        this.burstEstimator = null;
        this.agingWindowSize = 0;
        this.isDummy = true;
        this.isCopy = true;
        this.timeframeStats = new TimeframeStats();
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
        BasicSettings basicSettings = new BasicSettings(config);

        Assert.assertCondition(settings.quantumSize() == basicSettings.maximumSize() / totalQuanta,
                               () -> String.format("Quantum Size: %d and max size: %d mismatch",
                                                   settings.quantumSize(),
                                                   basicSettings.maximumSize()));

        quantumSize = settings.quantumSize() >> shrinkOrder;
        Assert.assertCondition(quantumSize > 0, () -> String.format("The sampling order is too high: %d", shrinkOrder));
        cacheCapacity = totalQuanta * quantumSize;

        fetchingStage = new FetchStage(10 * cacheCapacity);

        blockCount = settings.numOfBlocks();
        quota = new int[blockCount];

        blocks = new PipelineBlock[blockCount];

        latencyEstimator = new LatestLatencyEstimator();
        burstEstimator = createBurstEstimator(settings);

        this.agingWindowSize = settings.agingWindowSize();

        final var blockConfigs = settings.blocksConfigs();

        for (int idx = 0; idx < blockCount; ++idx) {
            final Config currConfig = blockConfigs.get(idx);
            final PipelineBlockSettings blockSettings = new PipelineBlockSettings(currConfig);
            final int currQuota = blockSettings.quota();
            final String type = blockSettings.type();


            blocks[idx] = createBlock(type, currQuota, config, currConfig);
            quota[idx] = currQuota;
        }

        stats = new PolicyStats(generatePipelineName());
        timeframeStats = new TimeframeStats();

        try {
            if (DEBUG) {
                FileWriter fileWriter = new FileWriter("/home/nadav/caching/pipeline-ops.dump", Charset.defaultCharset());
                opDumpWriter = new PrintWriter(fileWriter);
                opDumpWriter.println();

                FileWriter file = new FileWriter("/home/nadav/caching/pipeline.dump", Charset.defaultCharset());
                dumper = new PrintWriter(file);
                dumper.println();
            }
        } catch (IOException exception) {
            Assert.assertCondition(false, "Got an I/O error on opening the dumpfiles: " + exception.getCause());
        }

        isDummy = false;
        isCopy = false;
    }

    private LatencyEstimator createBurstEstimator(PipelineSettings settings) {
        String type = settings.burstEstimationType();
        LatencyEstimator burstEstimator;

        switch (type) {
            case "normal":
                burstEstimator = new MovingAverageBurstEstimator(settings.ageSmoothFactor(),
                                                                 settings.numOfPartitions(),
                                                                 this.cacheCapacity);
                break;
            case "approximate" :
                burstEstimator = new ApproximateBurstLatencyEstimator(settings.agingWindowSize(),
                                                                      settings.ageSmoothFactor(),
                                                                      settings.numOfPartitions(),
                                                                      this.cacheCapacity);
                break;
            case "sketch":
                burstEstimator = new ApproximateWithSketchBurstEstimator(settings.agingWindowSize(),
                                                                         settings.ageSmoothFactor(),
                                                                         settings.numOfPartitions(),
                                                                         settings.eps(),
                                                                         settings.confidence(),
                                                                         settings.randomSeed(),
                                                                           settings.agingWindowSize() * this.cacheCapacity,
                                                                         settings.ageSmoothFactor(),
                                                                         this.cacheCapacity);
                break;
            default:
                Assert.assertCondition(false, "No such estimation type");
                throw new AssertionError();
        }

        return burstEstimator;
    }

    public static Policy policy(Config config) {
         return new PipelinePolicy(config);
    }

    public void clear() {
        for (int i = 0; i < blockCount; ++i) {
            this.blocks[i].clear();
        }

        if (this.fetchingStage != null) {
            this.fetchingStage.clear();
        }

        stats = new PolicyStats(generatePipelineName());
        timeframeStats.clear();
    }

    public void copyInto(PipelinePolicy other) {
        Assert.assertCondition(this.blockCount == other.blockCount,
                               () -> String.format("pipeline size mismatch: this: %d\tother: %d",
                                                   this.blockCount,
                                                   other.blockCount));
        int blockIdx = 0;
        for (var block: this.blocks) {
            block.copyInto(other.blocks[blockIdx]);
            other.quota[blockIdx] = this.quota[blockIdx];
            ++blockIdx;

            other.fetchingStage = new FetchStage(this.fetchingStage);
            other.latencyEstimator = this.latencyEstimator.createDeepCopy();
            other.burstEstimator = this.burstEstimator.createDeepCopy();
        }
        other.timeframeStats.clear();
    }

    public String generatePipelineName() {
        StringBuilder sb = new StringBuilder();
        sb.append("Pipeline (");
        sb.append(this.cacheCapacity);
        sb.append(") [");

        for (int i = 0; i < blockCount; ++i) {
            sb.append(blocks[i].type());
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
                block = new LruBlock(quota, quantumSize);
                break;
            case "LFU" :
                block = new LfuBlock(quota, quantumSize, generalConfig);
                break;
            case "LA-LRU":
                block = new LALruBlock(blockConfig,
                                       new UneditableLatencyEstimatorProxy(latencyEstimator),
                                       quantumSize,
                                       quota);
                break;
            case "LA-LFU":
                block = new LALfuBlock(generalConfig,
                                       blockConfig,
                                       new UneditableLatencyEstimatorProxy(latencyEstimator),
                                       quantumSize,
                                       quota);

                break;
            case "LBU":
                block = new LbuBlock(new UneditableLatencyEstimatorProxy(burstEstimator),
                                     cacheCapacity,
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
        Assert.assertCondition(!source.isDummy, "Should not copy a dummy cache");

        this.totalQuanta = source.totalQuanta;
        this.blockCount = source.blockCount;
        this.quantumSize = source.quantumSize;
        this.cacheCapacity = source.cacheCapacity;
        this.timeframeStats = new TimeframeStats();
        this.dumper = null;
        this.opDumpWriter = null;

        this.fetchingStage = new FetchStage(source.fetchingStage);
        this.isDummy = false;

        this.blocks = new PipelineBlock[blockCount];
        this.quota = new int[blockCount];

        this.latencyEstimator = source.latencyEstimator.createDeepCopy();
        this.burstEstimator = source.burstEstimator.createDeepCopy();
        this.agingWindowSize = source.agingWindowSize;
        this.opsSinceLastAging = source.opsSinceLastAging;

        for (int i = 0; i < blockCount; ++i) {
            blocks[i] = source.blocks[i].createCopy();
            quota[i] = source.quota[i];

            Assert.assertCondition(blocks[i] != null, "Created null copy at: " + i);
        }

        stats = new PolicyStats("Copy of " + generatePipelineName());
        isCopy = true;
    }

    public PipelinePolicy createCopy() {
        return new PipelinePolicy(this);
    }

    private void insertArrivals(double timeStamp) {
        while (fetchingStage.size() > 0 && fetchingStage.getClosestArrival() < timeStamp) {
            AccessEvent arrivedEvent = fetchingStage.extractClosestArrival();
            EntryData arrivedData = new EntryData(arrivedEvent);
            insertionProcess(arrivedData);
        }
    }

    @Override
    public void record(AccessEvent event) {
        if (isDummy) {
            return;
        }

        insertArrivals(event.getRequestTime());
        EntryData entry = null;

        if (opDumpWriter != null) {
            opDumpWriter.print(ConsoleColors.colorString(String.format("event: \t%d\t", event.eventNum()), ConsoleColors.WHITE_BOLD));
        }

        if (fetchingStage.contains(event.key())) {
            onHitAtFetchStage(fetchingStage.get(event.key()), event);

            for (PipelineBlock block : blocks) {
                block.bookkeeping(event.key());
            }
        } else {
            for (PipelineBlock block : blocks) {
                // Not stopping after item is found in order to let all blocks perform bookkeeping
                block.bookkeeping(event.key());

                if (entry == null) {
                    entry = block.getEntry(event.key());
                }

                if (DEBUG && opDumpWriter != null && dumper != null && entry != null) {
                    opDumpWriter.println(event.key() + " found in " + block.type());
                    dumper.println(event.eventNum() + " in cache");
                }
            }

            if (entry == null) {
                onMiss(event);
            } else {
                onCacheHit(entry, event);
            }
        }

        ++opsSinceLastAging;
        if (opsSinceLastAging >= agingWindowSize) {
            latencyEstimator.ageAll();
            burstEstimator.ageAll();
            opsSinceLastAging = 0;
        }
    }

    @Override
    public PolicyStats stats() {
        return stats;
    }

    private void onMiss(AccessEvent event) {
        event.changeEventStatus(AccessEvent.EventStatus.MISS);

        latencyEstimator.record(event.key(), event.missPenalty(), event.getRequestTime());
        burstEstimator.record(event.key(), event.missPenalty(), event.getRequestTime());

        stats.recordMiss();
        stats.recordMissPenalty(event.missPenalty());

        this.timeframeStats.recordMiss(event.missPenalty());

        for (int idx = 0; idx < blockCount; ++idx) {
            blocks[idx].onMiss(event.key());
        }

        this.fetchingStage.insert(event);
    }

    private void insertionProcess(EntryData newItem) {
        var eventNum = newItem.event().eventNum();
        if (opDumpWriter != null) {
            opDumpWriter.println(ConsoleColors.colorString("not found", ConsoleColors.WHITE_BOLD));
        }

        final int totalSizeBeforeInsertion = Arrays.stream(blocks).mapToInt(PipelineBlock::size).sum();

        for (PipelineBlock block : blocks) {
            if (newItem != null) {
                if (dumper != null) {
                    dumper.print(eventNum + "\t"
                                 + block.type() + ":\t"
                                 + newItem.key() + " -> ");
                }

                newItem = block.insert(newItem);

                if (DEBUG) {
                    debugPrint(block.type(), newItem, eventNum);
                }
            }
        }

        if (DEBUG && dumper != null) {
            dumper.println("-------------------------------------------------------");
            dumper.flush();
        }

        if (newItem != null) {
            stats.recordEviction();
            latencyEstimator.remove(newItem.key());
            burstEstimator.remove(newItem.key());

            Assert.assertCondition(isCopy || latencyEstimator.size() <= cacheCapacity + fetchingStage.size(),
                                   () -> String.format("The latency estimator size is bigger than the cache size: %d", latencyEstimator.size()));
            Assert.assertCondition(isCopy || burstEstimator.size() <= cacheCapacity + fetchingStage.size(),
                                   () -> String.format("The burst estimator size is bigger than the cache size: %d", burstEstimator.size()));
        } else {
            Assert.assertCondition(totalSizeBeforeInsertion < cacheCapacity, () -> String.format("The total size %d before the insertion is the cache-capacity %d, and the removed item is null", totalSizeBeforeInsertion, cacheCapacity));
        }
    }

    private void debugPrint(String blockType, @Nullable EntryData item, int eventNum) {
        if (dumper == null) {
            return;
        }

        String itemStr = item == null
                       ? ConsoleColors.colorString("null", ConsoleColors.PURPLE)
                       : ConsoleColors.colorString(String.valueOf(item.key()), ConsoleColors.CYAN);

        dumper.println(eventNum + ":\t" + ConsoleColors.colorString(blockType + " -> ", ConsoleColors.YELLOW) + itemStr);
    }

    private void onCacheHit(EntryData entry, AccessEvent currEvent) {
        boolean isAvailable = entry.event().isAvailableAt(currEvent.getRequestTime());
        Assert.assertCondition(isAvailable, "Should not consider an non-available event as cache hit");

        currEvent.changeEventStatus(AccessEvent.EventStatus.HIT);
        stats.recordHit();
        stats.recordHitPenalty(currEvent.hitPenalty());
        burstEstimator.addValueToRecord(currEvent.key(), 0, currEvent.getRequestTime());

        latencyEstimator.recordHit(currEvent.hitPenalty());
        burstEstimator.recordHit(currEvent.hitPenalty());

        this.timeframeStats.recordHit(currEvent.hitPenalty());
    }

    private void onHitAtFetchStage(AccessEvent fetchEvent, AccessEvent pendingEvent) {
        pendingEvent.changeEventStatus(AccessEvent.EventStatus.DELAYED_HIT);
        pendingEvent.setDelayedHitPenalty(fetchEvent.getAvailabilityTime());

        stats.recordDelayedHitPenalty(pendingEvent.delayedHitPenalty());
        stats.recordDelayedHit();

        this.timeframeStats.recordDelayed(pendingEvent.delayedHitPenalty());

        latencyEstimator.addValueToRecord(pendingEvent.key(),
                                          pendingEvent.delayedHitPenalty(),
                                          pendingEvent.getRequestTime());
        burstEstimator.addValueToRecord(pendingEvent.key(),
                                        pendingEvent.delayedHitPenalty(),
                                        pendingEvent.getRequestTime());
    }

    public double getTimeframeAveragePenalty() {
        return isDummy ? Double.MAX_VALUE : this.timeframeStats.avgPenalty();
    }

    public double getTimeframeHitRatio() {
        return isDummy ? Double.MAX_VALUE : this.timeframeStats.hitRatio();
    }

    public String getTimeframeStats() {
        return this.timeframeStats.getStats();
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

    public void makeDummy() {
        this.isDummy = true;
    }

    public boolean isDummy() {
        return this.isDummy;
    }

    @Override
    public void dump() {
        if (opDumpWriter != null) {
            opDumpWriter.flush();
            opDumpWriter.close();
        }
    }

    public int[] getQuota() {
        return Arrays.copyOf(quota, quota.length);
    }

    public PipelineState getCurrentState() {
        String[] types = new String[this.blockCount];
        for (int idx = 0; idx < types.length; ++idx) {
            types[idx] = blocks[idx].type();
        }

        return new PipelineState(types, this.quota);
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

    public String getType(int idx) { return blocks[idx].type(); }

    public void resetTimeframeStats() {
        this.timeframeStats.clear();
    }

    public static final class PipelineState {
        final public String[] types;
        final public int[] quotas;

        public PipelineState(String[] types, int[] quotas) {
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

    private class TimeframeStats {
        private int hitCount = 0;
        private int delayedCount = 0;
        private int missCount = 0;
        private double penalty = 0d;

        public void recordHit(double penalty) {
            hitCount++;
            this.penalty += penalty;
        }

        public void recordMiss(double penalty) {
            missCount++;
            this.penalty += penalty;
        }

        public void recordDelayed(double penalty) {
            delayedCount++;
            this.penalty += penalty;
        }

        public void clear() {
            hitCount = 0;
            delayedCount = 0;
            missCount = 0;
            penalty = 0d;
        }

        public String getStats() {
            StringBuilder sb = new StringBuilder();

            int totalCount = totalCount();


            sb.append(String.format("\thits: %d %.2f", hitCount, 100d * hitCount / totalCount));
            sb.append(String.format("\tdelayed: %d %.2f", delayedCount, 100d * delayedCount / totalCount));
            sb.append(String.format("\tmiss: %d %.2f", missCount, 100d * missCount / totalCount));
            sb.append(String.format("\tavg. pen: %.2f", avgPenalty()));
            if (!isDummy) {
                for (PipelineBlock block : blocks) {
                    sb.append(String.format("\t%s: used: %d", block.type(), block.size()));
                }
            }

            clear();
            return sb.toString();
        }

        private int totalCount() { return hitCount + delayedCount + missCount; }

        public double avgPenalty() { return  penalty / totalCount(); }

        public double hitRatio() { return 100d * hitCount / totalCount(); }
    }
}
