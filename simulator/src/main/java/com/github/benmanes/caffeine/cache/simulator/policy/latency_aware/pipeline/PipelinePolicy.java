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

    private PolicyStats stats;
    final private FetchStage fetchingStage;
    final private PipelineBlock[] blocks;
    final private int[] quota;
    final private int totalQuanta;
    final private int blockCount;
    final private int quantumSize;
    final private int cacheCapacity;

    private boolean isDummy = false;

    private double timeframePenalty = 0;
    private int timeframeOpCount = 0;

    @Nullable private PrintWriter dumper = null;
    @Nullable private PrintWriter opDumpWriter = null;

    @Nullable final private ExtendedStats extendedStats;

    /*
     * TODO: nkeren: consult Ben regarding how to share these with only one party making the updates.
     */
    final private LatencyEstimator latencyEstimator;
    final private LatencyEstimator burstEstimator;

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
        this.extendedStats = null;
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
        Assert.assertCondition(quantumSize > 0, () -> String.format("The sampling order is too high: %d", shrinkOrder));
        cacheCapacity = totalQuanta * quantumSize;

        fetchingStage = new FetchStage(10 * cacheCapacity);

        blockCount = settings.numOfBlocks();
        quota = new int[blockCount];

        blocks = new PipelineBlock[blockCount];

        latencyEstimator = new LatestLatencyEstimator();
        burstEstimator = createBurstEstimator(settings);

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

        extendedStats = new ExtendedStats(burstEstimator, blocks[0].type());

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
    }

    private LatencyEstimator createBurstEstimator(PipelineSettings settings) {
        String type = settings.burstEstimationType();
        LatencyEstimator burstEstimator;

        switch (type) {
            case "normal" :
                burstEstimator = new MovingAverageBurstLatencyEstimator(settings.agingWindowSize(),
                                                                        settings.ageSmoothFactor(),
                                                                        settings.numOfPartitions(),
                                                                        this.cacheCapacity);
                break;
            case "sketch":
                burstEstimator = new MovingAverageWithSketchBurstEstimator(settings.agingWindowSize(),
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

        isDummy = false;
        stats = new PolicyStats(generatePipelineName());
    }

    public void makeDummy() {
        isDummy = true;
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
        }
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
                block = new LruBlock(blockConfig,
                                     new UneditableLatencyEstimatorProxy(latencyEstimator),
                                     quantumSize,
                                     quota);
                break;
            case "LBU":
                block = new LBUBlock(new UneditableLatencyEstimatorProxy(burstEstimator),
                                     cacheCapacity,
                                     quantumSize,
                                     quota);
                break;
            case "LFU":
                block = new LfuBlock(generalConfig,
                                     blockConfig,
                                     new UneditableLatencyEstimatorProxy(latencyEstimator),
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
        this.timeframePenalty = 0;
        this.timeframeOpCount = 0;
        this.dumper = null;
        this.opDumpWriter = null;

        this.fetchingStage = new FetchStage(10 * cacheCapacity);
        this.isDummy = false;

        this.blocks = new PipelineBlock[blockCount];
        this.quota = new int[blockCount];

        this.latencyEstimator = new UneditableLatencyEstimatorProxy(source.latencyEstimator);
        this.burstEstimator = new UneditableLatencyEstimatorProxy(source.burstEstimator);

        for (int i = 0; i < blockCount; ++i) {
            blocks[i] = source.blocks[i].createCopy();
            quota[i] = source.quota[i];

            Assert.assertCondition(blocks[i] != null, "Created null copy at: " + i);
        }

        stats = new PolicyStats("Copy of " + generatePipelineName());
        extendedStats = null;
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

        if (extendedStats != null) {
            extendedStats.recordEvent(event);
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

        ++this.timeframeOpCount;
        this.timeframePenalty += event.missPenalty();

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

            Assert.assertCondition(latencyEstimator.size() <= cacheCapacity + fetchingStage.size(),
                                   () -> String.format("The latency estimator size is bigger than the cache size: %d", latencyEstimator.size()));
            Assert.assertCondition(burstEstimator.size() <= cacheCapacity + fetchingStage.size(),
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
        this.timeframePenalty += currEvent.hitPenalty();

        latencyEstimator.recordHit(currEvent.hitPenalty());
        burstEstimator.recordHit(currEvent.hitPenalty());

        ++this.timeframeOpCount;
    }

    private void onHitAtFetchStage(AccessEvent fetchEvent, AccessEvent pendingEvent) {
        pendingEvent.changeEventStatus(AccessEvent.EventStatus.DELAYED_HIT);
        pendingEvent.setDelayedHitPenalty(fetchEvent.getAvailabilityTime());

        stats.recordDelayedHitPenalty(pendingEvent.delayedHitPenalty());
        stats.recordDelayedHit();

        this.timeframePenalty += pendingEvent.delayedHitPenalty();

        latencyEstimator.addValueToRecord(pendingEvent.key(),
                                          pendingEvent.delayedHitPenalty(),
                                          pendingEvent.getRequestTime());
        burstEstimator.addValueToRecord(pendingEvent.key(),
                                        pendingEvent.delayedHitPenalty(),
                                        pendingEvent.getRequestTime());
    }

    public double getTimeframeAveragePenalty() {
        if (isDummy) {
            return Double.MAX_VALUE;
        }

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

        if (extendedStats != null) {
            extendedStats.printStats();
        }
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

    private static class ExtendedStats {
        final static private int NUM_OF_RECENCY = 250000;
        final static private int NUM_OF_FREQUENCY = 100;
        final static private int NUM_OF_BURSTY = 100;

        final static private int START_KEY_OF_FREQ = NUM_OF_RECENCY;
        final static private int START_KEY_OF_BURSTY = START_KEY_OF_FREQ + NUM_OF_FREQUENCY;
        final static private int START_OF_ONE_HIT_WONDERS = START_KEY_OF_BURSTY + NUM_OF_BURSTY;

        private long recencyHits = 0;
        private long frequencyHits = 0;
        private long burstyHits = 0;
        private double recencyLatency = 0d;
        private double frequencyLatency = 0d;
        private double burstyLatency = 0d;
        private long recencyReqs = 0;
        private long frequencyReqs = 0;
        private long burstyReqs = 0;

        private double burstScoresOfRec = 0d;
        private double burstScoresOfFreq = 0d;
        private double burstScoresOfBursty = 0d;

        final private String type;

        private UneditableLatencyEstimatorProxy burstEstimator;

        public ExtendedStats(LatencyEstimator burstEstimator, String type) {
            this.burstEstimator = new UneditableLatencyEstimatorProxy(burstEstimator);
            this.type = type;
        }

        public void recordEvent(AccessEvent event) {
            long key = event.key();
            recordRequest(key);
            AccessEvent.EventStatus status = event.getStatus();
            if (status == AccessEvent.EventStatus.HIT) {
                recordHit(key);
            } else {
                double latency =
                        status == AccessEvent.EventStatus.MISS ? event.missPenalty() : event.delayedHitPenalty();
                Assert.assertCondition(latency >= 0, () -> String.format("Bad latency: %.2f", latency));
                recordLatency(key, latency);
            }
        }

        private void recordHit(long key) {
            if (key < START_KEY_OF_FREQ) {
                ++recencyHits;
            } else if (key < START_KEY_OF_BURSTY) {
                ++frequencyHits;
            } else if (key < START_OF_ONE_HIT_WONDERS) {
                ++burstyHits;
            }
        }

        private void recordRequest(long key) {
            if (key < START_KEY_OF_FREQ) {
                ++recencyReqs;
            } else if (key < START_KEY_OF_BURSTY) {
                ++frequencyReqs;
            } else if (key < START_OF_ONE_HIT_WONDERS) {
                ++burstyReqs;
            }
        }

        private void recordLatency(long key, double value) {
            if (key < START_KEY_OF_FREQ) {
                recencyLatency += value;
            } else if (key < START_KEY_OF_BURSTY) {
                frequencyLatency += value;
            } else if (key < START_OF_ONE_HIT_WONDERS) {
                burstyLatency += value;
            }
        }

        private void aggregateBurstScores() {
            for (int key = 0; key < START_KEY_OF_FREQ; ++key) {
                burstScoresOfRec += burstEstimator.getLatencyEstimation(key);
            }

            for (int key = START_KEY_OF_FREQ; key < START_KEY_OF_BURSTY; ++key) {
                burstScoresOfFreq += burstEstimator.getLatencyEstimation(key);
            }

            for (int key = START_KEY_OF_BURSTY; key < START_OF_ONE_HIT_WONDERS; ++key) {
                burstScoresOfBursty += burstEstimator.getLatencyEstimation(key);
            }
        }

        public void printStats() {
            StringBuilder sb = new StringBuilder();

            aggregateBurstScores();

            sb.append("Cache type: ");
            sb.append(type);
            sb.append('\n');
            sb.append('\n');

            sb.append(String.format("Recency H/R: %.2f\n", (100d * recencyHits) / recencyReqs));
            sb.append(String.format("Recency ARL: %.2f\n", recencyLatency / recencyReqs));
            sb.append(String.format("Recency BV: %.2f\n\n", burstScoresOfRec / NUM_OF_RECENCY));

            sb.append(String.format("Frequency H/R: %.2f\n", (100d * frequencyHits) / frequencyReqs));
            sb.append(String.format("Frequency ARL: %.2f\n", frequencyLatency / frequencyReqs));
            sb.append(String.format("Frequency BV: %.2f\n\n", burstScoresOfFreq / NUM_OF_FREQUENCY));

            sb.append(String.format("Bursty H/R: %.2f\n", (100d * burstyHits) / burstyReqs));
            sb.append(String.format("Bursty ARL: %.2f\n", burstyLatency / burstyReqs));
            sb.append(String.format("Bursty BV: %.2f\n", burstScoresOfBursty / NUM_OF_BURSTY));

            System.out.println(sb);
        }
    }
}
