package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.admission.Admittor;
import com.github.benmanes.caffeine.cache.simulator.admission.LATinyLfu;
import com.github.benmanes.caffeine.cache.simulator.policy.*;
import com.github.benmanes.caffeine.cache.simulator.policy.linked.CraBlock;
import com.typesafe.config.Config;

import javax.annotation.Nullable;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;
import java.util.function.DoubleSupplier;

import static com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats.Metric.MetricType.PERCENT;
import static java.util.stream.Collectors.toSet;

@Policy.PolicySpec(name = "sketch.WindowCABurstBlock")
public class WindowCostAwareWithBurstinessBlockPolicy implements Policy {
    private static int ID = 0;
    private final static boolean DEBUG = false;
    @Nullable private PrintWriter dumper = null;
    protected WindowCAWithBBStats policyStats;
    protected final LatencyEstimator<Long> latencyEstimator;
    protected final LatencyEstimator<Long> burstEstimator;
    protected final Admittor admittor;
    protected final int cacheCapacity;

    protected final CraBlock windowBlock;
    protected final CraBlock probationBlock;
    protected final CraBlock protectedBlock;
    protected final BurstBlock burstBlock;

    protected double normalizationBias;
    protected double normalizationFactor;
    protected double maxDelta;
    protected int maxDeltaCounts;
    protected int samplesCount;

    private static final System.Logger logger = System.getLogger(LatencyEstimator.class.getName());

    public WindowCostAwareWithBurstinessBlockPolicy(double percentMain,
                                                    WindowCAWithBBSettings settings,
                                                    int decayFactor,
                                                    int maxLists) {
        this.latencyEstimator = createLatencyEstimator(settings);
        this.burstEstimator = createBurstEstimator(settings);

        this.cacheCapacity = (int) settings.maximumSize();

        final int burstCacheCapacity = (int) (settings.percentBurstBlock() * cacheCapacity);
        int nonBurstCacheCapacity = cacheCapacity - burstCacheCapacity;

        final int mainCacheSize = (int) (nonBurstCacheCapacity * percentMain);
        final int protectedCapacity = (int) (mainCacheSize * settings.percentMainProtected());
        final int windowCapacity = nonBurstCacheCapacity - mainCacheSize;

        this.protectedBlock = new CraBlock(decayFactor,
                                           maxLists,
                                           protectedCapacity,
                                           latencyEstimator,
                                           "protected-block");
        this.probationBlock = new CraBlock(decayFactor,
                                           maxLists,
                                           mainCacheSize - protectedCapacity,
                                           latencyEstimator,
                                           "probation-block");
        this.windowBlock = new CraBlock(decayFactor, maxLists, windowCapacity, latencyEstimator, "window-block");
        this.burstBlock = new BurstBlock(burstCacheCapacity, burstEstimator);

        this.policyStats = new WindowCAWithBBStats("sketch.WindowCAWithBB (%.0f%%, %.0f%%, %.0f%%)",
                                                   (double) 100 * windowCapacity / cacheCapacity,
                                                   (double) 100 * mainCacheSize / cacheCapacity,
                                                   (double) 100 * burstCacheCapacity / cacheCapacity);

        this.admittor = new LATinyLfu(settings.config(), policyStats, latencyEstimator);

        this.normalizationBias = 0;
        this.normalizationFactor = 0;
        this.maxDelta = 0;
        this.maxDeltaCounts = 0;
        this.samplesCount = 0;

        if (DEBUG) {
            try {
                FileWriter file = new FileWriter("/tmp/WCAWBB.dump", Charset.defaultCharset());
                dumper = new PrintWriter(file);
            } catch (IOException ex) {
                Assert.assertCondition(false, ex.getMessage());
            }
        }
    }

    public WindowCostAwareWithBurstinessBlockPolicy(int windowSize,
                                                    int probationSize,
                                                    int protectedSize,
                                                    int burstSize,
                                                    Config config,
                                                    @Nullable WindowCAWithBBStats stats) {
        WindowCAWithBBSettings settings = new WindowCAWithBBSettings(config);
        int decayFactor = settings.decayFactor();
        int maxLists = settings.maxLists();

        this.policyStats = (stats == null)
                           ? new WindowCAWithBBStats(
                "sketch.WindowCAWithBB (W:%d,LFU:%d,B:%d,decayFactor=%d,maxLists=%d)",
                windowSize,
                probationSize + protectedSize,
                burstSize,
                decayFactor,
                maxLists)
                           : stats;

        this.latencyEstimator = createLatencyEstimator(settings);
        this.burstEstimator = createBurstEstimator(settings);
        this.admittor = new LATinyLfu(settings.config(), policyStats, latencyEstimator);

        this.cacheCapacity = (int) settings.maximumSize();

        this.protectedBlock = new CraBlock(decayFactor, maxLists, protectedSize, latencyEstimator, "protected-block");
        this.probationBlock = new CraBlock(decayFactor, maxLists, probationSize, latencyEstimator, "probation-block");
        this.windowBlock = new CraBlock(decayFactor, maxLists, windowSize, latencyEstimator, "window-block");
        this.burstBlock = new BurstBlock(burstSize, burstEstimator);

        this.normalizationBias = 0;
        this.normalizationFactor = 0;
        this.maxDelta = 0;
        this.maxDeltaCounts = 0;
        this.samplesCount = 0;
    }

    public WindowCostAwareWithBurstinessBlockPolicy(WindowCAWithBBStats stats,
                                                    int windowSize,
                                                    int probationSize,
                                                    int protectedSize,
                                                    int burstSize,
                                                    LatencyEstimator<Long> latencyEstimator,
                                                    LatencyEstimator<Long> burstEstimator,
                                                    Admittor admittor,
                                                    CraBlock otherWindow,
                                                    CraBlock otherProtected,
                                                    CraBlock otherProbation,
                                                    BurstBlock otherBurst,
                                                    String name) {
        this.policyStats = stats;
        this.latencyEstimator = latencyEstimator;
        this.burstEstimator = burstEstimator;
        this.admittor = admittor;

        this.cacheCapacity = windowSize + probationSize + protectedSize + burstSize;

        this.protectedBlock = otherProtected.createGhostCopy(name + "-protected@" + ID);
        this.probationBlock = otherProbation.createGhostCopy(name + "-probation@" + ID);
        this.windowBlock = otherWindow.createGhostCopy(name + "-window@" + ID);
        this.burstBlock = new BurstBlock(otherBurst, name + "-burst@" + ID);

        ++ID;

        this.normalizationBias = 0;
        this.normalizationFactor = 0;
        this.maxDelta = 0;
        this.maxDeltaCounts = 0;
        this.samplesCount = 0;
    }

    /***
     * Creates a dummy cache.
     */
    public WindowCostAwareWithBurstinessBlockPolicy() {
        this.policyStats = null;
        this.latencyEstimator = null;
        this.burstEstimator = null;
        this.admittor = null;
        this.cacheCapacity = 40;
        this.protectedBlock = null;
        this.probationBlock = null;
        this.windowBlock = null;
        this.burstBlock = null;
    }

    private LatencyEstimator<Long> createLatencyEstimator(WindowCAWithBBSettings settings) {
        BasicSettings.LatencyEstimationSettings latencySettings = settings.latencyEstimationSettings();
        String estimationType = latencySettings.estimationType();

        LatencyEstimator<Long> estimator;
        switch (estimationType) {
            case "latest":
                estimator = new LatestLatencyEstimator<>();
                break;
            case "true-average":
                estimator = new TrueAverageEstimator<>();
                break;
            case "buckets":
                estimator = new BucketLatencyEstimation<>(latencySettings.numOfBuckets(), latencySettings.epsilon());
                break;
            default:
                throw new IllegalStateException("Unknown estimator type: " + estimationType);
        }

        logger.log(System.Logger.Level.DEBUG,
                   String.format("Created estimator of type %s, class: %s",
                                 estimationType,
                                 estimator.getClass().getSimpleName()));

        return estimator;
    }

    private LatencyEstimator<Long> createBurstEstimator(WindowCAWithBBSettings settings) {
        LatencyEstimator<Long> estimator;
        String strategy = settings.burstEstimationStrategy();
        switch (strategy) {
            case "naive":
                estimator = new NaiveBurstLatencyEstimator<>();
                break;
            case "moving-average":
                estimator = new MovingAverageBurstLatencyEstimator<>(settings.agingWindowSize(),
                                                                     settings.ageSmoothFactor(),
                                                                     settings.numOfPartitions());
                break;
            case "random":
                estimator = new RandomNaiveBurstEstimator<>(0.05);
                break;
            default:
                throw new IllegalStateException("Unknown strategy: " + strategy);
        }

        return estimator;
    }

    /**
     * Returns all variations of this policy based on the configuration parameters.
     */
    public static Set<Policy> policies(Config config) {
        WindowCAWithBBSettings settings = new WindowCAWithBBSettings(config);
        return settings.percentMain().stream()
                       .map(percentMain ->
                                    new WindowCostAwareWithBurstinessBlockPolicy(percentMain,
                                                                                 settings,
                                                                                 settings.decayFactor(),
                                                                                 settings.maxLists()))
                       .collect(toSet());
    }

    @Override
    public PolicyStats stats() {
        return policyStats;
    }

    /**
     * Adds the entry to the admission window, evicting if necessary.
     */
    private void onMiss(AccessEvent event) {
        event.changeEventStatus(AccessEvent.EventStatus.MISS);
        admittor.record(event);
        windowBlock.addEntry(event);

        if (dumper != null) {
            dumper.print(event.eventNum() + " LRU: " + event.key() + " -> ");
        }
        evict(event.getArrivalTime(), event.eventNum());
    }

    /**
     * Moves the entry to the MRU position in the admission window.
     */
    private void onWindowHit(EntryData entry) {
        windowBlock.moveToTail(entry);
    }

    /**
     * Promotes the entry to the protected region's MRU position, demoting an entry if necessary.
     */
    private void onProbationHit(EntryData entry) {
        probationBlock.remove(entry.key());
        protectedBlock.addEntry(entry);

        if (protectedBlock.isFull()) {
            EntryData demote = protectedBlock.findVictim();
            protectedBlock.remove(demote.key());
            probationBlock.addEntry(demote);
        }
    }

    /**
     * Moves the entry to the MRU position, if it falls outside the fast-path threshold.
     */
    private void onProtectedHit(EntryData entry) {
        protectedBlock.moveToTail(entry);
    }

    private void updateNormalization(long key) {
        double delta = latencyEstimator.getDelta(key);

        if (delta > normalizationFactor) {
            ++samplesCount;
            ++maxDeltaCounts;

            maxDelta = (maxDelta * maxDeltaCounts + delta) / maxDeltaCounts;
        }

        normalizationBias = normalizationBias > 0
                            ? Math.min(normalizationBias, Math.max(0, delta))
                            : Math.max(0, delta);

        if (samplesCount % 1000 == 0 || normalizationFactor == 0) {
            normalizationFactor = maxDelta;
            maxDeltaCounts = 1;
            samplesCount = 0;
        }

        protectedBlock.setNormalization(normalizationBias, normalizationFactor);
        probationBlock.setNormalization(normalizationBias, normalizationFactor);
        windowBlock.setNormalization(normalizationBias, normalizationFactor);
    }

    /**
     * Evicts from the admission window into the probation space. If the size exceeds the maximum,
     * then the admission candidate and probation's victim are evaluated and one is evicted.
     */
    private void evict(double eventTime, int eventNum) {
        if (!windowBlock.isFull()) {
            if (dumper != null) {
                dumper.println("null");
                dumper.println("---------------");
            }
            return;
        }

        EntryData windowBlockVictim = windowBlock.removeVictim();

        if (dumper != null) {
            dumper.println(windowBlockVictim.key());
            dumper.print(eventNum + " LFU: " + windowBlockVictim.key() + " -> ");
        }

        if (lfuCapacity() > 0) {
            probationBlock.addEntry(windowBlockVictim);
            if (lfuSize() > lfuCapacity()) {
                EntryData probationBlockVictim = probationBlock.findVictim();
                EntryData evict = admittor.admit(windowBlockVictim.event(), probationBlockVictim.event())
                                  ? probationBlockVictim
                                  : windowBlockVictim;

                if (dumper != null) {
                    dumper.println(evict.key());
                    dumper.print(eventNum + " BC: " + evict.key() + " -> ");
                }

                probationBlock.remove(evict.key());

                if (burstBlock.capacity() > 0) {
                    final double evictScore = burstEstimator.getLatencyEstimation(evict.key(), eventTime);
                    if (burstBlock.isFull()) {
                        final EntryData burstVictim = burstBlock.getVictim();
                        final double burstVictimScore = burstEstimator.getLatencyEstimation(burstVictim.key(),
                                                                                            eventTime);

                        if (evictScore >= burstVictimScore) {
                            burstBlock.removeVictim();
                            burstBlock.admit(evict);

                            if (dumper != null) {
                                dumper.println(burstVictim.key());
                            }
                        } else if (dumper != null) {
                            dumper.println(evict.key());
                        }

                        policyStats.recordEviction();
                    } else {
                        burstBlock.admit(evict);

                        if (dumper != null) {
                            dumper.println("null");
                        }
                    }
                } else {
                    policyStats.recordEviction();

                    if (dumper != null) {
                        dumper.println(evict.key());
                    }
                }
            } else if (dumper != null) {
                dumper.println("null");
            }

        } else {
            policyStats.recordEviction();
            if (dumper != null) {
                dumper.println("null");
            }
        }

        if (dumper != null) {
            dumper.println("---------------");
        }
    }

    private int lfuSize() {
        return protectedBlock.size() + probationBlock.size();
    }

    private int lfuCapacity() {
        return protectedBlock.capacity() + probationBlock.capacity();
    }

    @Override
    public void record(AccessEvent event) {
        final double hitPenaltyBefore = stats().hitPenalty();
        final double delayedHitPenaltyBefore = stats().delayedHitPenalty();
        final double missPenaltyBefore = stats().missPenalty();

        final long key = event.key();
        policyStats.recordOperation();
        EntryData entry = null;

        if (windowBlock.isHit(key)) {
            entry = windowBlock.get(key);
            policyStats.recordWindowHit(event.missPenalty());
            onWindowHit(entry);
        } else if (probationBlock.isHit(key)) {
            entry = probationBlock.get(key);
            policyStats.recordProbationHit(event.missPenalty());
            onProbationHit(entry);
        } else if (protectedBlock.isHit(key)) {
            entry = protectedBlock.get(key);
            policyStats.recordProtectedHit(event.missPenalty());
            onProtectedHit(entry);
        } else if (burstBlock.isHit(key)) {
            entry = burstBlock.get(key);
            policyStats.recordBurstBlockHit(event.missPenalty());
        } else {
            onMiss(event);
            latencyEstimator.record(event.key(), event.missPenalty(), event.getArrivalTime());
            burstEstimator.record(event.key(), event.missPenalty(), event.getArrivalTime());
            policyStats.recordMiss(event.missPenalty());
            policyStats.recordMissPenalty(event.missPenalty());
            updateNormalization(key);
        }

        if (entry != null) {
            admittor.record(event.key());
            recordAccordingToAvailability(entry, event);
        }

        final double hitPenaltyAfter = stats().hitPenalty();
        final double delayedHitPenaltyAfter = stats().delayedHitPenalty();
        final double missPenaltyAfter = stats().missPenalty();

        Assert.assertCondition((hitPenaltyAfter > hitPenaltyBefore
                                && delayedHitPenaltyAfter == delayedHitPenaltyBefore
                                && missPenaltyAfter == missPenaltyBefore)
                               || (hitPenaltyAfter == hitPenaltyBefore
                                   && delayedHitPenaltyAfter > delayedHitPenaltyBefore
                                   && missPenaltyAfter == missPenaltyBefore)
                               || (hitPenaltyAfter == hitPenaltyBefore
                                   && delayedHitPenaltyAfter == delayedHitPenaltyBefore
                                   && missPenaltyAfter > missPenaltyBefore) || event.hitPenalty() == 0,
                               () -> String.format("No stats update: Before: %.2f %.2f %.2f After: %.2f %.2f %.2f",
                                                   hitPenaltyBefore,
                                                   delayedHitPenaltyBefore,
                                                   missPenaltyBefore,
                                                   hitPenaltyAfter,
                                                   delayedHitPenaltyAfter,
                                                   missPenaltyAfter));
    }

    private void recordAccordingToAvailability(EntryData entry, AccessEvent currEvent) {
        boolean isAvailable = entry.event().isAvailableAt(currEvent.getArrivalTime());
        if (isAvailable) {
            currEvent.changeEventStatus(AccessEvent.EventStatus.HIT);
            policyStats.recordHit();
            policyStats.recordHitPenalty(currEvent.hitPenalty());
            burstEstimator.addValueToRecord(currEvent.key(), 0, currEvent.getArrivalTime());

            latencyEstimator.recordHit(currEvent.hitPenalty());
            burstEstimator.recordHit(currEvent.hitPenalty());
        } else {
            currEvent.changeEventStatus(AccessEvent.EventStatus.DELAYED_HIT);
            currEvent.setDelayedHitPenalty(entry.event().getAvailabilityTime());
            policyStats.recordDelayedHitPenalty(currEvent.delayedHitPenalty());
            policyStats.recordDelayedHit();
            latencyEstimator.addValueToRecord(currEvent.key(),
                                              currEvent.delayedHitPenalty(),
                                              currEvent.getArrivalTime());
            burstEstimator.addValueToRecord(currEvent.key(), currEvent.delayedHitPenalty(), currEvent.getArrivalTime());
        }
    }

    @Override
    public void dump() {
        burstBlock.dump();

        if (dumper != null) {
            dumper.flush();
            dumper.close();
        }
    }


    @Override
    public void finished() {
        final int windowSize = windowBlock.size();
        final int probationSize = probationBlock.size();
        final int protectedSize = protectedBlock.size();
        final int burstSize = burstBlock.size();
        final int totalSize = windowSize + probationSize + protectedSize + burstSize;

        Assert.assertCondition(totalSize <= cacheCapacity,
                               () -> String.format("size overflow: capacity: %d, window: %d probation: %d protected: %d burst: %d",
                                                   cacheCapacity,
                                                   windowSize,
                                                   probationSize,
                                                   protectedSize,
                                                   burstSize));

        final int windowCapacity = windowBlock.capacity();
        final int probationCapacity = probationBlock.capacity();
        final int protectedCapacity = protectedBlock.capacity();
        final int burstCapacity = burstBlock.capacity();
        Assert.assertCondition(windowCapacity + probationCapacity + protectedCapacity + burstCapacity == cacheCapacity,
                               () -> String.format("capacity mismatch, Expected: %d\tGot: window: %d, probation: %d, protected: %d, burst: %d",
                                                   cacheCapacity,
                                                   windowCapacity,
                                                   probationCapacity,
                                                   protectedCapacity,
                                                   burstCapacity));
    }

    @Override
    public boolean isPenaltyAware() {return true;}

    protected static final class WindowCAWithBBSettings extends BasicSettings {

        public WindowCAWithBBSettings(Config config) {
            super(config);
        }

        public List<Double> percentMain() {
            return config().getDoubleList("ca-bb-window.percent-main");
        }

        public double percentMainProtected() {
            return config().getDouble("ca-bb-window.percent-main-protected");
        }

        public double percentBurstBlock() {return config().getDouble("ca-bb-window.percent-burst-block");}

        public String burstEstimationStrategy() {return config().getString("ca-bb-window.burst-strategy");}

        public int agingWindowSize() {return config().getInt("ca-bb-window.aging-window-size");}

        public double ageSmoothFactor() {return config().getDouble("ca-bb-window.age-smoothing");}

        public int numOfPartitions() {return config().getInt("ca-bb-window.number-of-partitions");}

        public int decayFactor() {
            return config().getInt("ca-bb-window.cra.decayFactors");
        }

        public int maxLists() {
            return config().getInt("ca-bb-window.cra.max-lists");
        }
    }

    protected static class WindowCAWithBBStats extends PolicyStats {
        private long windowHitCount = 0;
        private long protectedHitCount = 0;
        private long probationHitCount = 0;
        private long burstBlockHitCount = 0;
        private double windowBenefit = 0;
        private double protectedBenefit = 0;
        private double probationBenefit = 0;
        private double missCosts = 0;
        private double burstBenefit = 0;
        private double totalBenefit = 0;
        private double timeframeLruBenefit = 0;
        private double timeframeLfuBenefit = 0;
        private double timeframeBcBenefit = 0;

        public WindowCAWithBBStats(String format, Object... args) {
            super(format, args);
            addMetric(Metric.of("Window Hit Rate", (DoubleSupplier) this::windowRate, PERCENT, true));
            addMetric(Metric.of("Protected Hit Rate", (DoubleSupplier) this::protectedRate, PERCENT, true));
            addMetric(Metric.of("Probation Hit Rate", (DoubleSupplier) this::probationRate, PERCENT, true));
            addMetric(Metric.of("Burst Hit Rate", (DoubleSupplier) this::burstBlockRate, PERCENT, true));
            addMetric(Metric.of("Window Benefit", (DoubleSupplier) this::windowBenefit, PERCENT, true));
            addMetric(Metric.of("Protected Benefit", (DoubleSupplier) this::protectedBenefit, PERCENT, true));
            addMetric(Metric.of("Probation Benefit", (DoubleSupplier) this::probationBenefit, PERCENT, true));
            addMetric(Metric.of("Burst Benefit", (DoubleSupplier) this::burstBenefit, PERCENT, true));
            addMetric(Metric.of("Miss Cost", (DoubleSupplier) this::missCost, PERCENT, true));
        }

        final public void recordWindowHit(double missPenalty) {
            ++this.windowHitCount;
            this.windowBenefit += missPenalty;
            this.totalBenefit += missPenalty;
            this.timeframeLruBenefit += missPenalty;
        }

        final public void recordProtectedHit(double missPenalty) {
            ++this.protectedHitCount;
            this.protectedBenefit += missPenalty;
            this.totalBenefit += missPenalty;
            this.timeframeLfuBenefit += missPenalty;
        }

        final public void recordProbationHit(double missPenalty) {
            ++this.probationHitCount;
            this.probationBenefit += missPenalty;
            this.totalBenefit += missPenalty;
            this.timeframeLfuBenefit += missPenalty;
        }

        final public void recordBurstBlockHit(double missPenalty) {
            ++this.burstBlockHitCount;
            this.burstBenefit += missPenalty;
            this.totalBenefit += missPenalty;
            this.timeframeBcBenefit += missPenalty;
        }

        public void recordMiss(double missPenalty) {
            super.recordMiss();
            this.missCosts += missPenalty;
            this.totalBenefit += missPenalty;
        }

        final protected double windowRate() { return (double) windowHitCount / requestCount(); }

        final protected double protectedRate() { return (double) protectedHitCount / requestCount(); }

        final protected double probationRate() { return (double) probationHitCount / requestCount(); }

        final protected double burstBlockRate() { return (double) burstBlockHitCount / requestCount(); }

        final public double windowBenefit()  {return windowBenefit / totalBenefit; }

        final public double protectedBenefit() {return protectedBenefit / totalBenefit;}

        final public double probationBenefit() {return probationBenefit / totalBenefit;}

        final public double burstBenefit() {return burstBenefit / totalBenefit;}

        final public double missCost() {return missCosts / totalBenefit;}

        public double timeframeLruBenefit() {
            return timeframeLruBenefit;
        }

        public double timeframeLfuBenefit() {
            return timeframeLfuBenefit;
        }

        public double timeframeBcBenefit() {
            return timeframeBcBenefit;
        }

        public void resetTimeframeCounters() {
            this.timeframeLruBenefit = 0;
            this.timeframeLfuBenefit = 0;
            this.timeframeBcBenefit = 0;
        }
    }
}
