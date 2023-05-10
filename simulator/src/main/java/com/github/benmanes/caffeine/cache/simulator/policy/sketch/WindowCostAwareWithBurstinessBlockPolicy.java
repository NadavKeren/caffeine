package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.admission.Admittor;
import com.github.benmanes.caffeine.cache.simulator.admission.LATinyLfu;
import com.github.benmanes.caffeine.cache.simulator.policy.*;
import com.github.benmanes.caffeine.cache.simulator.policy.linked.CraBlock;
import com.typesafe.config.Config;

import java.util.List;
import java.util.Set;
import java.util.function.DoubleSupplier;

import static com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats.Metric.MetricType.PERCENT;
import static com.google.common.base.Preconditions.checkState;
import static java.util.stream.Collectors.toSet;

@Policy.PolicySpec(name = "sketch.WindowCABurstBlock")
public class WindowCostAwareWithBurstinessBlockPolicy implements Policy {
    private final WindowCAWithBBStats policyStats;
    private final LatencyEstimator<Long> latencyEstimator;
    private final LatencyEstimator<Long> burstEstimator;
    private final Admittor admittor;
    private final long cacheCapacity;
    private final long nonBurstCacheCapacity;

    private final CraBlock windowBlock;
    private final CraBlock probationBlock;
    private final CraBlock protectedBlock;
    private final BurstBlock burstBlock;

    private double normalizationBias;
    private double normalizationFactor;
    private double maxDelta;
    private int maxDeltaCounts;
    private int samplesCount;

    private static final System.Logger logger = System.getLogger(LatencyEstimator.class.getName());

    public WindowCostAwareWithBurstinessBlockPolicy(double percentMain,
                                                    WindowCAWithBBSettings settings,
                                                    int decayFactor,
                                                    int maxLists) {
        this.policyStats = new WindowCAWithBBStats("sketch.WindowCAWithBB (%.0f%%,decayFactor=%d,maxLists=%d)",
                                                   100 * (1.0d - percentMain),
                                                   decayFactor,
                                                   maxLists);
        this.latencyEstimator = createLatencyEstimator(settings);
        this.burstEstimator = createBurstEstimator(settings);
        this.admittor = new LATinyLfu(settings.config(), policyStats, latencyEstimator);

        this.cacheCapacity = settings.maximumSize();

        final int burstCacheCapacity = (int) (settings.percentBurstBlock() * cacheCapacity);
        this.nonBurstCacheCapacity = cacheCapacity - burstCacheCapacity;

        final long mainCacheSize = (long) (nonBurstCacheCapacity * percentMain);
        final long protectedCapacity = (long) (mainCacheSize * settings.percentMainProtected());
        final long windowCapacity = nonBurstCacheCapacity - mainCacheSize;

        this.protectedBlock = new CraBlock(decayFactor, maxLists, protectedCapacity, latencyEstimator);
        this.probationBlock = new CraBlock(decayFactor, maxLists, mainCacheSize - protectedCapacity, latencyEstimator);
        this.windowBlock = new CraBlock(decayFactor, maxLists, windowCapacity, latencyEstimator);
        this.burstBlock = new BurstBlock(burstCacheCapacity, burstEstimator);

        this.normalizationBias = 0;
        this.normalizationFactor = 0;
        this.maxDelta = 0;
        this.maxDeltaCounts = 0;
        this.samplesCount = 0;
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
                                 estimator.getClass().getName()));

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
                       .flatMap(percentMain ->
                                        settings.decayFactors().stream()
                                                .map(k -> settings.maxLists().stream()
                                                                  .map(ml -> new WindowCostAwareWithBurstinessBlockPolicy(percentMain, settings, k,ml)
                                                                  )))
                       .flatMap(x -> x)
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
        evict(event.getArrivalTime());
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

        if (samplesCount % 1000 == 0 || normalizationFactor == 0){
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
    private void evict(double eventTime) {
        if (!windowBlock.isFull()) {
            return;
        }

        EntryData windowBlockVictim = windowBlock.removeVictim();
        probationBlock.addEntry(windowBlockVictim);
        if (windowCASize() > nonBurstCacheCapacity) {
            EntryData probationBlockVictim = probationBlock.findVictim();
            EntryData evict = admittor.admit(windowBlockVictim.event(), probationBlockVictim.event())
                              ? probationBlockVictim
                              : windowBlockVictim;

            probationBlock.remove(evict.key());

            final double evictScore =  burstEstimator.getLatencyEstimation(evict.key(), eventTime);
            if (burstBlock.isFull()) {
                final EntryData burstVictim = burstBlock.getVictim();
                final double burstVictimScore = burstEstimator.getLatencyEstimation(burstVictim.key(), eventTime);

                if (evictScore >= burstVictimScore) {
                    burstBlock.evict();
                    burstBlock.admit(evict);
                    policyStats.recordEviction();
                }
            } else {
                burstBlock.admit(evict);
            }
        }
    }

    private long windowCASize() {
        return windowBlock.size() + protectedBlock.size() + probationBlock.size();
    }

    @Override
    public void record(AccessEvent event) {
        final long key = event.key();
        policyStats.recordOperation();
        EntryData entry = null;
//        boolean isBurstBlockHit = false;

        if (windowBlock.isHit(key)) {
            entry = windowBlock.get(key);
            policyStats.recordWindowHit();
            onWindowHit(entry);
        } else if (probationBlock.isHit(key)) {
            entry = probationBlock.get(key);
            policyStats.recordProbationHit();
            onProbationHit(entry);
        } else if (protectedBlock.isHit(key)) {
            entry = protectedBlock.get(key);
            policyStats.recordProtectedHit();
            onProtectedHit(entry);
        } else if (burstBlock.isHit(key)){
            entry = burstBlock.get(key);
            policyStats.recordBurstBlockHit();
//            isBurstBlockHit = true;
        } else {
            onMiss(event);
            latencyEstimator.record(event.key(), event.missPenalty(), event.getArrivalTime());
            burstEstimator.record(event.key(), event.missPenalty(), event.getArrivalTime());
            policyStats.recordMiss();
            policyStats.recordMissPenalty(event.missPenalty());
            updateNormalization(key);
        }

        if (entry != null) {
            admittor.record(event.key());
            recordAccordingToAvailability(entry, event);
        }
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
            latencyEstimator.addValueToRecord(currEvent.key(), currEvent.delayedHitPenalty(), currEvent.getArrivalTime());
            burstEstimator.addValueToRecord(currEvent.key(), currEvent.delayedHitPenalty(), currEvent.getArrivalTime());
        }
    }

    @Override
    public void dump() { burstBlock.dump(); }



    @Override
    public void finished() {
        checkState(windowCASize() <= cacheCapacity);
    }

    @Override
    public boolean isPenaltyAware() { return true; }

    private static final class WindowCAWithBBSettings extends BasicSettings {

        public WindowCAWithBBSettings(Config config) {
            super(config);
        }

        public List<Double> percentMain() {
            return config().getDoubleList("ca-bb-window.percent-main");
        }

        public double percentMainProtected() {
            return config().getDouble("ca-bb-window.percent-main-protected");
        }

        public double percentBurstBlock() { return config().getDouble("ca-bb-window.percent-burst-block"); }

        public String burstEstimationStrategy() { return config().getString("ca-bb-window.burst-strategy"); }

        public int agingWindowSize() { return config().getInt("ca-bb-window.aging-window-size"); }
        public double ageSmoothFactor() { return config().getDouble("ca-bb-window.age-smoothing"); }

        public int numOfPartitions() { return config().getInt("ca-bb-window.number-of-partitions"); }

        public List<Integer> decayFactors() {
            return config().getIntList("ca-bb-window.cra.decayFactors");
        }

        public List<Integer> maxLists() {
            return config().getIntList("ca-bb-window.cra.max-lists");
        }
    }

    protected static class WindowCAWithBBStats extends PolicyStats {
        private long windowHitCount = 0;
        private long protectedHitCount = 0;
        private long probationHitCount = 0;
        private long burstBlockHitCount = 0;

        public WindowCAWithBBStats(String format, Object... args) {
            super(format, args);
            addMetric(Metric.of("Window Hit Rate", (DoubleSupplier) this::windowRate, PERCENT, true));
            addMetric(Metric.of("Protected Hit Rate", (DoubleSupplier) this::protectedRate, PERCENT, true));
            addMetric(Metric.of("Probation Hit Rate", (DoubleSupplier) this::probationRate, PERCENT, true));
            addMetric(Metric.of("Burst Hit Rate", (DoubleSupplier) this::burstBlockRate, PERCENT, true));
        }

        final public void recordWindowHit() { ++this.windowHitCount; }
        final public void recordProtectedHit() { ++this.protectedHitCount; }
        final public void recordProbationHit() { ++this.probationHitCount; }
        final public void recordBurstBlockHit() { ++this.burstBlockHitCount; }

        final protected double windowRate() { return (double) windowHitCount / requestCount(); }
        final protected double protectedRate() { return (double) protectedHitCount / requestCount(); }
        final protected double probationRate() { return (double) probationHitCount / requestCount(); }
        final protected double burstBlockRate() { return (double) burstBlockHitCount / requestCount(); }
    }
}
