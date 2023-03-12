package com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.admission.Admittor;
import com.github.benmanes.caffeine.cache.simulator.admission.LATinyLfu;
import com.github.benmanes.caffeine.cache.simulator.policy.*;
import com.github.benmanes.caffeine.cache.simulator.policy.linked.CraBlock;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.*;
import com.typesafe.config.Config;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.DoubleSupplier;

import static com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats.Metric.MetricType.PERCENT;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Locale.US;
import static java.util.stream.Collectors.toSet;


/**
 * The Adaptive Cost-Aware with additional Burst-Heavy items block.
 * The size of the admission window is adjusted using the latency
 * aware hill climbing algorithm.
 *
 * @author nadav.keren@gmail.com (Nadav Keren)
 */

@Policy.PolicySpec(name = "sketch.AdaptiveCAWithBB")
public class AdaptiveCAWithBurstBlockPolicy implements Policy {

    private final double initialPercentMain;
    private final AdaptiveCAWithBBStats policyStats;
    private final LAHillClimber climber;
    private final LatencyEstimator<Long> latencyEstimator;
    private final LatencyEstimator<Long> burstEstimator;
    private final Admittor admittor;
    private final long cacheCapacity;

    private final CraBlock probationBlock;
    private final CraBlock protectedBlock;
    private final CraBlock windowBlock;
    private final BurstBlock burstBlock;

    private long windowCapacity;
    private long protectedCapacity;

    private final long burstBlockCapacity;
    private final long nonBurstCacheCapacity;

    private double windowSize;
    private double protectedSize;

    static final boolean TRACE = false;

    private double normalizationBias;
    private double normalizationFactor;
    private double maxDelta;
    private int maxDeltaCounts;
    private int samplesCount;


    public AdaptiveCAWithBurstBlockPolicy(LAHillClimberType strategy,
                                          double percentMain,
                                          AdaptiveCAWithBBSettings settings,
                                          double decayFactor,
                                          int maxLists) {
        this.policyStats = new AdaptiveCAWithBBStats("sketch.AdaptiveCAWithBB (%s)(k=%.2f,maxLists=%d)",
                                                     strategy.name().toLowerCase(US),
                                                     decayFactor,
                                                     maxLists);

        this.cacheCapacity = settings.maximumSize();
        this.burstBlockCapacity = (long) (cacheCapacity * settings.percentBurstBlock());
        this.nonBurstCacheCapacity = cacheCapacity - burstBlockCapacity;

        this.latencyEstimator = createLatencyEstimator(settings.config());
        this.burstEstimator = createBurstEstimator(settings);

        int mainCacheCapacity = (int) (nonBurstCacheCapacity * percentMain);
        this.protectedCapacity = (int) (mainCacheCapacity * settings.percentMainProtected());
        this.windowCapacity = nonBurstCacheCapacity - mainCacheCapacity;

        this.protectedBlock = new CraBlock(decayFactor, maxLists, this.protectedCapacity, latencyEstimator);
        this.probationBlock = new CraBlock(decayFactor, maxLists, mainCacheCapacity - this.protectedCapacity, latencyEstimator);
        this.windowBlock = new CraBlock(decayFactor, maxLists, this.windowCapacity, latencyEstimator);
        this.burstBlock = new BurstBlock(this.burstBlockCapacity);

        this.initialPercentMain = percentMain;
        this.admittor = new LATinyLfu(settings.config(), policyStats, latencyEstimator);
        this.climber = strategy.create(settings.config());
        this.normalizationBias = 0;
        this.normalizationFactor = 0;
        this.maxDelta = 0;
        this.maxDeltaCounts = 0;
        this.samplesCount = 0;

        printSegmentSizes();
    }

    private LatencyEstimator<Long> createLatencyEstimator(Config config) {
        BasicSettings settings = new BasicSettings(config);
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



        return estimator;
    }

    private LatencyEstimator<Long> createBurstEstimator(AdaptiveCAWithBBSettings settings) {
        LatencyEstimator<Long> estimator;
        String strategy = settings.burstEstimationStrategy();
        switch (strategy) {
            case "naive":
                estimator = new NaiveBurstLatencyEstimator<>();
                break;
            case "moving-average":
                estimator = new MovingAverageBurstLatencyEstimator<>(settings.smoothingFactor(), settings.numOfPartitions());
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
        AdaptiveCAWithBBSettings settings = new AdaptiveCAWithBBSettings(config);
        Set<Policy> policies = new HashSet<>();
        for (LAHillClimberType climber : settings.strategy()) {
            for (double percentMain : settings.percentMain()) {
                for (double decayFactor : settings.decayFactors()) {
                    for (int maxNumOfLists : settings.maxLists()) {
                        Policy policy = new AdaptiveCAWithBurstBlockPolicy(climber,
                                                                           percentMain,
                                                                           settings,
                                                                           decayFactor,
                                                                           maxNumOfLists);
                        policies.add(policy);
                    }
                }
            }
        }
        return policies;
    }

    @Override
    public PolicyStats stats() {
        return policyStats;
    }

    @Override
    public void record(AccessEvent event) {
        final long key = event.key();
        policyStats.recordOperation();
        EntryData entry = null;
        admittor.record(event);

        LAHillClimber.QueueType queue = null;
        if (windowBlock.isHit(key)) {
            entry = windowBlock.get(key);
            policyStats.recordWindowHit();
            onWindowHit(entry);
            queue = LAHillClimber.QueueType.WINDOW;
        } else if (probationBlock.isHit(key)) {
            entry = probationBlock.get(key);
            policyStats.recordProbationHit();
            onProbationHit(entry);
            queue = LAHillClimber.QueueType.PROBATION;
        } else if (protectedBlock.isHit(key)) {
            entry = protectedBlock.get(key);
            policyStats.recordProtectedHit();
            onProtectedHit(entry);
            queue = LAHillClimber.QueueType.PROTECTED;
        } else if (burstBlock.isHit(key)) {
            entry = burstBlock.get(key);
            policyStats.recordBurstBlockHit();
            queue = LAHillClimber.QueueType.OTHER;
        } else {
            updateNormalization(event.key());
            onMiss(event);
            latencyEstimator.record(event.key(), event.missPenalty(), event.getArrivalTime());
            burstEstimator.record(event.key(), event.missPenalty(), event.getArrivalTime());
            policyStats.recordMiss();
            policyStats.recordMissPenalty(event.missPenalty());
        }

        if (entry != null) {
            recordAccordingToAvailability(entry, event);
        }

        final boolean isFull = (mainCacheSize() >= cacheCapacity);
        climb(event, queue, isFull);
    }

    private void recordAccordingToAvailability(EntryData entry, AccessEvent currEvent) {
        boolean isAvailable = entry.event().isAvailableAt(currEvent.getArrivalTime());
        if (isAvailable) {
            currEvent.changeEventStatus(AccessEvent.EventStatus.HIT);
            policyStats.recordHit();
            policyStats.recordHitPenalty(currEvent.hitPenalty());
            burstEstimator.addValueToRecord(currEvent.key(), 0, currEvent.getArrivalTime());
        } else {
            currEvent.changeEventStatus(AccessEvent.EventStatus.DELAYED_HIT);
            currEvent.setDelayedHitPenalty(entry.event().getAvailabilityTime());
            policyStats.recordDelayedHitPenalty(currEvent.delayedHitPenalty());
            policyStats.recordDelayedHit();
            latencyEstimator.addValueToRecord(currEvent.key(), currEvent.delayedHitPenalty(), currEvent.getArrivalTime());
            burstEstimator.addValueToRecord(currEvent.key(), currEvent.delayedHitPenalty(), currEvent.getArrivalTime());
        }
    }

    private void updateNormalization(long key) {
        double delta = latencyEstimator.getDelta(key);

        if (delta > normalizationFactor){
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
     * Adds the entry to the admission window, evicting if necessary.
     */
    private void onMiss(AccessEvent event) {
        windowBlock.addEntry(event);
        windowSize++;
        evict();
    }

    /**
     * Moves the entry to the MRU position in the admission window.
     */
    private void onWindowHit(EntryData entryData) {
        windowBlock.moveToTail(entryData);
    }

    /**
     * Promotes the entry to the protected region's MRU position, demoting an entry if necessary.
     */
    private void onProbationHit(EntryData entry) {
        probationBlock.remove(entry.key());
        protectedBlock.addEntry(entry);
        protectedSize++;
        demoteProtected();
    }

    private void demoteProtected() {
        if (protectedSize > protectedCapacity) {
            EntryData demote = protectedBlock.removeVictim();
            probationBlock.addEntry(demote);
            protectedSize--;
        }
    }

    /**
     * Moves the entry to the MRU position, if it falls outside the fast-path threshold.
     */
    private void onProtectedHit(EntryData entry) {
        protectedBlock.moveToTail(entry);
    }

    private long mainCacheSize() { return windowBlock.size() + protectedBlock.size() + probationBlock.size(); }

    /**
     * Evicts from the admission window into the probation space. If the size exceeds the maximum,
     * then the admission candidate and probation's victim are evaluated and one is evicted.
     */
    private void evict() {
        if (!windowBlock.isFull()) {
            return;
        }

        EntryData candidate = windowBlock.removeVictim();
        --windowSize;

        probationBlock.addEntry(candidate);
        if (mainCacheSize() > nonBurstCacheCapacity) {
            EntryData probationBlockVictim = probationBlock.findVictim();
            EntryData evict = admittor.admit(candidate.event(), probationBlockVictim.event())
                              ? probationBlockVictim
                              : candidate;

            probationBlock.remove(evict.key());

            if (burstBlockCapacity > 0) {
                final double evictScore =  burstEstimator.getLatencyEstimation(evict.key());
                if (burstBlock.isFull()) {
                    final double burstVictimScore = burstBlock.getVictimScore();

                    if (evictScore >= burstVictimScore) {
                        burstBlock.evict();
                        burstBlock.admit(evict, evictScore);
                        policyStats.recordEviction();
                    }
                } else {
                    burstBlock.admit(evict, evictScore);
                }
            }
        }
    }

    /**
     * Performs the hill climbing process.
     */
    private void climb(AccessEvent event, LAHillClimber.QueueType queue, boolean isFull) {
        if (queue == null) {
            climber.onMiss(event, isFull);
        } else {
            climber.onHit(event, queue, isFull);
        }

        double probationSize = cacheCapacity - windowSize - protectedSize;
        LAHillClimber.Adaptation adaptation = climber
                .adapt(windowSize, probationSize, protectedSize, isFull);
        if (adaptation.type == LAHillClimber.AdaptationType.INCREASE_WINDOW) {
            increaseWindow(adaptation.amount);
        } else if (adaptation.type == LAHillClimber.AdaptationType.DECREASE_WINDOW) {
            decreaseWindow(adaptation.amount);
        }
    }

    private void increaseWindow(double amount) {
        checkState(amount >= 0.0);
        if (protectedCapacity == 0) {
            return;
        }

        double increaseAmount = Math.min(amount, (double) protectedCapacity);
        int numOfItemsToMove = (int) (windowSize + increaseAmount) - (int) windowSize;
        windowSize += increaseAmount;

        for (int i = 0; i < numOfItemsToMove; i++) {
            ++windowCapacity;
            --protectedCapacity;

            demoteProtected();
            EntryData candidate = probationBlock.removeVictim();
            windowBlock.addEntry(candidate);
        }
        checkState(windowSize >= 0);
        checkState(windowCapacity >= 0);
        checkState(protectedCapacity >= 0);

        if (TRACE) {
            System.out.printf("+%,d (%,d -> %,d)%n", numOfItemsToMove, windowCapacity - numOfItemsToMove, windowCapacity);
        }
    }

    private void decreaseWindow(double amount) {
        checkState(amount >= 0.0);
        if (windowCapacity == 0) {
            return;
        }

        double quota = Math.min(amount, windowSize);
        int steps = (int) Math.min((int) windowSize - (int) (windowSize - quota), windowCapacity);
        windowSize -= quota;

        for (int i = 0; i < steps; i++) {
            windowCapacity--;
            protectedCapacity++;
            EntryData candidate = windowBlock.removeVictim();
            probationBlock.addEntry(candidate);
        }
        checkState(windowSize >= 0);
        checkState(windowCapacity >= 0);
        checkState(protectedCapacity >= 0);

        if (TRACE) {
            System.out.printf("-%,d (%,d -> %,d)%n", steps, windowCapacity + steps, windowCapacity);
        }
    }

    private void printSegmentSizes() {
        if (TRACE) {
            System.out.printf(
                    "maxWindow=%d, maxProtected=%d, percentWindow=%.1f",
                    windowCapacity, protectedCapacity, (100.0 * windowCapacity) / cacheCapacity);
        }
    }

    @Override
    public void finished() {
        policyStats.setPercentAdaption(
                (windowCapacity / (double) cacheCapacity) - (1.0 - initialPercentMain));
        printSegmentSizes();

        checkState(Math.abs(windowSize - windowBlock.size()) < 2,
                   "Window: %s != %s",
                   (long) windowSize,
                   windowBlock.size());

        checkState(Math.abs(protectedSize - protectedBlock.size()) < 2,
                   "Protected: %s != %s",
                   (long) protectedSize,
                   protectedBlock.size());

        checkState(mainCacheSize() <= nonBurstCacheCapacity,
                   "Maximum: %s > %s",
                   mainCacheSize(),
                   nonBurstCacheCapacity);
    }

    private static final class AdaptiveCAWithBBSettings extends BasicSettings {

        public AdaptiveCAWithBBSettings(Config config) {
            super(config);
        }

        public List<Double> percentMain() {
            return config().getDoubleList("ca-bb-hill-climber-window.percent-main");
        }

        public double percentMainProtected() {
            return config().getDouble("ca-bb-hill-climber-window.percent-main-protected");
        }

        public double percentBurstBlock() {
            return config().getDouble("ca-bb-hill-climber-window.percent-burst-block");
        }

        public String burstEstimationStrategy() { return config().getString("ca-bb-hill-climber-window.burst-strategy"); }

        public double smoothingFactor() { return config().getDouble("ca-bb-hill-climber-window.smoothing-factor"); }

        public int numOfPartitions() { return config().getInt("ca-bb-hill-climber-window.number-of-partitions"); }

        public Set<LAHillClimberType> strategy() {
            return config().getStringList("ca-bb-hill-climber-window.strategy").stream()
                           .map(strategy -> strategy.replace('-', '_').toUpperCase(US))
                           .map(LAHillClimberType::valueOf)
                           .collect(toSet());
        }

        public List<Integer> decayFactors() {
            return config().getIntList("ca-bb-hill-climber-window.cra.decayFactors");
        }

        public List<Integer> maxLists() {
            return config().getIntList("ca-bb-hill-climber-window.cra.max-lists");
        }
    }

    protected static class AdaptiveCAWithBBStats extends PolicyStats {
        private long windowHitCount = 0;
        private long protectedHitCount = 0;
        private long probationHitCount = 0;
        private long burstBlockHitCount = 0;

        public AdaptiveCAWithBBStats(String format, Object... args) {
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
