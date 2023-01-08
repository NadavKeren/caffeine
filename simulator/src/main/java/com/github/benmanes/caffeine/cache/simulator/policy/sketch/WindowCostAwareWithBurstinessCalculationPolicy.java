package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.admission.Admittor;
import com.github.benmanes.caffeine.cache.simulator.admission.BALATinyLfu;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.LatencyEstimator;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.linked.CraBlock;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static java.util.stream.Collectors.toSet;

@Policy.PolicySpec(name = "sketch.WindowCABurstCal")
public class WindowCostAwareWithBurstinessCalculationPolicy implements Policy {
    private final Long2ObjectMap<CraBlock.Node> data;
    private final PolicyStats policyStats;
    private final LatencyEstimator<Long> latencyEstimator;
    private final LatencyEstimator<Long> burstEstimator;
    private final Admittor admittor;
    private final long maximumSize;

    private final CraBlock headWindow;
    private final CraBlock headProbation;
    private final CraBlock headProtected;

    private final long maxWindow;
    private final long maxProtected;

    private int sizeWindow;
    private int sizeProtected;
    private double normalizationBias;
    private double normalizationFactor;
    private double maxDelta;
    private int maxDeltaCounts;
    private int samplesCount;

    private static final System.Logger logger = System.getLogger(LatencyEstimator.class.getName());

    public WindowCostAwareWithBurstinessCalculationPolicy(double percentMain,
                                                          WindowCostAwareWithBurstinessCalculationPolicy.WindowCASettings settings,
                                                          int k,
                                                          int maxLists) {
        this.policyStats = new PolicyStats("sketch.WindowCABurstCalculation (%.0f%%,k=%d,maxLists=%d)",
                                           100 * (1.0d - percentMain),
                                           k,
                                           maxLists);
        this.latencyEstimator = createEstimator(settings.config());
        this.burstEstimator = new BurstLatencyEstimator<>();
        this.admittor = new BALATinyLfu(settings.config(), policyStats, latencyEstimator, burstEstimator, settings.burstinessWeight());
        int maxMain = (int) (settings.maximumSize() * percentMain);
        this.maxProtected = (int) (maxMain * settings.percentMainProtected());
        this.maxWindow = settings.maximumSize() - maxMain;
        this.data = new Long2ObjectOpenHashMap<>();
        this.maximumSize = settings.maximumSize();
        this.headProtected = new CraBlock(k, maxLists, this.maxProtected, latencyEstimator);
        this.headProbation = new CraBlock(k, maxLists, maxMain - this.maxProtected, latencyEstimator);
        this.headWindow = new CraBlock(k, maxLists, this.maxWindow, latencyEstimator);
        this.normalizationBias = 0;
        this.normalizationFactor = 0;
        this.maxDelta = 0;
        this.maxDeltaCounts = 0;
        this.samplesCount = 0;
    }

    private LatencyEstimator<Long> createEstimator(Config config) {
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

        logger.log(System.Logger.Level.DEBUG,
                String.format("Created estimator of type %s, class: %s",
                        estimationType,
                        estimator.getClass().getName()));

        return estimator;
    }

    /**
     * Returns all variations of this policy based on the configuration parameters.
     */
    public static Set<Policy> policies(Config config) {
        var settings = new WindowCostAwareWithBurstinessCalculationPolicy.WindowCASettings(config);
        return settings.percentMain().stream()
                .flatMap(percentMain ->
                        settings.kValues().stream()
                                .map(k -> settings.maxLists().stream()
                                        .map(maxLists -> new WindowCostAwareWithBurstinessCalculationPolicy(percentMain, settings, k, maxLists)
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
        long key = event.key();
        event.changeEventStatus(AccessEvent.EventStatus.MISS);
        admittor.record(event);
        CraBlock.Node n = headWindow.addEntry(event);
        data.put(key, n);
        sizeWindow++;
        evict();
    }

    /**
     * Moves the entry to the MRU position in the admission window.
     */
    private void onWindowHit(CraBlock.Node node) {
        node.moveToTail();
    }

    /**
     * Promotes the entry to the protected region's MRU position, demoting an entry if necessary.
     */
    private void onProbationHit(CraBlock.Node node) {
        node.remove();
        headProbation.remove(node.key());
        headProtected.addEntry(node);

        sizeProtected++;
        if (sizeProtected > maxProtected) {
            CraBlock.Node demote = headProtected.findVictim();
            demote.remove();
            headProtected.remove(demote.key());
            headProbation.addEntry(demote);
            sizeProtected--;
        }
    }

    /**
     * Moves the entry to the MRU position, if it falls outside the fast-path threshold.
     */
    private void onProtectedHit(CraBlock.Node node) {
        node.moveToTail();
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

        headProtected.setNormalization(normalizationBias, normalizationFactor);
        headProbation.setNormalization(normalizationBias, normalizationFactor);
        headWindow.setNormalization(normalizationBias, normalizationFactor);
    }
    /**
     * Evicts an item from the admission window into the probation space.
     * Evicts an item from the probation space, when beneficial, admit to the burst cache.
     * If needed, evict an item from the burst Cache.
     */
    private void evict() {
        if (sizeWindow <= maxWindow) {
            return;
        }
        CraBlock.Node candidate = headWindow.findVictim();
        sizeWindow--;
        candidate.remove();
        headWindow.remove(candidate.key());
        headProbation.addEntry(candidate);
        if (data.size() > maximumSize) {
            CraBlock.Node victim = headProbation.findVictim();
            CraBlock.Node freqEvict = admittor.admit(candidate.event(), victim.event()) ? victim : candidate;
            removeFromProbation(freqEvict);

            policyStats.recordEviction();
        }
    }

    private void removeFromProbation(CraBlock.Node node) {
        node.remove();
        headProbation.remove(node.key());
        data.remove(node.key());
    }

    @Override
    public void record(AccessEvent event) {
        long key = event.key();
        policyStats.recordOperation();
        CraBlock.Node node = data.get(key);

        if (node == null) {
            onMiss(event);
            latencyEstimator.record(event.key(), event.missPenalty());
            burstEstimator.record(event.key(), event.missPenalty());
            policyStats.recordMiss();
            policyStats.recordMissPenalty(event.missPenalty());
            updateNormalization(key);
        } else {
            recordStatsByAvailabilityStatus(event, node.event());

            admittor.record(event.key());

            if (headWindow.isHit(key)) {
                onWindowHit(node);
            } else if (headProbation.isHit(key)) {
                onProbationHit(node);
            } else if (headProtected.isHit(key)) {
                onProtectedHit(node);
            } else {
                throw new IllegalStateException();
            }
        }
    }

    /***
     * Updates the currently recorded event, the estimators and the policy stats according to the availability status
     * of the item.
     *
     * @param currEvent - The event currently being recorded by the policy
     * @param admittedEvent - The event in which the item was introduced to the cache
     * @throws IllegalArgumentException if the key doesn't exist in the latency estimators.
     */
    private void recordStatsByAvailabilityStatus(AccessEvent currEvent, AccessEvent admittedEvent) {
        boolean isAvailable = admittedEvent.isAvailableAt(currEvent.getArrivalTime());
        if (isAvailable) {
            currEvent.changeEventStatus(AccessEvent.EventStatus.HIT);
            policyStats.recordHit();
            policyStats.recordHitPenalty(currEvent.hitPenalty());
        } else {
            currEvent.changeEventStatus(AccessEvent.EventStatus.DELAYED_HIT);
            currEvent.setDelayedHitPenalty(admittedEvent.getAvailabilityTime());
            policyStats.recordDelayedHitPenalty(currEvent.delayedHitPenalty());
            policyStats.recordDelayedHit();
            latencyEstimator.addValueToRecord(currEvent.key(), currEvent.delayedHitPenalty());
            burstEstimator.addValueToRecord(currEvent.key(), currEvent.delayedHitPenalty());
        }

//        logger.log(System.Logger.Level.DEBUG,
//                   String.format("key: %d\tevent type: %s\tarrival time: %f\tavailability time: %f\tIs available: %b",
//                                 currEvent.key(),
//                                 currEvent.getStatus(),
//                                 currEvent.getArrivalTime(),
//                                 admittedEvent.getAvailabilityTime(),
//                                 isAvailable));
    }

    @Override
    public void finished() {
        long windowSize = data.values().stream().filter(n -> headWindow.isHit(n.key())).count();
        long probationSize = data.values().stream().filter(n -> headProbation.isHit(n.key())).count();
        long protectedSize = data.values().stream().filter(n -> headProtected.isHit(n.key())).count();
        checkState(windowSize == sizeWindow);
        checkState(protectedSize == sizeProtected);
        checkState(probationSize == data.size() - windowSize - protectedSize);

        checkState(data.size() <= maximumSize);
    }

    @Override
    public boolean isPenaltyAware() { return true; }

    enum Status {
        WINDOW, PROBATION, PROTECTED
    }

    public static final class WindowCASettings extends BasicSettings {

        public WindowCASettings(Config config) {
            super(config);
        }

        public List<Double> percentMain() {
            return config().getDoubleList("ca-window.percent-main");
        }

        public double percentMainProtected() {
            return config().getDouble("ca-window.percent-main-protected");
        }

        public List<Integer> kValues() {
            return config().getIntList("ca-window.cra.k");
        }

        public List<Integer> maxLists() {
            return config().getIntList("ca-window.cra.max-lists");
        }

        public double burstinessWeight() { return config().getDouble("ca-window.burstiness.weight"); }
    }
}
