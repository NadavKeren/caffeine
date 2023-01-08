package com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.admission.Admittor;
import com.github.benmanes.caffeine.cache.simulator.admission.LATinyLfu;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.LatencyEstimator;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.linked.CraBlock;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing.LAHillClimber.QueueType;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing.LAHillClimber.AdaptationType;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.*;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Locale.US;
import static java.util.stream.Collectors.toSet;

@Policy.PolicySpec(name = "sketch.AdaptiveCAWithBB")
public class AdaptiveCAWithBurstBlockPolicy implements Policy {
    private final double initialPercentMain;
    private final Long2ObjectMap<CraBlock.Node> data;
    private final PolicyStats policyStats;
    private final LAHillClimber climber;
    private final LatencyEstimator<Long> latencyEstimator;
    private final LatencyEstimator<Long> burstEstimator;
    private final Admittor admittor;
    private final long maximumWLFUSize;

    private final CraBlock headProbation;
    private final CraBlock headProtected;
    private final CraBlock headWindow;
    private final BurstBlock burstBlock;

    private long maxWindow;
    private long maxProtected;

    private double windowSize;
    private double protectedSize;

    static final boolean debug = false;
    static final boolean trace = false;
    double k;

    private double normalizationBias;
    private double normalizationFactor;
    private double maxDelta;
    private int maxDeltaCounts;
    private int samplesCount;


    public AdaptiveCAWithBurstBlockPolicy(LAHillClimberType strategy,
                                          double percentMain,
                                          double percentBurst,
                                          AdaptiveCAWithBurstBlockSettings settings,
                                          double k,
                                          int maxLists) {
        this.latencyEstimator = createEstimator(settings.config());
        this.burstEstimator = new BurstLatencyEstimator<>();
        long burstBlockSize = (long) (settings.maximumSize() * percentBurst);
        this.maximumWLFUSize = settings.maximumSize() - burstBlockSize;
        int maxMain = (int) (maximumWLFUSize * percentMain);
        this.maxProtected = (int) (maxMain * settings.percentMainProtected());
        this.maxWindow = maximumWLFUSize - maxMain;
        this.data = new Long2ObjectOpenHashMap<>();
        this.headProtected = new CraBlock(k, maxLists, this.maxProtected, latencyEstimator);
        this.headProbation = new CraBlock(k, maxLists, maxMain - this.maxProtected, latencyEstimator);
        this.headWindow = new CraBlock(k, maxLists, this.maxWindow, latencyEstimator);
        this.burstBlock = new BurstBlock(burstBlockSize);
        this.initialPercentMain = percentMain;
        this.policyStats = new PolicyStats("CAWithBBClimber (%s)(k=%.2f,maxLists=%d,WTLFU=%d,BB=%d)",
                strategy.name().toLowerCase(US), k, maxLists, this.maximumWLFUSize, burstBlockSize);
        this.admittor = new LATinyLfu(settings.config(), policyStats, latencyEstimator);
        this.climber = strategy.create(settings.config());
        this.k = k;
        this.normalizationBias = 0;
        this.normalizationFactor = 0;
        this.maxDelta = 0;
        this.maxDeltaCounts = 0;
        this.samplesCount = 0;

        printSegmentSizes();
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
            case "latest-with-delayed-hits":
                estimator = new BurstLatencyEstimator<>();
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

    /**
     * Returns all variations of this policy based on the configuration parameters.
     */
    public static Set<Policy> policies(Config config) {
        var settings = new AdaptiveCAWithBurstBlockSettings(config);
        Set<Policy> policies = new HashSet<>();
        for (LAHillClimberType climber : settings.strategy()) {
            for (double percentMain : settings.percentMain()) {
                for (double k : settings.kValues()) {
                    for (int ml : settings.maxLists()) {
                        policies.add(new AdaptiveCAWithBurstBlockPolicy(climber,
                                                                        percentMain,
                                                                        settings.percentBurstBlock(),
                                                                        settings,
                                                                        k,
                                                                        ml));
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
        long key = event.key();
        boolean isFull = (data.size() >= maximumWLFUSize);
        policyStats.recordOperation();
        CraBlock.Node node = data.get(key);
        admittor.record(event);

        QueueType queue = null;
        if (node == null) {
            BurstBlock.Node burstNode = burstBlock.isHit(key, event.getArrivalTime());
            if (burstNode != null) {
                recordStatsByAvailabilityStatus(event, burstNode.getEvent());
            } else {
                latencyEstimator.record(event.key(), event.missPenalty());
                burstEstimator.record(event.key(), event.missPenalty());

                updateNormalization(event.key());
                onMiss(event);
                policyStats.recordMiss();
                policyStats.recordMissPenalty(event.missPenalty());
            }
        } else {
            recordStatsByAvailabilityStatus(event, node.event());

            if (headWindow.isHit(key)) {
                onWindowHit(node);
                queue = QueueType.WINDOW;
            } else if (headProbation.isHit(key)) {
                onProbationHit(node);
                queue = QueueType.WINDOW;
            } else if (headProtected.isHit(key)) {
                onProtectedHit(node);
                queue = QueueType.WINDOW;
            } else {
                throw new IllegalStateException();
            }
        }

        climb(event, queue, isFull);
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

        headProtected.setNormalization(normalizationBias,normalizationFactor);
        headProbation.setNormalization(normalizationBias,normalizationFactor);
        headWindow.setNormalization(normalizationBias,normalizationFactor);
    }

    /**
     * Adds the entry to the admission window, evicting if necessary.
     */
    private void onMiss(AccessEvent event) {
        long key = event.key();
        CraBlock.Node node = headWindow.addEntry(event);
        data.put(key, node);
        windowSize++;
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
        protectedSize++;
        demoteProtected();
    }

    private void demoteProtected() {
        if (protectedSize > maxProtected) {
            CraBlock.Node demote = headProtected.findVictim();
            demote.remove();
            headProtected.remove(demote.key());
            headProbation.addEntry(demote);
            protectedSize--;
        }
    }

    /**
     * Moves the entry to the MRU position, if it falls outside the fast-path threshold.
     */
    private void onProtectedHit(CraBlock.Node node) {
        node.moveToTail();
    }

    /**
     * Evicts from the admission window into the probation space. If the size exceeds the maximum,
     * then the admission candidate and probation's victim are evaluated and one is evicted.
     */
    private void evict() {
        if (windowSize <= maxWindow) {
            return;
        }

        CraBlock.Node candidate = headWindow.findVictim();
        --windowSize;
        candidate.remove();
        headWindow.remove(candidate.key());
        headProbation.addEntry(candidate);
        if (data.size() > maximumWLFUSize) {
            CraBlock.Node victim = headProbation.findVictim();
            CraBlock.Node freqEvict = admittor.admit(candidate.event(), victim.event()) ? victim : candidate;
            data.remove(freqEvict.key());
            freqEvict.remove();
            headProbation.remove(freqEvict.key());

            double burstCandidateScore = burstEstimator.getLatencyEstimation(freqEvict.key());
            if (burstCandidateScore > 0) {
                if (burstBlock.isFull()) {
                    BurstBlock.Node burstVictim = burstBlock.findVictim();
                    if (burstVictim.getScore() < burstCandidateScore) {
                        burstBlock.evict();
                        burstBlock.admit(freqEvict.key(), freqEvict.event(), burstCandidateScore);
                    }
                } else {
                    burstBlock.admit(freqEvict.key(), freqEvict.event(), burstCandidateScore);
                }
            }
            policyStats.recordEviction();
        }
    }

    /**
     * Performs the hill climbing process.
     */
    private void climb(AccessEvent event, @Nullable QueueType queue, boolean isFull) {
        if (queue == null) {
            climber.onMiss(event, isFull);
        } else {
            climber.onHit(event, queue, isFull);
        }

        double probationSize = maximumWLFUSize - windowSize - protectedSize;
        LAHillClimber.Adaptation adaptation = climber
                .adapt(windowSize, probationSize, protectedSize, isFull);
        if (adaptation.type == AdaptationType.INCREASE_WINDOW) {
            increaseWindow(adaptation.amount);
        } else if (adaptation.type == AdaptationType.DECREASE_WINDOW) {
            decreaseWindow(adaptation.amount);
        }

        if (adaptation.type != AdaptationType.HOLD) {
            policyStats.recordAdaption(this.windowSize / this.maximumWLFUSize);
        }
    }

    private void increaseWindow(double amount) {
        checkState(amount >= 0.0);
        if (maxProtected == 0) {
            return;
        }

        double quota = Math.min(amount, (double)maxProtected);
        int steps = (int) (windowSize + quota) - (int) windowSize;
        windowSize += quota;

        for (int i = 0; i < steps; i++) {
            maxWindow++;
            maxProtected--;

            demoteProtected();
            CraBlock.Node candidate = headProbation.findVictim();
            candidate.remove();
            headProbation.remove(candidate.key());
            headWindow.addEntry(candidate);
        }
        checkState(windowSize >= 0);
        checkState(maxWindow >= 0);
        checkState(maxProtected >= 0);

        if (trace) {
            System.out.printf("+%,d (%,d -> %,d)%n", steps, maxWindow - steps, maxWindow);
        }
    }

    private void decreaseWindow(double amount) {
        checkState(amount >= 0.0);
        if (maxWindow == 0) {
            return;
        }

        double quota = Math.min(amount, windowSize);
        int steps = (int) Math.min((int) windowSize - (int) (windowSize - quota), maxWindow);
        windowSize -= quota;

        for (int i = 0; i < steps; i++) {
            maxWindow--;
            maxProtected++;
            CraBlock.Node candidate = headWindow.findVictim();
            candidate.remove();
            headWindow.remove(candidate.key());
            headProbation.addEntry(candidate);
        }
        checkState(windowSize >= 0);
        checkState(maxWindow >= 0);
        checkState(maxProtected >= 0);

        if (trace) {
            System.out.printf("-%,d (%,d -> %,d)%n", steps, maxWindow + steps, maxWindow);
        }
    }

    private void printSegmentSizes() {
        if (debug) {
            System.out.printf(
                    "maxWindow=%d, maxProtected=%d, percentWindow=%.1f",
                    maxWindow, maxProtected, (100.0 * maxWindow) / maximumWLFUSize);
        }
    }

    @Override
    public void finished() {
        policyStats.setPercentAdaption(
                (maxWindow / (double) maximumWLFUSize) - (1.0 - initialPercentMain));
        printSegmentSizes();

        long actualWindowSize = data.values().stream().filter(n -> headWindow.isHit(n.key())).count();
        long actualProbationSize = data.values().stream().filter(n -> headProbation.isHit(n.key()))
                .count();
        long actualProtectedSize = data.values().stream().filter(n -> headProtected.isHit(n.key()))
                .count();
        long calculatedProbationSize = data.size() - actualWindowSize - actualProtectedSize;

        checkState((long) Math.abs(windowSize - actualWindowSize) <= 1,
                   "Window size mismatch: expected: %s, received: %s",
                   (long) windowSize,
                   actualWindowSize);
        checkState((long) Math.abs(protectedSize - actualProtectedSize) <= 1,
                   "Protected size mismatch: expected: %s, received: %s",
                   (long) protectedSize,
                   actualProtectedSize);
        checkState(Math.abs(actualProbationSize - calculatedProbationSize) <= 1,
                   "Probation size mismatch: expected: %s, received: %s",
                   calculatedProbationSize,
                   actualProbationSize);
        checkState(data.size() <= maximumWLFUSize,
                   "The Window-TinyLFU size exceeds expected: Actual: %s expected: %s",
                   data.size(),
                   maximumWLFUSize);
    }

    private static final class AdaptiveCAWithBurstBlockSettings extends BasicSettings {

        public AdaptiveCAWithBurstBlockSettings(Config config) {
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

        public Set<LAHillClimberType> strategy() {
            return config().getStringList("ca-bb-hill-climber-window.strategy").stream()
                    .map(strategy -> strategy.replace('-', '_').toUpperCase(US))
                    .map(LAHillClimberType::valueOf)
                    .collect(toSet());
        }

        public List<Integer> kValues() {
            return config().getIntList("ca-bb-hill-climber-window.cra.k");
        }
        public List<Integer> maxLists() {
            return config().getIntList("ca-bb-hill-climber-window.cra.max-lists");
        }
    }
}
