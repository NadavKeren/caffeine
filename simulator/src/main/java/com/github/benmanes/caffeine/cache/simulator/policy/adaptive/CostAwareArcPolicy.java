package com.github.benmanes.caffeine.cache.simulator.policy.adaptive;

import static com.google.common.base.Preconditions.checkState;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.EntryData;
import com.github.benmanes.caffeine.cache.simulator.policy.LatencyEstimator;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy.PolicySpec;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.linked.CraBlock;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.LatestLatencyEstimator;
import com.google.common.primitives.Ints;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

/**
 * Adaptive Replacement Cache. This algorithm uses a queue for items that are seen once (T1), a
 * queue for items seen multiple times (T2), and non-resident queues for evicted items that are
 * being monitored (B1, B2). The maximum size of the T1 and T2 queues is adjusted dynamically based
 * on the workload patterns and effectiveness of the cache.
 * <p>
 * This implementation is based on the pseudo code provided by the authors in their paper
 * <a href="http://www.cs.cmu.edu/~15-440/READINGS/megiddo-computer2004.pdf">Outperforming LRU with
 * an Adaptive Replacement Cache Algorithm</a> and is further described in their paper,
 * <a href="https://www.usenix.org/event/fast03/tech/full_papers/megiddo/megiddo.pdf">ARC: A
 * Self-Tuning, Low Overhead Replacement Cache</a>.
 * <p>
 * This algorithm is patented by IBM (6996676, 7096321, 7058766, 8612689) and Sun (7469320), making
 * its use in applications ambiguous due to Sun's ZFS providing an implementation under the CDDL.
 *
 * This version is using CRA blocks to become latency aware, and is aware to delayed hits as well.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 * @author himelbrand@gmail.com (Omri Himelbrand)
 * @author nadav.keren@gmail.com (Nadav Keren)
 */
@PolicySpec(name = "adaptive.CA-Arc")
public final class CostAwareArcPolicy implements Policy {
    // In Cache:
    // - T1: Pages that have been accessed at least once
    // - T2: Pages that have been accessed at least twice
    // Ghost:
    // - B1: Evicted from T1
    // - B2: Evicted from T2
    // Adapt:
    // - Hit in B1 should increase size of T1, drop entry from T2 to B2
    // - Hit in B2 should increase size of T2, drop entry from T1 to B1

    private final Long2ObjectMap<Entry> data;
    private final PolicyStats policyStats;
    private final int maximumSize;
    private LatencyEstimator<Long> latencyEstimator;

    private final CraBlock headT1;
    private final CraBlock headT2;
    private final CraBlock headB1;
    private final CraBlock headB2;

    private int sizeT1;
    private int sizeT2;
    private int sizeB1;
    private int sizeB2;
    private int p;
    private double normalizationBias;
    private double normalizationFactor;
    private double maxDelta;
    private int maxDeltaCounts;
    private int samplesCount;

    public CostAwareArcPolicy(Config config) {
        BasicSettings settings = new BasicSettings(config);
        this.maximumSize = Ints.checkedCast(settings.maximumSize());
        this.policyStats = new PolicyStats(name());
        this.data = new Long2ObjectOpenHashMap<>();
        this.latencyEstimator = new LatestLatencyEstimator<>();
        this.headT1 = new CraBlock(1, 10, this.maximumSize, latencyEstimator, "T1");
        this.headT2 = new CraBlock(1, 10, this.maximumSize, latencyEstimator, "T2");
        this.headB1 = new CraBlock(1, 10, this.maximumSize, latencyEstimator, "B1");
        this.headB2 = new CraBlock(1, 10, this.maximumSize, latencyEstimator, "B2");
        this.normalizationBias = 0;
        this.normalizationFactor = 0;
        this.maxDelta = 0;
        this.maxDeltaCounts = 0;
        this.samplesCount = 0;
    }

    private void updateNormalization() {
        headT1.setNormalization(normalizationBias,normalizationFactor);
        headT2.setNormalization(normalizationBias,normalizationFactor);
        headB1.setNormalization(normalizationBias,normalizationFactor);
        headB2.setNormalization(normalizationBias,normalizationFactor);
    }

    @Override
    public void record(AccessEvent event) {
        long key = event.key();
        policyStats.recordOperation();
        Entry entry = data.get(key);
        if (entry == null) {
            if (event.delta() > normalizationFactor){
                samplesCount++;
                maxDelta = (maxDelta*maxDeltaCounts + event.delta())/++maxDeltaCounts;
            }
            normalizationBias = normalizationBias > 0 ? Math.min(normalizationBias,Math.max(0,event.delta())) : Math.max(0,event.delta());
            if (samplesCount % 1000 == 0 || normalizationFactor == 0){
                normalizationFactor = maxDelta;
                maxDeltaCounts = 1;
                samplesCount = 0;
            }
            updateNormalization();
            onMiss(event);
        } else if (entry.type == QueueType.B1) {
            onHitB1(entry);
        } else if (entry.type == QueueType.B2) {
            onHitB2(entry);
        } else {
            onHit(entry, event);
        }
    }

    private void onHit(Entry entry, AccessEvent event) {
        // x ∈ T1 ∪ T2 (a hit in ARC(c) and DBL(2c)): Move x to the top of T2
        if (entry.type == QueueType.T1) {
            headT1.remove(entry.data.key());
            sizeT1--;
            sizeT2++;
        } else {
            headT2.remove(entry.data.key());
        }

        entry.type = QueueType.T2;
        headT2.addEntry(entry.data);
        recordAccordingToAvailability(entry.data, event);
    }

    private void recordAccordingToAvailability(EntryData entry, AccessEvent currEvent) {
        boolean isAvailable = entry.event().isAvailableAt(currEvent.getArrivalTime());
        if (isAvailable) {
            currEvent.changeEventStatus(AccessEvent.EventStatus.HIT);
            policyStats.recordHit();
            policyStats.recordHitPenalty(currEvent.hitPenalty());
        } else {
            currEvent.changeEventStatus(AccessEvent.EventStatus.DELAYED_HIT);
            currEvent.setDelayedHitPenalty(entry.event().getAvailabilityTime());
            policyStats.recordDelayedHitPenalty(currEvent.delayedHitPenalty());
            policyStats.recordDelayedHit();
        }
    }

    private void onHitB1(Entry entry) {
        // x ∈ B1 (a miss in ARC(c), a hit in DBL(2c)):
        // Adapt p = min{ c, p + max{ |B2| / |B1|, 1} }. REPLACE(p).
        // Move x to the top of T2 and place it in the cache.

        p = Math.min(maximumSize, p + Math.max(sizeB2 / sizeB1, 1));
        evict(entry);
        headB1.remove(entry.data.key());
        sizeT2++;
        sizeB1--;
        entry.type = QueueType.T2;
        headT2.addEntry(entry.data);
        policyStats.recordMiss();
        policyStats.recordMissPenalty(entry.data.event().missPenalty());
    }

    private void onHitB2(Entry entry) {
        // x ∈ B2 (a miss in ARC(c), a hit in DBL(2c)):
        // Adapt p = max{ 0, p – max{ |B1| / |B2|, 1} } . REPLACE(p).
        // Move x to the top of T2 and place it in the cache.

        p = Math.max(0, p - Math.max(sizeB1 / sizeB2, 1));
        evict(entry);
        headB2.remove(entry.data.key());
        sizeT2++;
        sizeB2--;
        entry.type = QueueType.T2;
        headT2.addEntry(entry.data);
        policyStats.recordMiss();
        policyStats.recordMissPenalty(entry.data.event().missPenalty());
    }

    private void onMiss(AccessEvent event) {
        // x ∈ L1 ∪ L2 (a miss in DBL(2c) and ARC(c)):
        // case (i) |L1| = c:
        //   If |T1| < c then delete the LRU page of B1 and REPLACE(p).
        //   else delete LRU page of T1 and remove it from the cache.
        // case (ii) |L1| < c and |L1| + |L2|≥ c:
        //   if |L1| + |L2|= 2c then delete the LRU page of B2.
        //   REPLACE(p) .
        // Put x at the top of T1 and place it in the cache.

        //Add to CRA block to generate node and entry
        EntryData entryData = new EntryData(event);
        Entry entry = new Entry(entryData);

        int sizeL1 = (sizeT1 + sizeB1);
        int sizeL2 = (sizeT2 + sizeB2);
        if (sizeL1 == maximumSize) {
            if (sizeT1 < maximumSize) {
                EntryData victim = headB1.removeVictim();
                data.remove(victim.key());
                sizeB1--;

                evict(entry);
            } else {
                EntryData victim = headT1.removeVictim();
                data.remove(victim.key());
                sizeT1--;
            }
        } else if ((sizeL1 < maximumSize) && ((sizeL1 + sizeL2) >= maximumSize)) {
            if ((sizeL1 + sizeL2) >= (2 * maximumSize)) {
                EntryData victim = headB2.removeVictim();
                data.remove(victim.key());
                sizeB2--;
            }
            evict(entry);
        }

        sizeT1++;
        data.put(entry.data.key(), entry);
        headT1.addEntry(entryData);

        policyStats.recordMiss();
        policyStats.recordMissPenalty(event.missPenalty());
    }

    /** Evicts while the map exceeds the maximum capacity. */
    private void evict(Entry candidate) {
        // if (|T1| ≥ 1) and ((x ∈ B2 and |T1| = p) or (|T1| > p))
        //   then move the LRU page of T1 to the top of B1 and remove it from the cache.
        // else move the LRU page in T2 to the top of B2 and remove it from the cache.

        if ((sizeT1 >= 1) && (((candidate.type == QueueType.B2) && (sizeT1 == p)) || (sizeT1 > p))) {
            EntryData victim = headT1.removeVictim();;
            Entry victimEntry = data.get(victim.key());
            victimEntry.type = QueueType.B1;
            headB1.addEntry(victim);
            sizeT1--;
            sizeB1++;
        } else {
            EntryData victim = headT2.removeVictim();
            Entry victimEntry = data.get(victim.key());
            victimEntry.type = QueueType.B2;
            headB2.addEntry(victim);
            sizeT2--;
            sizeB2++;
        }
        policyStats.recordEviction();
    }

    @Override
    public PolicyStats stats() {
        return policyStats;
    }

    @Override
    public void finished() {
        policyStats.setPercentAdaption((sizeT1 / (double) maximumSize) - 0.5);

        checkState(sizeT1 == data.values().stream().filter(node -> node.type == QueueType.T1).count());
        checkState(sizeT2 == data.values().stream().filter(node -> node.type == QueueType.T2).count());
        checkState(sizeB1 == data.values().stream().filter(node -> node.type == QueueType.B1).count());
        checkState(sizeB2 == data.values().stream().filter(node -> node.type == QueueType.B2).count());
        checkState((sizeT1 + sizeT2) <= maximumSize);
        checkState((sizeB1 + sizeB2) <= maximumSize);
    }

    private enum QueueType {
        T1, B1,
        T2, B2,
    }
    private static class Entry {
        EntryData data;
        QueueType type;

        public Entry(EntryData data) {
            this.data = data;
            this.type = QueueType.T1;
        }
    }
}