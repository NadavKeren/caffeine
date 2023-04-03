package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import com.github.benmanes.caffeine.cache.simulator.policy.EntryData;
import com.github.benmanes.caffeine.cache.simulator.policy.LatencyEstimator;

final public class BurstBlock {
    private SearchableMinimumHeap<Long, EntryData> heap;
    final private int maximumSize;
    private int fetchCounter;

    public BurstBlock(int maximumSize, LatencyEstimator<Long> estimator) {
        this.heap = new SearchableMinimumHeap<>(maximumSize, (l1, l2) -> (int)(estimator.getLatencyEstimation(l1) - estimator.getLatencyEstimation(l2)));
        this.maximumSize = maximumSize;
        this.fetchCounter = 0;
    }

    public void evict() {
        heap.extractMin();
    }

    public void admit(EntryData entry) {
        if (isFull()) {
            throw new IllegalArgumentException(); // TODO: nkeren: check if there is better exception for this
        }
        heap.insert(entry.key(), entry);
    }

    public boolean isHit(long key) {
        return heap.contains(key);
    }

//    public void update(long key) {
//        heap.update(key);
//    }

    private void update() {
        heap.makeHeap();
        fetchCounter = 0;
    }

    public EntryData get(long key) {
        ++fetchCounter;
        return heap.get(key);
    }

    public EntryData getVictim() {
        if (fetchCounter > 10 * maximumSize) {
            update();
        }

        return heap.min().second();
    }

    public boolean isFull() { return heap.size() == maximumSize; }

    public void dump() { heap.dump(); }
}
