package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.ConsoleColors;
import com.github.benmanes.caffeine.cache.simulator.policy.EntryData;
import com.github.benmanes.caffeine.cache.simulator.policy.LatencyEstimator;
import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.objects.ObjectObjectImmutablePair;

import java.util.ArrayList;
import java.util.List;
import java.lang.System.Logger;

import static com.google.common.base.Preconditions.checkState;

final public class BurstBlock {
    final private static boolean DEBUG = true;
    final private static Logger logger = System.getLogger(BurstBlock.class.getSimpleName());
    private SearchableMinimumHeap<Long, EntryData> heap;
    private int maximumSize;
    private int fetchCounter;

    public BurstBlock(int maximumSize, LatencyEstimator<Long> estimator) {
        this.heap = new SearchableMinimumHeap<>(maximumSize, (l1, l2) -> (int)(estimator.getLatencyEstimation(l1) - estimator.getLatencyEstimation(l2)));
        this.maximumSize = maximumSize;
        this.fetchCounter = 0;
    }

    public BurstBlock(BurstBlock other, String name) {
        this.heap = new SearchableMinimumHeap<>(other.heap);
        this.maximumSize = other.maximumSize;
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

    public void increaseSize(int amount, List<EntryData> items) {
        List<Pair<Long, EntryData>> expendedItems = new ArrayList<>(items.size());
        for (EntryData item : items) {
            expendedItems.add(new ObjectObjectImmutablePair<>(item.key(), item));
        }

        this.maximumSize += amount;
        this.heap.increaseSize(amount, expendedItems);

        if (DEBUG) {
            logger.log(Logger.Level.INFO, ConsoleColors.infoString("BurstBlock: Increased by %d to %d", amount, this.maximumSize));
        }
    }

    public List<EntryData> decreaseSize(int amount) {
        checkState(amount <= maximumSize, "Cannot lower the size below 0");
        var items = this.heap.decreaseSize(amount);
        maximumSize -= amount;

        List<EntryData> evicted = new ArrayList<>(items.size());
        for (Pair<Long, EntryData> item : items) {
            evicted.add(item.second());
        }

        if (DEBUG) {
            logger.log(Logger.Level.INFO, ConsoleColors.infoString("BurstBlock: Decreased by %d to %d", amount, this.maximumSize));
        }

        return evicted;
    }

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

    public int capacity() { return this.maximumSize; }

    public void dump() { heap.dump(); }

    public int size() {
        return heap.size();
    }
}
