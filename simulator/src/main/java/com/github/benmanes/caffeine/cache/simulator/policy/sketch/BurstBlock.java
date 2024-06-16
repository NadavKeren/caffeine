package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.ConsoleColors;
import com.github.benmanes.caffeine.cache.simulator.policy.EntryData;
import com.github.benmanes.caffeine.cache.simulator.policy.LatencyEstimator;
import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.objects.ObjectObjectImmutablePair;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.lang.System.Logger;

final public class BurstBlock {
    final private static boolean DEBUG = true;
    final private static Logger logger = System.getLogger(BurstBlock.class.getSimpleName());
    private SearchableMinimumHeap<Long, EntryData> heap;
    private int currentCapacity;
    final private int maximalCapacity;
    private int fetchCounter;
    final private Comparator<Long> c;

    public BurstBlock(int maximalCapacity, int currentCapacity, LatencyEstimator<Long> estimator) {
        this.c = (l1, l2) -> (int) Math.round(estimator.getLatencyEstimation(l1) - estimator.getLatencyEstimation(l2));
        this.heap = new SearchableMinimumHeap<>(maximalCapacity, currentCapacity, this.c);
        this.currentCapacity = currentCapacity;
        this.maximalCapacity = maximalCapacity;
        this.fetchCounter = 0;
    }

    public BurstBlock(BurstBlock other, String name) {
        this.c = other.c;
        this.heap = new SearchableMinimumHeap<>(other.heap);
        this.currentCapacity = other.currentCapacity;
        this.maximalCapacity = other.maximalCapacity;
        this.fetchCounter = 0;
    }


    public void copyInto(BurstBlock other) {
        this.heap.copyInto(other.heap);
        other.currentCapacity = this.currentCapacity;
        other.fetchCounter = this.fetchCounter;
    }

    public void clear() {
        this.heap.clear();
    }

    public EntryData removeVictim() {
        var res = heap.extractMin();

        return res.second();
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

        this.currentCapacity += amount;
        this.heap.increaseSize(amount, expendedItems);

        if (DEBUG) {
            logger.log(Logger.Level.INFO, ConsoleColors.infoString("BurstBlock: Increased by %d to %d", amount, this.currentCapacity));
        }
    }

    public List<EntryData> decreaseSize(int amount) {
        Assert.assertCondition(amount <= currentCapacity, "Cannot lower the size below 0");
        var items = this.heap.decreaseSize(amount);
        currentCapacity -= amount;

        List<EntryData> evicted = new ArrayList<>(items.size());
        for (Pair<Long, EntryData> item : items) {
            evicted.add(item.second());
        }

        if (DEBUG) {
            logger.log(Logger.Level.INFO, ConsoleColors.infoString("BurstBlock: Decreased by %d to %d", amount, this.currentCapacity));
        }

        return evicted;
    }

    public EntryData remove(long key) {
        return this.heap.remove(key);
    }

    private void update() {
        heap.makeHeap();
        fetchCounter = 0;
    }

    public @Nullable EntryData get(long key) {
        ++fetchCounter;
        return heap.get(key);
    }

    public int getIndex(long key) {
        return this.heap.getIndex(key);
    }

    public EntryData getVictim() {
        if (fetchCounter > 10 * currentCapacity) {
            update();
        }

        return heap.min().second();
    }

    public int compareToVictim(EntryData item) {
        return c.compare(item.key(), getVictim().key());
    }

    public boolean isFull() { return heap.size() == currentCapacity; }

    public int capacity() { return this.currentCapacity; }

    public void dump() { heap.dump(); }

    public int size() {
        return heap.size();
    }

    public void validate() {
        this.heap.validate();
    }

    public void setSize(int size) {
        this.currentCapacity = size;
        this.heap.setSize(size);
    }
}
