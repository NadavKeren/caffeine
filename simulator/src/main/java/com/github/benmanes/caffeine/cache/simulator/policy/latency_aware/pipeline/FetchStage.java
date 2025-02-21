package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline;

import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import it.unimi.dsi.fastutil.longs.*;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.NoSuchElementException;

@SuppressWarnings("unchecked")
public class FetchStage {
    final private static float DEFAULT_LOAD_FACTOR = 0.875f;
    private final KeyAndAvailabilityTimePair[] heap;
    private final Long2ObjectOpenHashMap<AccessEvent> valuesMap;
    protected int size;
    final protected int maxSize;

    public FetchStage(int maxSize) {
        this.heap = new KeyAndAvailabilityTimePair[maxSize];
        this.valuesMap = new Long2ObjectOpenHashMap<>(maxSize, DEFAULT_LOAD_FACTOR);
        this.valuesMap.defaultReturnValue(null);
        this.size = 0;
        this.maxSize = maxSize;
    }

    public void insert(AccessEvent event) {
        Assert.assertCondition(this.size <= this.heap.length, "Insertion into a full Fetch Stage");
        this.heap[this.size++] = new KeyAndAvailabilityTimePair(event.key(), event.getAvailabilityTime());
        this.valuesMap.put(event.key(), event);
        upHeap(this.size - 1);
    }

    public AccessEvent extractClosestArrival() {
        Assert.assertCondition(this.size > 0, "Cannot extract from empty heap");

        long resultKey = this.heap[0].key();

        this.heap[0] = this.heap[--this.size];

        this.heap[this.size] = null;
        if (this.size != 0) {
            downHeap(0);
        }

        final AccessEvent event = this.valuesMap.remove(resultKey);

        Assert.assertCondition(event != null, "Got null value");

        return event;
    }

    public double getClosestArrival() {
        if (this.size == 0) {
            throw new NoSuchElementException();
        }

        return this.heap[0].arrivalTime();
    }

    public boolean contains(long key) {
        return this.valuesMap.containsKey(key);
    }

    public @Nullable AccessEvent get(long key) {
        return this.valuesMap.get(key);
    }

    public int size() {
        return this.size;
    }

    public void downHeap(int i) {
        final int originIdx = i;
        Assert.assertCondition(i < size && i >= 0, () -> String.format("Invalid index: %d in size %d", originIdx, size));

        KeyAndAvailabilityTimePair targetItem = heap[i];
        KeyAndAvailabilityTimePair minimalChild;
        int leftChildIdx = (i << 1) + 1;
        int rightChildIdx = leftChildIdx + 1;
        int minimalChildIdx;
        boolean isWellPositioned = false;

        while (leftChildIdx < size && !isWellPositioned) {
            if (rightChildIdx < size && heap[rightChildIdx].arrivalTime() < heap[leftChildIdx].arrivalTime()) {
                minimalChildIdx = rightChildIdx;
                minimalChild = heap[rightChildIdx];
            } else {
                minimalChildIdx = leftChildIdx;
                minimalChild = heap[leftChildIdx];
            }

            isWellPositioned = targetItem.arrivalTime() < minimalChild.arrivalTime();

            if (!isWellPositioned) {
                heap[i] = minimalChild;
                i = minimalChildIdx;
            }

            leftChildIdx = (minimalChildIdx << 1) + 1;
            rightChildIdx = leftChildIdx + 1;
        }

        heap[i] = targetItem;

        validate();
    }

    public void upHeap(int i) {
        final int originIdx = i;
        Assert.assertCondition(i < size && i >= 0, () -> String.format("Invalid index: %d in size %d", originIdx, size));

        KeyAndAvailabilityTimePair target = heap[i];
        int parentIdx;
        KeyAndAvailabilityTimePair parent;
        boolean isWellPositioned = false;

        while(i != 0 && !isWellPositioned) {
            parentIdx = (i - 1) >>> 1;
            parent = heap[parentIdx];
            isWellPositioned = parent.arrivalTime() <= target.arrivalTime();

            if (!isWellPositioned) {
                heap[i] = parent;
                i = parentIdx;
            }
        }

        heap[i] = target;

        validate();
    }

    public void makeHeap() {
        int i = size >>> 1;

        while(i-- != 0) {
            downHeap(i);
        }

        validate();
    }

    private void validate() {
        for (int i = 0; i < size; ++i) {
            final KeyAndAvailabilityTimePair entry = heap[i];
            final int idx = i;
            Assert.assertCondition(this.valuesMap.containsKey(entry.key()),
                                   () -> String.format("No value stored for the key: %s at index: %d", entry, idx));

            Assert.assertCondition(2 * i + 1 >= size || entry.arrivalTime() <= heap[2 * i + 1].arrivalTime(),
                                   () -> String.format("Bad ordering at index %d with %d", idx, 2 * idx + 1));

            Assert.assertCondition(2 * i + 2 >= size || entry.arrivalTime() <= heap[2 * i + 2].arrivalTime(),
                                   () -> String.format("Bad ordering at index %d with %d", idx, 2 * idx + 2));
        }
    }

    public void clear() {
        Arrays.fill(this.heap, null);
        this.valuesMap.clear();
        this.size = 0;
    }

    private static class KeyAndAvailabilityTimePair {
        final private long key;
        final private double arrivalTime;

        public KeyAndAvailabilityTimePair(long key, double arrivalTime) {
            this.key = key;
            this.arrivalTime = arrivalTime;
        }

        public long key() { return key; }
        public double arrivalTime() { return arrivalTime; }
    }
}
