package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.longs.*;

import javax.annotation.Nullable;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/***
 * A combination of an heap and an hash table that allows min() and get() in constant time,
 * and allows an update of the position of an item within the heap in logarithmic time.
 * Adapted from fastutil HeapPriorityQueue.
 */

@SuppressWarnings("unchecked")
public class SearchableMinimumHeap<V> {
    final private static float DEFAULT_LOAD_FACTOR = 0.875f;
    protected long[] heap;
    protected Long2ObjectOpenHashMap<V> valuesMap;
    protected Long2IntOpenHashMap idxMap;
    protected int size;
    protected int maxSize;
    final protected LongComparator c;

    final private static boolean DEBUG = false;

    public SearchableMinimumHeap(int maximalCapacity, LongComparator c) {
        this.c = c;
        this.heap = new long[maximalCapacity];
        this.valuesMap = new Long2ObjectOpenHashMap<>(maximalCapacity, DEFAULT_LOAD_FACTOR);
        this.idxMap = new Long2IntOpenHashMap(maximalCapacity, DEFAULT_LOAD_FACTOR);
        this.valuesMap.defaultReturnValue(null);
        this.idxMap.defaultReturnValue(-1);
        this.size = 0;
        this.maxSize = 0;
    }

    public SearchableMinimumHeap(SearchableMinimumHeap<V> other) {
        this.c = other.c;
        int maximalCapacity = other.heap.length;
        this.heap = new long[maximalCapacity];

        this.valuesMap = new Long2ObjectOpenHashMap<>(maximalCapacity, DEFAULT_LOAD_FACTOR);
        this.idxMap = new Long2IntOpenHashMap(maximalCapacity, DEFAULT_LOAD_FACTOR);

        other.copyInto(this);
    }

    public void copyInto(SearchableMinimumHeap<V> other) {
        Assert.assertCondition(this.heap.length == other.heap.length,
                               () -> String.format("copy fail: heap sizes mismatch, src: %d vs dst: %d",
                                                   this.heap.length,
                                                   other.heap.length));
        int maximalCapacity = this.heap.length;

        int numItemsToMove = Math.min(maximalCapacity, this.size);
        for (int i = 0; i < numItemsToMove; ++i) {
            long key = this.heap[i];
            V value = this.get(key);

            other.heap[i] = key;
            other.valuesMap.put(key, value);
            other.idxMap.put(key, i);
        }

        other.size = this.size;
        other.maxSize = this.maxSize;

        other.makeHeap();
    }

    public void increaseSize(int amount, @Nullable List<LongObjectPair<V>> items) {
        Assert.assertCondition(amount > 0, "Cannot increase by non-positive number " + amount);
        Assert.assertCondition((items != null && amount >= items.size()),
                               () -> String.format("Too many items offered: %d when increasing by: %d",
                                                   items.size(),
                                                   amount));
        this.maxSize += amount;

        Assert.assertCondition(this.maxSize <= this.heap.length,
                               () -> String.format("Exceeding the maximal capacity possible, current maximum: %d, maximal capacity: %d",
                                                   this.maxSize,
                                                   this.heap.length));

        int i = size;
        if (items != null) {
            for (Pair<Long, V> itemPair : items) {
                long key = itemPair.first();
                heap[i] = key;
                valuesMap.put(key, itemPair.second());
                idxMap.put(key, i);
                ++i;
            }

            this.size += items.size();
        }

        makeHeap();
        final int idx = i; // for lambda capture
        Assert.assertCondition((this.size == idx),
                               () -> String.format("Size mismatch; expected = %d, actual = %d", size, idx));
        Assert.assertCondition(this.valuesMap.size() == size,
                               () -> String.format("Class and map sizes mismatch; Class size: %d, Map size: %d",
                                                   size,
                                                   this.valuesMap.size()));
    }

    public List<LongObjectPair<V>> decreaseSize(int amount) {
        Assert.assertCondition(amount > 0, "Cannot decrease by non-positive number " + amount);
        int numOfItemsToRemove = Math.min(amount, size);

        List<LongObjectPair<V>> itemsRemoved = new ArrayList<>(numOfItemsToRemove);

        for (int i = 0; i < numOfItemsToRemove; ++i) {
            LongObjectPair<V> item = extractMin();
            itemsRemoved.add(item);
        }

        this.maxSize -= amount;
        validate();

        return itemsRemoved;
    }

    public void insert(long k, V v) {
        Assert.assertCondition(this.size <= this.heap.length, "Insertion into full heap");
        Assert.assertCondition(!this.idxMap.containsKey(k), "Inserting duplicate item");
        this.heap[this.size++] = k;
        this.valuesMap.put(k, v);
        upHeap(this.size - 1);
    }

    public V remove(long k) {
        int idx = this.idxMap.get(k);
        V value = this.valuesMap.get(k);

        this.heap[idx] = this.heap[--this.size];
        if (idx < size) {
            downHeap(idx);
            upHeap(idx);
        }

        this.idxMap.remove(k);
        this.valuesMap.remove(k);

        return value;
    }

    public LongObjectPair<V> extractMin() {
        Assert.assertCondition(this.size > 0, "Cannot extract from empty heap");

        long resultKey = this.heap[0];
        V resultValue = this.valuesMap.get(resultKey);

        long replacementKey = this.heap[--this.size];
        this.heap[0] = replacementKey;

        if (this.size > 0) {
            this.idxMap.put(replacementKey, 0);
        }

        this.heap[this.size] = 0;
        if (this.size != 0) {
            downHeap(0);
        }

        final V valuesRes = this.valuesMap.remove(resultKey);
        final int idxRes = this.idxMap.remove(resultKey);

        Assert.assertCondition(valuesRes != null, "Got null value");
        Assert.assertCondition(idxRes >= 0, "Got invalid index");

        return new LongObjectImmutablePair<>(resultKey, resultValue);
    }

    public LongObjectPair<V> min() {
        if (this.size == 0) {
            throw new NoSuchElementException();
        } else {
            long key = this.heap[0];
            V value = this.valuesMap.get(key);
            return new LongObjectImmutablePair<>(key, value);
        }
    }

    public boolean contains(long key) {
        return this.valuesMap.containsKey(key);
    }

    public @Nullable V get(long key) {
        return this.valuesMap.get(key);
    }

    public int getIndex(long key) {
        return this.idxMap.get(key);
    }

    public int size() {
        return this.size;
    }

    public void clear() {
        Arrays.fill(this.heap, 0, this.size, 0);
        this.valuesMap.clear();
        this.idxMap.clear();
        this.size = 0;
    }

    public int downHeap(int i) {
        final int originIdx = i;
        Assert.assertCondition(i < size && i >= 0, () -> String.format("Invalid index: %d in size %d", originIdx, size));

        long targetItem = heap[i];
        long minimalChild;
        int leftChildIdx = (i << 1) + 1;
        int rightChildIdx = leftChildIdx + 1;
        int minimalChildIdx;
        boolean isWellPositioned = false;

        while (leftChildIdx < size && !isWellPositioned) {
            if (rightChildIdx < size && c.compare(heap[rightChildIdx], heap[leftChildIdx]) < 0) {
                minimalChildIdx = rightChildIdx;
                minimalChild = heap[rightChildIdx];
            } else {
                minimalChildIdx = leftChildIdx;
                minimalChild = heap[leftChildIdx];
            }

            isWellPositioned = c.compare(targetItem, minimalChild) <= 0;

            if (!isWellPositioned) {
                this.idxMap.put(minimalChild, i);
                heap[i] = minimalChild;
                i = minimalChildIdx;
            }

            leftChildIdx = (minimalChildIdx << 1) + 1;
            rightChildIdx = leftChildIdx + 1;
        }

        this.idxMap.put(targetItem, i);
        heap[i] = targetItem;

        return i;
    }

    public int upHeap(int i) {
        final int originIdx = i;
        Assert.assertCondition(i < size && i >= 0, () -> String.format("Invalid index: %d in size %d", originIdx, size));

        long target = heap[i];
        int parentIdx;
        long parentKey;
        boolean isWellPositioned = false;

        while(i != 0 && !isWellPositioned) {
            parentIdx = (i - 1) >>> 1;
            parentKey = heap[parentIdx];
            isWellPositioned = c.compare(parentKey, target) <= 0;

            if (!isWellPositioned) {
                this.idxMap.put(parentKey, i);
                heap[i] = parentKey;
                i = parentIdx;
            }
        }

        this.idxMap.put(target, i);
        heap[i] = target;

        return i;
    }

    public void makeHeap() {
        int i = size >>> 1;

        while(i-- != 0) {
            downHeap(i);
        }

        validate();
    }

    public void validate() {
        for (int i = 0; i < size; ++i) {
            final long key = heap[i];
            final int idx = i;
            Assert.assertCondition(this.valuesMap.containsKey(key), () -> String.format("No value stored for the key: %s at index: %d", key, idx));
            Assert.assertCondition(this.idxMap.containsKey(key), () -> String.format("No index stored for the key: %s at index: %d", key, idx));
            final int expectedIdx = i;
            final int storedIdx = this.idxMap.get(key);
            Assert.assertCondition(storedIdx == i, () -> String.format("Wrong index stored for the key: %s, expected: %d, got: %d", key, expectedIdx, this.idxMap.get(key)));
        }
    }

    private PrintWriter prepareFileWriter() {
        LocalDateTime currentTime = LocalDateTime.now(ZoneId.systemDefault());
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("dd-MM-HH-mm-ss");
        PrintWriter writer = null;
        try {
            FileWriter fwriter = new FileWriter("/tmp/searchable-heap-dump-" + currentTime.format(timeFormatter) + ".dump", StandardCharsets.UTF_8);
            writer = new PrintWriter(fwriter);
        } catch (IOException e) {
            System.err.println("Error creating the log file handler");
            e.printStackTrace();
            System.exit(1);
        }

        return writer;
    }

    public void dump() {
        if (DEBUG) {
            PrintWriter writer = prepareFileWriter();

            for (int idx = 0; idx < size; ++idx) {
                long key = heap[idx];
                V value = this.valuesMap.get(key);
                writer.printf("%s %s%n", key, value);
            }

            writer.close();
        }
    }

    public void setSize(int size) {
        this.maxSize = size;
    }
}
