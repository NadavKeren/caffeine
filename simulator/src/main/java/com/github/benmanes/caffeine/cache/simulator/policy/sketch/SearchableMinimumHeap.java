package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.objects.ObjectObjectImmutablePair;

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
public class SearchableMinimumHeap<K, V> {
    final private static float DEFAULT_LOAD_FACTOR = 1.5f;
    protected K[] heap;
    protected Map<K, V> valuesMap;
    protected Map<K, Integer> idxMap;
    protected int size;
    protected Comparator<? super K> c;

    final private static boolean DEBUG = false;

    public SearchableMinimumHeap(int capacity, Comparator<? super K> c) {
        this.c = c;
        this.heap = (K[]) new Object[capacity];
        this.valuesMap = new HashMap<>(capacity, DEFAULT_LOAD_FACTOR);
        this.idxMap = new HashMap<>(capacity, DEFAULT_LOAD_FACTOR);
        this.size = 0;
    }

    public SearchableMinimumHeap(SearchableMinimumHeap<K,V> other) {
        this. c = other.c;
        int capacity = other.heap.length;
        this.heap = (K[]) new Object[capacity];

        this.valuesMap = new HashMap<>(capacity, DEFAULT_LOAD_FACTOR);
        this.idxMap = new HashMap<>(capacity, DEFAULT_LOAD_FACTOR);

        int numItemsToMove = Math.min(capacity, other.size);
        for (int i = 0; i < numItemsToMove; ++i) {
            K key = other.heap[i];
            V value = other.get(key);

            this.heap[i] = key;
            this.valuesMap.put(key, value);
            this.idxMap.put(key, i);
        }

        this.size = other.size;

        makeHeap();
    }

    public void increaseSize(int amount, @Nullable List<Pair<K, V>> items) {
        Assert.assertCondition(amount > 0, "Cannot increase by non-positive number " + amount);
        Assert.assertCondition((items != null && amount >= items.size()),
                               () -> String.format("Too many items offered: %d when increasing by: %d",
                                                   items.size(),
                                                   amount));
        int newCapacity = heap.length + amount;

        K[] newHeap = (K[]) new Object[newCapacity];
        Map<K, V> newValuesMap = new HashMap<>(newCapacity, DEFAULT_LOAD_FACTOR);
        Map<K, Integer> newIdxMap = new HashMap<>(newCapacity, DEFAULT_LOAD_FACTOR);
        int i = 0;
        for (; i < size; ++i) {
            K key = heap[i];
            V value = get(key);

            newHeap[i] = key;
            newValuesMap.put(key, value);
            newIdxMap.put(key, i);
        }

        if (items != null) {
            for (Pair<K, V> itemPair : items) {
                K key = itemPair.first();
                newHeap[i] = key;
                newValuesMap.put(key, itemPair.second());
                newIdxMap.put(key, i);
                ++i;
            }
        }

        this.heap = newHeap;
        this.valuesMap = newValuesMap;
        this.idxMap = newIdxMap;
        this.size += items.size();

        makeHeap();
        final int idx = i; // for lambda capture
        Assert.assertCondition((this.size == idx),
                               () -> String.format("Size mismatch; expected = %d, actual = %d", size, idx));
        Assert.assertCondition(this.valuesMap.size() == size, () -> String.format("Class and map sizes mismatch; Class size: %d, Map size: %d", size, this.valuesMap.size()));
    }

    public List<Pair<K, V>> decreaseSize(int amount) {
        Assert.assertCondition(amount > 0, "Cannot decrease by non-positive number " + amount);
        int numOfItemsToRemove = Math.min(amount, size);

        List<Pair<K, V>> itemsRemoved = new ArrayList<>(numOfItemsToRemove);

        for (int i = 0; i < numOfItemsToRemove; ++i) {
            Pair<K, V> item = extractMin();
            itemsRemoved.add(item);
        }

        validate();

        return itemsRemoved;
    }

    public void insert(K k, V v) {
        Assert.assertCondition(this.size <= this.heap.length, "Insertion into full heap");

        this.heap[this.size++] = k;
        this.valuesMap.put(k, v);
        upHeap(this.size - 1);
    }

    public V remove(K k) {
        int idx = this.idxMap.get(k);
        V value = this.valuesMap.get(k);

        this.heap[idx] = this.heap[--this.size];
        downHeap(idx);
        upHeap(idx);

        this.idxMap.remove(k);
        this.valuesMap.remove(k);

        return value;
    }

    public Pair<K, V> extractMin() {
        Assert.assertCondition(this.size > 0, "Cannot extract from empty heap");

        K resultKey = this.heap[0];
        V resultValue = this.valuesMap.get(resultKey);

        K replacement = this.heap[--this.size];
        this.heap[0] = replacement;

        if (this.size > 0) {
            this.idxMap.put(replacement, 0);
        }

        this.heap[this.size] = null;
        if (this.size != 0) {
            downHeap(0);
        }

        final V valuesRes = this.valuesMap.remove(resultKey);
        final Integer idxRes = this.idxMap.remove(resultKey);

        Assert.assertCondition(valuesRes != null, "Got null at values");
        Assert.assertCondition(idxRes != null, "Got null at indexes");

        return new ObjectObjectImmutablePair<>(resultKey, resultValue);
    }

    public Pair<K, V> min() {
        if (this.size == 0) {
            throw new NoSuchElementException();
        } else {
            K key = this.heap[0];
            V value = this.valuesMap.get(key);
            return new ObjectObjectImmutablePair<>(key, value);
        }
    }

    public boolean contains(K key) {
        return this.valuesMap.containsKey(key);
    }

    public @Nullable V get(K key) {
        return this.valuesMap.get(key);
    }

    public int getIndex(K key) {
        return this.idxMap.get(key);
    }

    public int size() {
        return this.size;
    }

    public void clear() {
        Arrays.fill(this.heap, 0, this.size, (Object)null);
        this.valuesMap.clear();
        this.size = 0;
    }

    public int downHeap(int i) {
        final int originIdx = i;
        Assert.assertCondition(i < size && i >= 0, () -> String.format("Invalid index: %d in size %d", originIdx, size));

        K targetItem = heap[i];
        K minimalChild;
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
            rightChildIdx = minimalChildIdx + 1;
        }

        this.idxMap.put(targetItem, i);
        heap[i] = targetItem;

        return i;
    }

    public int upHeap(int i) {
        final int originIdx = i;
        Assert.assertCondition(i < size && i >= 0, () -> String.format("Invalid index: %d in size %d", originIdx, size));

        K e = heap[i];
        int parent;
        K t;
        boolean isWellPositioned = false;

        while(i != 0 && !isWellPositioned) {
            parent = (i - 1) >>> 1;
            t = heap[parent];
            isWellPositioned = c.compare(t, e) <= 0;

            if (!isWellPositioned) {
                this.idxMap.put(t, i);
                heap[i] = t;
                i = parent;
            }
        }

        this.idxMap.put(e, i);
        heap[i] = e;

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
            final K key = heap[i];
            final int idx = i;
            Assert.assertCondition(key != null, "Null value found");
            Assert.assertCondition(this.valuesMap.containsKey(key), () -> String.format("No value stored for the key: %s at index: %d", key, idx));
            Assert.assertCondition(this.idxMap.containsKey(key), () -> String.format("No index stored for the key: %s at index: %d", key, idx));
            final int expectedIdx = i;
            Assert.assertCondition(this.idxMap.get(key) == i, () -> String.format("Wrong index stored for the key: %s, expected: %d, got: %d", key, expectedIdx, this.idxMap.get(key)));
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
                K key = heap[idx];
                writer.printf("%s%n", key.toString());
            }

            writer.close();
        }
    }
}
