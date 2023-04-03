package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.objects.ObjectObjectImmutablePair;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.google.common.base.Preconditions.checkState;

/***
 * A combination of an heap and an hash table that allows min() and get() in constant time,
 * and allows an update of the position of an item within the heap in logarithmic time.
 * Adapted from fastutil HeapPriorityQueue.
 */

@SuppressWarnings("unchecked")
public class SearchableMinimumHeap<K, V> {
    final private static float DEFAULT_LOAD_FACTOR = 1.5f;
    protected K[] heap;
    protected Map<K, Integer> idxMap;
    protected Map<K, V> valuesMap;
    protected int size;
    protected Comparator<? super K> c;

    final private static boolean DEBUG = true;

    public SearchableMinimumHeap(int capacity, Comparator<? super K> c) {
        this.c = c;
        this.heap = (K[]) new Object[capacity];
        this.idxMap = new HashMap<>(capacity, DEFAULT_LOAD_FACTOR);
        this.valuesMap = new HashMap<>(capacity, DEFAULT_LOAD_FACTOR);
        this.size = 0;
    }

    public void insert(K k, V v) {
        checkState(this.size <= this.heap.length, "Insertion into full heap");

        this.heap[this.size++] = k;
        int heapIdx = upHeap(this.size - 1);
        this.idxMap.put(k, heapIdx);
        this.valuesMap.put(k, v);
    }

    public Pair<K, V> extractMin() {
        checkState(this.size > 0, "Cannot extract from empty heap");

        K resultKey = this.heap[0];
        V resultValue = this.valuesMap.get(resultKey);

        K replacement = this.heap[--this.size];
        this.heap[0] = replacement;
        this.idxMap.put(replacement, 0);

        this.heap[this.size] = null;
        if (this.size != 0) {
            downHeap(0);
        }

        this.idxMap.remove(resultKey);
        this.valuesMap.remove(resultKey);

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
        return this.idxMap.containsKey(key);
    }

    public V get(K key) {
        if (!contains(key)) {
            throw new NoSuchElementException(String.format("No element with key %s", key));
        }

        return this.valuesMap.get(key);
    }

    public int update(K key) {
        Integer idx = this.idxMap.get(key);
        checkState(idx != null, "Invalid update");

        int res = downHeap(idx);

        if (res == idx) {
            res = upHeap(idx);
        }

        return res;
    }

    public int size() {
        return this.size;
    }

    public void clear() {
        Arrays.fill(this.heap, 0, this.size, (Object)null);
        this.idxMap.clear();
        this.size = 0;
    }

    public int downHeap(int i) {
        assert i < size;

        K e = heap[i];
        int leftChild;
        K t;
        int rightChild;
        boolean isWellPositioned = false;

        while ((leftChild = (i << 1) + 1) < size && !isWellPositioned) {
            t = heap[leftChild];
            rightChild = leftChild + 1;
            if (rightChild < size && c.compare(heap[rightChild], t) < 0) {
                leftChild = rightChild;
                t = heap[rightChild];
            }

            isWellPositioned = c.compare(e, t) <= 0;

            if (!isWellPositioned) {
                heap[i] = t;
                idxMap.put(t, i);
                i = leftChild;
            }
        }

        heap[i] = e;
        idxMap.put(e, i);
        return i;
    }

    public int upHeap(int i) {
        checkState(i < size);

        K e = heap[i];
        int parent;
        K t;
        boolean isWellPositioned = false;

        while(i != 0 && !isWellPositioned) {
            parent = (i - 1) >>> 1;
            t = heap[parent];
            isWellPositioned = c.compare(t, e) <= 0;

            if (!isWellPositioned) {
                heap[i] = t;
                idxMap.put(t, i);
                i = parent;
            }
        }

        heap[i] = e;
        idxMap.put(e, i);
        return i;
    }

    public void makeHeap() {
        int i = size >>> 1;

        while(i-- != 0) {
            downHeap(i);
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
