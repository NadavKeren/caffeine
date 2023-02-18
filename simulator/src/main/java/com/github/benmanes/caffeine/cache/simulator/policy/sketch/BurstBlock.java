package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import com.github.benmanes.caffeine.cache.simulator.policy.EntryData;
import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;

import java.util.Map;

final public class BurstBlock {
    private Map<Long, Node> dataMap;
    private PriorityQueue<Node> dataHeap; // This is a minimum heap
    final private long maximumSize;
    private long currSize;

    public BurstBlock(long maximumSize) {
        this.dataMap = new Long2ObjectOpenHashMap<>();
        this.dataHeap = new ObjectHeapPriorityQueue<>((o1, o2) -> (int)(o1.score() - o2.score()));
        this.maximumSize = maximumSize;
        this.currSize = 0;
    }

    public void evict() {
        Node item = dataHeap.dequeue();
        dataMap.remove(item.key());
        --currSize;
    }

    public void admit(EntryData entry, double score) {
        if (isFull()) {
            throw new IllegalArgumentException(); // TODO: nkeren: check if there is better exception for this
        }

        Node admittedNode = new Node(entry, score);
        dataMap.put(entry.key(), admittedNode);
        dataHeap.enqueue(admittedNode);
        ++currSize;
    }

    public boolean isHit(long key) {
        return dataMap.containsKey(key);
    }

    public EntryData get(long key) {
        return dataMap.get(key).data();
    }

    public double getVictimScore() { return dataHeap.first().score(); }

    public boolean isFull() { return currSize == maximumSize; }

    private static class Node {
        private EntryData data;
        private double score;

        public Node(EntryData entry, double score) {
            this.data = entry;
            this.score = score;
        }

        public EntryData data() {
            return data;
        }

        public long key() {
            return data.key();
        }

        public double score() {
            return score;
        }
    }
}
