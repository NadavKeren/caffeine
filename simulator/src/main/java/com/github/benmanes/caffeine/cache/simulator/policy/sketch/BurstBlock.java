package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
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
        this.dataHeap = new ObjectHeapPriorityQueue<>((o1, o2) -> (int)(o1.getScore() - o2.getScore()));
        this.maximumSize = maximumSize;
        this.currSize = 0;
    }

    public Node findVictim() {
        return currSize > 0 ? dataHeap.first() : null;
    }

    public void evict() {
        Node item = dataHeap.dequeue();
        dataMap.remove(item.getKey());
        --currSize;
    }

    public void admit(long key, AccessEvent event, double score) {
        if (isFull()) {
            throw new IllegalArgumentException(); // TODO: nkeren: check if there is better exception for this
        }

        Node admittedNode = new Node(event, key, score);
        dataMap.put(key, admittedNode);
        dataHeap.enqueue(admittedNode);
        ++currSize;
    }

    public Node isHit(long key, double time) {
        Node node = dataMap.get(key);
        if (node != null) {
            node.updateLastOccurrence(time);
        }

        return node;
    }

    public boolean isFull() { return currSize == maximumSize; }

    public static class Node {
        private AccessEvent event;
        private long key;
        private double score;
        private double lastOccurrence;

        public Node(AccessEvent event, long key, double score) {
            this.event = event;
            this.key = key;
            this.score = score;
            this.lastOccurrence = event.getArrivalTime();
        }

        public AccessEvent getEvent() {
            return event;
        }

        public long getKey() {
            return key;
        }

        public double getLastOccurrence() {
            return lastOccurrence;
        }

        public void updateLastOccurrence(double time) {
            lastOccurrence = time;
        }

        public double getScore() {
            return score;
        }
    }
}
