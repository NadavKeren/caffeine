package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline;

import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.policy.EntryData;
import com.google.common.base.MoreObjects;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class LruBlock implements PipelineBlock {
    private Node sentinel;
    private Long2ObjectMap<Node> items;
    final private int quantumSize;
    private int size;
    private int cacheCapacity;

    public LruBlock(int quanta, int quantumSize) {
        this.sentinel = new Node();
        this.quantumSize = quantumSize;
        this.size = 0;
        this.cacheCapacity = this.quantumSize * quanta;
        this.items = new Long2ObjectOpenHashMap<>();
        this.items.defaultReturnValue(null);
    }

    private LruBlock(LruBlock other) {
        this.sentinel = new Node();
        this.quantumSize = other.quantumSize;
        this.size = other.size;
        this.cacheCapacity = other.cacheCapacity;
        this.items = new Long2ObjectOpenHashMap<>(other.cacheCapacity);
        this.items.defaultReturnValue(null);

        copyItemsFrom(other);
    }

    private void copyItemsFrom(LruBlock other) {
        Node curr = other.sentinel.prev;
        for (int i = 0; i < other.size; ++i) {
            Node toInsert = new Node(curr.data);
            items.put(toInsert.data.key(), toInsert);
            toInsert.appendToHead(sentinel);
            curr = curr.prev;
            Assert.assertCondition(curr.data != null || i == other.size - 1, "Trying to insert the sentinel of other");
        }

        validateCopy(other);
    }

    private void validateCopy(LruBlock other) {
        Node curr = other.sentinel.next;
        Node thisItr = this.sentinel.next;
        for (int i = 0; i < this.size; ++i) {
            int finalI = i;
            Assert.assertCondition(thisItr.data.key() == curr.data.key(), () -> String.format("mismatch at item number %d",
                                                                                              finalI));
            curr = curr.next;
            thisItr = thisItr.next;
        }
    }


    @Override
    public void increaseSize(List<EntryData> items) {
        for (EntryData item : items) {
            Node node = new Node(item);
            node.appendToHead(sentinel);
            this.items.put(item.key(), node);
        }

        size += items.size();
        cacheCapacity += quantumSize;
        Assert.assertCondition(size <= cacheCapacity, "Size exceeding the maximum capacity");
        Assert.assertCondition(size == this.items.size(), "The data size and size field mismatch");
    }

    @Override
    public List<EntryData> decreaseSize() {
        int numItemsToRemove = Math.min(quantumSize, size);

        List<EntryData> removedItems = new ArrayList<>(numItemsToRemove);
        cacheCapacity -= Math.min(quantumSize, cacheCapacity);

        for (int i = 0; i < numItemsToRemove; ++i) {
            Node victim = this.sentinel.next;
            EntryData removedItem = victim.data;
            victim.remove();
            items.remove(victim.data.key());
            removedItems.add(removedItem);
            --size;
        }

        return removedItems;
    }

    @Override
    public PipelineBlock createCopy() {
        return new LruBlock(this);
    }

    @Nullable
    @Override
    public EntryData getEntry(long key) {
        EntryData res = null;
        Node node = items.get(key);
        if (node != null) {
            node.moveToTail(sentinel);
            res = node.data;
        }

        return res;
    }

    @Nullable
    @Override
    public EntryData insert(EntryData data) {
        Node node = new Node(data);
        node.appendToTail(sentinel);
        items.put(node.data.key(), node);

        EntryData res = null;

        if (this.size >= this.cacheCapacity) {
            Node victimNode = getVictimNode();
            items.remove(victimNode.data.key());
            victimNode.remove();
            res = victimNode.data;
        } else {
            ++this.size;
        }

        return res;
    }

    private Node getVictimNode() {
        return sentinel.next;
    }

    @Override
    public EntryData getVictim() {
        return getVictimNode().data;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public int capacity() {
        return cacheCapacity;
    }

    @Override
    public void validate() {
        Assert.assertCondition(size <= cacheCapacity, "Size exceeding the maximum capacity");
        Assert.assertCondition(size == this.items.size(), "The data size and size field mismatch");
    }

    @Override
    public void clear() {
        items.clear();

        Node curr = sentinel.next;
        for (int idx = 0; idx < size; ++idx) {
            Node temp = curr;
            curr = curr.next;
            temp.remove();
        }

        size = 0;

        Assert.assertCondition(sentinel.data == null
                               && sentinel.next == sentinel
                               && sentinel.prev == sentinel, "The list hasn't been cleared properly");
    }

    @Override
    public void copyInto(PipelineBlock other) {
        if (!(other instanceof LruBlock)) {
            throw new ClassCastException("Invalid class cast: " + other.type());
        }

        LruBlock otherLRU = (LruBlock) other;
        otherLRU.copyItemsFrom(this);
        otherLRU.cacheCapacity = this.cacheCapacity;
        otherLRU.size = this.size;
    }

    @Override
    public String type() {
        return "LRU";
    }

    static final class Node {
        final EntryData data;

        Node prev;
        Node next;

        /**
         * Creates a new sentinel node.
         */
        public Node() {
            this.data = null;
            this.prev = this;
            this.next = this;
        }

        /**
         * Creates a new, unlinked node.
         */
        public Node(EntryData data) {
            this.data = data;
        }

        public void moveToTail(Node head) {
            remove();
            appendToTail(head);
        }

        /**
         * Appends the node to the tail of the list.
         */
        public void appendToHead(Node head) {
            Node first = head.next;
            head.next = this;
            first.prev = this;
            prev = head;
            next = first;
        }

        /**
         * Appends the node to the tail of the list.
         */
        public void appendToTail(Node head) {
            Node tail = head.prev;
            head.prev = this;
            tail.next = this;
            next = head;
            prev = tail;
        }

        /**
         * Removes the node from the list.
         */
        public void remove() {
            Assert.assertCondition(this.data != null, "Trying to remove the sentinel");
            prev.next = next;
            next.prev = prev;
            next = prev = null;
        }

        @Override
        public String toString() {
            return data != null
                   ? MoreObjects.toStringHelper(this)
                              .add("key", data.key())
                              .toString()
                   : "Sentinel of LRU";
        }
    }
}
