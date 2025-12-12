package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline;

import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.admission.Admittor;
import com.github.benmanes.caffeine.cache.simulator.admission.TinyLfu;
import com.github.benmanes.caffeine.cache.simulator.admission.UneditableAdmittorProxy;
import com.github.benmanes.caffeine.cache.simulator.policy.EntryData;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.google.common.base.MoreObjects;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("NullAway")
public class LfuBlock implements PipelineBlock {
    private static int ID = 0;
    final private int quantumSize;
    private Long2ObjectMap<Node> items;
    final private Admittor admittor;
    final private Node protectedHead;
    final private Node probationHead;

    private int protectedSize = 0;
    private int probationSize = 0;
    private int protectedCapacity;
    private int cacheCapacity;

    private int id;

    public LfuBlock(int quanta, int quantumSize, Config config) {
        this.quantumSize = quantumSize;
        this.cacheCapacity = this.quantumSize * quanta;
        this.items = new Long2ObjectOpenHashMap<>();
        this.items.defaultReturnValue(null);
        this.admittor = new TinyLfu(config, new PolicyStats("fake"));
        this.protectedHead = new Node(QueueType.PROTECTED);
        this.probationHead = new Node(QueueType.PROBATION);
        this.protectedCapacity = quanta > 1 ? (quanta - 1) * quantumSize : quanta * quantumSize;
        this.id = ID++;
    }

    private LfuBlock(LfuBlock other) {
        this.quantumSize = other.quantumSize;
        this.cacheCapacity = other.cacheCapacity;
        this.items = new Long2ObjectOpenHashMap<>();
        this.items.defaultReturnValue(null);
        this.admittor = new UneditableAdmittorProxy(other.admittor);
        this.protectedHead = new Node(QueueType.PROTECTED);
        this.probationHead = new Node(QueueType.PROBATION);
        this.protectedCapacity = other.protectedCapacity;
        this.id = ID++;
        copyItemsFrom(other);
    }


    private void copyItemsFrom(LfuBlock other) {
        copyListInto(other.protectedHead, this.protectedHead, other.protectedSize, QueueType.PROTECTED);
        copyListInto(other.probationHead, this.probationHead, other.probationSize, QueueType.PROBATION);

        validateCopy(other);
    }

    private void copyListInto(Node headOther, Node headThis, int otherSize, QueueType queueType) {
        Node curr = headOther.prev;
        for (int i = 0; i < otherSize; ++i) {
            Node toInsert = new Node(curr.data, queueType);
            items.put(toInsert.data.key(), toInsert);
            toInsert.appendToHead(headThis);
            curr = curr.prev;
            Assert.assertCondition(curr.data != null || i == otherSize - 1, "Trying to insert the sentinel of other");

            switch (queueType) {
                case PROTECTED:
                    ++protectedSize;
                    break;
                case PROBATION:
                    ++probationSize;
                    break;
            }
        }
    }

    private void validateCopy(LfuBlock other) {
        validateList(other.probationHead, this.probationHead);
        validateList(other.protectedHead, this.protectedHead);
    }

    private void validateList(Node otherHead, Node thisHead) {
        Node otherItr = otherHead.next;
        Node thisItr = thisHead.next;
        int i = 0;
        while (otherItr.data != null && thisItr.data != null) {
            int finalI = i++;
            Assert.assertCondition(thisItr.data.key() == otherItr.data.key(), () -> String.format("mismatch at item number %d", finalI));
            otherItr = otherItr.next;
            thisItr = thisItr.next;
        }

        Assert.assertCondition(otherItr.data == null && thisItr.data == null, "Both lists should be of the same length");
    }

    @Override
    public void increaseSize(List<EntryData> items) {
        QueueType queueType;
        Node head;

        if (cacheCapacity == protectedCapacity) {
            queueType = QueueType.PROBATION;
            head = probationHead;
            probationSize += items.size();
        } else {
            queueType = QueueType.PROTECTED;
            head = protectedHead;
            protectedSize += items.size();
            protectedCapacity += quantumSize;
        }

        for (EntryData item : items) {
            Node node = new Node(item, queueType);
            node.appendToHead(head);
            this.items.put(item.key(), node);
        }


        cacheCapacity += quantumSize;
        Assert.assertCondition(size() <= cacheCapacity, "Size exceeding the maximum capacity");
        Assert.assertCondition(size() == this.items.size(), "The data size and size field mismatch");
    }

    @Override
    public List<EntryData> decreaseSize() {
        Assert.assertCondition(cacheCapacity > 0, "Decreasing from empty block");
        final int numOfItems = Math.min(size(), quantumSize);
        List<EntryData> evictedItems = new ArrayList<>(numOfItems);

        for (int i = 0; i < numOfItems; ++i) {
            if (probationSize <= cacheCapacity - protectedCapacity && protectedSize > 0) {
                Node demotee = this.protectedHead.next;
                demotee.remove();
                demotee.appendToTail(probationHead);
                demotee.queue = QueueType.PROBATION;
                --protectedSize;
                ++probationSize;
            }

            Node victim = this.probationHead.next;
            EntryData victimData = victim.data;
            victim.remove();
            --probationSize;
            this.items.remove(victimData.key());

            evictedItems.add(victimData);
        }

        cacheCapacity -= quantumSize;

        if (protectedCapacity > 0) {
            protectedCapacity -= quantumSize;
        }

        this.validate();

        Assert.assertCondition(size() == items.size(), "size mismatch");

        return evictedItems;
    }

    @Override
    public PipelineBlock createCopy() {
        return new LfuBlock(this);
    }

    @Nullable
    @Override
    public EntryData getEntry(long key) {
        EntryData res = null;
        Node node = this.items.get(key);
        if (node != null) {
            res = node.data;
            switch (node.queue) {
                case PROBATION:
                    promoteToProtected(node);
                    break;
                case PROTECTED:
                    node.moveToTail(protectedHead);
                    break;
            }
        }

//        Assert.assertCondition(countItemsInList(probationHead) == probationSize, "probation size mismatch after get");
//        Assert.assertCondition(countItemsInList(protectedHead) == protectedSize, "protected size mismatch after get");

        return res;
    }

    @Nullable
    @Override
    public EntryData insert(EntryData data) {
        EntryData evicted = null;
        final int sizeBefore = size();

        if (cacheCapacity == 0) {
            return data;
        }

        if (size() >= cacheCapacity) {
            EntryData victim = getVictim();
            Assert.assertCondition(victim != data, "Got the same item");
            boolean shouldAdmit = admittor.admit(data.key(), victim.key());

            if (shouldAdmit) {
                Node evicteeNode = getVictimNode();
                evicteeNode.remove();
                this.items.remove(evicteeNode.data.key());
                evicted = evicteeNode.data;
                if (evicteeNode.queue == QueueType.PROTECTED) {
                        --protectedSize;
                        ++probationSize;
                }

                Node newItem = new Node(data, QueueType.PROBATION);
                newItem.appendToTail(probationHead);
                items.put(newItem.data.key(), newItem);
            } else {
                evicted = data;
            }
        } else {
            Node newItem = new Node(data, QueueType.PROBATION);
            newItem.appendToTail(probationHead);
            items.put(newItem.data.key(), newItem);

            ++probationSize;
        }

        Assert.assertCondition(protectedSize <= protectedCapacity && size() <= cacheCapacity,
                               "LFU: Size overflow");
        Assert.assertCondition(size() == items.size(), "size mismatch");
//        Assert.assertCondition(countItemsInList(probationHead) == probationSize, "probation size mismatch after insert");
//        Assert.assertCondition(countItemsInList(protectedHead) == protectedSize, "protected size mismatch after insert");

        Assert.assertCondition(sizeBefore < capacity() || evicted != null, "Got no evicted item when the cache is full");

        return evicted;
    }

    private void promoteToProtected(Node node) {
        node.remove();
        node.queue = QueueType.PROTECTED;
        node.appendToTail(protectedHead);

        if (protectedSize == protectedCapacity) {
            Node demotee = protectedHead.next;
            demotee.remove();
            demotee.queue = QueueType.PROBATION;
            Assert.assertCondition(demotee != protectedHead, "Removing the sentinel of PROTECTED");
            demotee.appendToTail(probationHead);
        } else {
            ++protectedSize;
            --probationSize;
        }

        Assert.assertCondition(protectedSize <= protectedCapacity && size() <= cacheCapacity,
                               "LFU: Size overflow after promotion");

//        Assert.assertCondition(countItemsInList(probationHead) == probationSize, "Probation size mismatch after promotion");
//        Assert.assertCondition(countItemsInList(protectedHead) == protectedSize, "Protected size mismatch after promotion");
    }

    private Node getVictimNode() {
        return probationSize > 0 ? probationHead.next : protectedHead.next;
    }

    @Override
    public EntryData getVictim() {
        return getVictimNode().data;
    }

    @Override
    public int size() {
        return probationSize + protectedSize;
    }

    @Override
    public int capacity() {
        return cacheCapacity;
    }

    @Override
    public void validate() {
        Assert.assertCondition(protectedSize + probationSize <= cacheCapacity, "Capacity overflow");
        Assert.assertCondition(protectedSize <= protectedCapacity, "Protected overflow");
        Assert.assertCondition(protectedSize + probationSize == this.items.size(), "The data size and the lists sizes mismatch");
    }

    @Override
    public void bookkeeping(long key) {
        admittor.record(key);
    }

    @Override
    public void clear() {
        items.clear();

        Node curr = probationHead.next;
        for (int idx = 0; idx < probationSize; ++idx) {
            Node temp = curr;
            curr = curr.next;
            temp.remove();
            Assert.assertCondition(curr.data != null || idx == probationSize - 1, id + ": Trying to remove the probation sentinel");
        }

        curr = protectedHead.next;
        for (int idx = 0; idx < protectedSize; ++idx) {
            Node temp = curr;
            curr = curr.next;
            temp.remove();
            Assert.assertCondition(curr.data != null || idx == protectedSize - 1, id + ": Trying to remove the protected sentinel");
        }

        probationSize = 0;
        protectedSize = 0;

        Assert.assertCondition(probationHead.data == null
                               && probationHead.next == probationHead
                               && probationHead.prev == probationHead, "The probation wasn't cleared properly");
        Assert.assertCondition(protectedHead.data == null
                               && protectedHead.next == protectedHead
                               && protectedHead.prev == protectedHead, "The probation wasn't cleared properly");
    }

    @Override
    public void copyInto(PipelineBlock other) {
        if (!(other instanceof LfuBlock)) {
            throw new ClassCastException();
        }

        LfuBlock otherLfu = (LfuBlock) other;
        otherLfu.copyItemsFrom(this);
        otherLfu.protectedCapacity = this.protectedCapacity;
        otherLfu.cacheCapacity = this.cacheCapacity;
    }

    @Override
    public String type() {
        return "LFU";
    }

    private enum QueueType {
        PROBATION,
        PROTECTED
    }

//    private int countItemsInList(Node head) {
//        int count = 0;
//        Node curr = head.next;
//        while (curr != head) {
//            ++count;
//            curr = curr.next;
//        }
//
//        return count;
//    }

    static final class Node {
        @Nullable
        final EntryData data;

        QueueType queue;
        @Nullable
        Node prev;
        @Nullable
        Node next;

        /**
         * Creates a new sentinel node.
         */
        public Node(QueueType queue) {
            this.data = null;
            this.prev = this;
            this.next = this;
            this.queue = queue;
        }

        /**
         * Creates a new, unlinked node.
         */
        public Node(EntryData data, QueueType queue) {
            this.data = data;
            this.queue = queue;
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
            prev.next = next;
            next.prev = prev;
            next = prev = null;
        }

        @Override
        public String toString() {
            return data != null
                   ? MoreObjects.toStringHelper(this)
                                .add("key", data.key())
                                .add("queue", queue)
                                .toString()
                   : "Sentinel of " + this.queue;
        }
    }
}
