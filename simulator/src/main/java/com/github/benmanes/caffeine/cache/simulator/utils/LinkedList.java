package com.github.benmanes.caffeine.cache.simulator.utils;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class LinkedList<K, V> {
    private Node<K, V> firstSentinel;
    private Node<K, V> lastSentinel;
    private long size;

    public LinkedList() {
        firstSentinel = new Node<>();
        lastSentinel = new Node<>();
        firstSentinel.setNext(lastSentinel);
        lastSentinel.setPrev(firstSentinel);
        size = 0;
    }

    public V remove(K key) {
        Node<K, V> toRemove = this.findNode(key);
        V value = toRemove != null ? toRemove.getData() : null;
        if (value != null) {
            toRemove.remove();
            --this.size;
        }

        return value;
    }

    public void addFirst(K key, V value) {
        Node<K, V> insertedNode = new Node<>(key, value);
        Node<K, V> oldFirst = firstSentinel.getNext();
        insertedNode.insert(firstSentinel, oldFirst);
        ++this.size;
    }

    public void addLast(K key, V value) {
        Node<K, V> insertedNode = new Node<>(key, value);
        Node<K, V> oldLast = lastSentinel.getPrev();
        insertedNode.insert(oldLast, lastSentinel);
        ++this.size;
    }

    public void moveToTail(K key) {
        V element = remove(key);
        addLast(key, element);
    }

    public void moveToHead(K key) {
        V element = remove(key);
        addFirst(key, element);
    }

    public void moveToHeadAndUpdateValue(K key, Consumer<V> updateFunc) {
        V element = remove(key);
        updateFunc.accept(element);
        addFirst(key, element);
    }

    public void moveToTailAndUpdateValue(K key, Consumer<V> updateFunc) {
        V element = remove(key);
        updateFunc.accept(element);
        addLast(key, element);
    }

    public <T> void moveToHeadAndUpdateValue(K key, BiConsumer<V, T> updateFunc, T additionalParam) {
        V element = remove(key);
        updateFunc.accept(element, additionalParam);
        addFirst(key, element);
    }

    public <T> void moveToTailAndUpdateValue(K key, BiConsumer<V, T> updateFunc, T additionalParam) {
        V element = remove(key);
        updateFunc.accept(element, additionalParam);
        addLast(key, element);
    }

    public V get(long index) {
        if (index >= size) {
            throw new IndexOutOfBoundsException(String.format("getting index %d of size %d", index, size));
        }

        Node<K, V> curr = firstSentinel.getNext();
        for (int idx = 0; idx < index; ++idx) {
            curr = curr.getNext();
        }

        return curr.getData();
    }

    public V get(K key) {
        var node = findNode(key);

        return node != null ? node.getData() : null;
    }

    private Node<K, V> findNode(K key) {
        Node<K, V> curr = firstSentinel.getNext();
        Node<K, V> res = null;

        while (curr != lastSentinel && res == null) {
            if (curr.getKey().equals(key)) {
                res = curr;
            }
        }

        return res;
    }

    public boolean isEmpty() { return size == 0; }


    private static class Node<K, V> {
        Node<K, V> prev;
        Node<K, V> next;

        K key;
        V data;

        protected Node() {
            this(null, null, null, null);
        }
        protected Node(K key, V data) {
            this(null, null, key, data);
        }

        protected Node(Node<K, V> prev, Node<K, V> next, K key, V data) {
            this.prev = prev;
            this.next = next;
            this.key = key;
            this.data = data;
        }

        public Node<K, V> getPrev() {
            return prev;
        }

        public void setPrev(Node<K, V> prev) {
            this.prev = prev;
        }

        public Node<K, V> getNext() {
            return next;
        }

        public void setNext(Node<K, V> next) {
            this.next = next;
        }

        public K getKey() { return key; }

        public V getData() {
            return data;
        }

        public void remove() {
            if (prev != null) {
                prev.next = next;
            }

            if (next != null) {
                next.prev = prev;
            }

            prev = null;
            next = null;
        }

        public void insert(Node<K, V> prev, Node<K, V> next) {
            next.setPrev(this);
            this.next = next;
            prev.setNext(this);
            this.prev = prev;
        }
    }
}
