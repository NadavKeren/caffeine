/*
 * Copyright 2020 Omri Himelbrand. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.benmanes.caffeine.cache.simulator.policy.linked;


import com.github.benmanes.caffeine.cache.simulator.CacheEntry;
import com.github.benmanes.caffeine.cache.simulator.policy.LatencyEstimator;
import com.google.common.base.MoreObjects;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.*;

import static com.google.common.base.Preconditions.checkState;

/**
 * A cache that uses multiple linked lists, each holding entries with close range of benefit to
 * utilize access times to create a simple "latency aware" replacement algorithms. This is a
 * building block to be used by other policies, just like LRU is being used as a building block.
 *
 * @author himelbrand@gmail.com (Omri Himelbrand)
 * @author nadav.keren@gmail.com (Nadav Keren)
 */
public final class CraBlock {

  private final Long2ObjectMap<Node> data;
  private final Node[] lists;
  private final Set<Integer> activeLists;
  private final double k;
  private final int maxLists;
  private long currentSize;
  private long maximumSize;
  private final boolean isFixedSize;
  private double normalizationBias;
  private double normalizationFactor;
  private int currOpNum;
  private final LatencyEstimator<Long> latencyEstimator;

  public CraBlock(double k, int maxLists, long maximumSize, LatencyEstimator<Long> latencyEstimator, boolean isFixedSize) {
    this.data = new Long2ObjectOpenHashMap<>();

    this.lists = new Node[maxLists+1];
    for (int i = 0; i <= maxLists; ++i) {
      this.lists[i] = new Node(i);
    }

    this.activeLists = new HashSet<>();
    this.k = k;
    this.maxLists = maxLists;

    this.currentSize = 0;
    this.maximumSize = maximumSize;
    this.isFixedSize = isFixedSize;
    this.currOpNum = 1;
    this.latencyEstimator = latencyEstimator;
  }

  public void setNormalization(double normalizationBias, double normalizationFactor) {
    this.normalizationBias = normalizationBias;
    this.normalizationFactor = normalizationFactor;
  }

  private int findExpectedList(long key) {
    int listNum = 0;
    double delta = latencyEstimator.getDelta(key);

    if (delta < 0) {
      int expectedListNum = (int) ((delta - normalizationBias) / normalizationFactor);
      listNum = Math.max(1, Math.min(expectedListNum, maxLists));
    }

    return listNum;
  }

  public void addEntry(CacheEntry entry) {
    int listIndex = findExpectedList(entry.getEvent().key());
    activeLists.add(listIndex);

    entry.recordOperation(currOpNum++);
    Node node = new Node(entry, lists[listIndex]);

    data.put(entry.getEvent().key(), node);
    node.appendToTail();

    ++currentSize;
    checkState(currentSize <= maximumSize, "Exceeded maximum size");
  }

  public boolean isFull() { return currentSize == maximumSize; }

  public void changeMaximalSize(long newMaxSize) {
    if (isFixedSize) {
      throw new UnsupportedOperationException("Block defined as fixed size");
    }

    checkState(currentSize <= newMaxSize, "CRA was not adjusted to the new size");

    maximumSize = newMaxSize;
  }

  public long getMaximumSize() { return maximumSize; }

  public CacheEntry findVictim() {
    Node victim = getVictim();

    return victim.data();
  }

  public CacheEntry removeVictim() {
    Node victim = getVictim();
    innerRemove(victim);

    return victim.data();
  }

  private Node getVictim() {
    double rank;
    Node currSentinel;
    Node victim = null;
    double minRank = Double.MAX_VALUE;

    for (int i : activeLists) {
      currSentinel = lists[i];
      if (currSentinel.size > 0) {
        Node currHead = currSentinel.next;
        double currDelta = latencyEstimator.getDelta(currHead.data().getEvent().key());

        rank = Math.signum(currDelta) * Math.pow(Math.abs(currDelta),
                Math.pow((double) (currOpNum - currHead.data().getLastOperationNum()), -k));

        if (victim == null
                || rank < minRank
                || (rank == minRank && (double) currHead.data().getLastOperationNum() / currOpNum < (double) victim.data().getLastOperationNum() / currOpNum)) {
          minRank = rank;
          victim = currHead;
        }
      }
    }

    checkState(victim != null,
            "Error: null victim in CRA Block.\nactiveLists = %s\nlists=%s",
            maxLists,
            java.util.Arrays.toString(activeLists.toArray()), java.util.Arrays.toString(lists));

    return victim;
  }

  public CacheEntry get(long key) {
    Node node = data.get(key);
    return node != null ? node.data() : null;
  }

  public void moveToTail(long key) {
    Node node = data.get(key);
    node.data().recordOperation(currOpNum++);
    node.moveToTail();
  }

  public CacheEntry remove(long key){
    Node node = data.get(key);
    innerRemove(node);
    return node.data();
  }

  private void innerRemove(Node node) {
    node.remove();
    data.remove(node.data().getEvent().key());
    --currentSize;
  }

  public boolean isHit(long key) {
    return data.containsKey(key);
  }

  public String type() {
    return (maxLists == 1) ? "LRU" : "LRBB";
  }


  /**
   * A node on the double-linked list.
   */
  private static final class Node {

    private Node sentinel;

    private Node prev;

    private Node next;
    private int size;
    private final int listIndex;
    private final CacheEntry data;

    /**
     * Creates a new sentinel node in a cyclic Doubly Linked List
     * @param listIndex - The index of the list in which the sentinel is added
     */
    public Node(int listIndex) {
      this.sentinel = this;
      this.prev = this;
      this.next = this;
      this.size = 0;
      this.listIndex = listIndex;
      this.data = new CacheEntry();
    }

    /**
     * Creates a new, unlinked node.
     */
    public Node(CacheEntry entry, Node sentinel) {
      this.sentinel = sentinel;
      this.prev = null;
      this.next = null;
      this.listIndex = sentinel.listIndex;
      this.data = entry;
    }

    /**
     * Appends the node to the tail of the list.
     */
    public void appendToTail() {
      insert(sentinel.prev, sentinel);
      ++sentinel.size;
    }

    private void insert(Node prev, Node next) {
      next.prev = this;
      this.next = next;
      prev.next = this;
      this.prev = prev;
    }

    public void moveToTail() {
      this.remove();
      this.appendToTail();
    }

    /**
     * Removes the node from the list.
     */
    public void remove() {
      if (prev != null) {
        prev.next = next;
      }

      if (next != null) {
        next.prev = prev;
      }

      prev = null;
      next = null;

      --sentinel.size;
    }

    public int size() {
      return this.sentinel.size;
    }

    public CacheEntry data() {
      return this.data;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
              .add("key", data.getEvent() != null ? data.getEvent().key() : null)
              .add("size", size())
              .toString();
    }
  }
}

