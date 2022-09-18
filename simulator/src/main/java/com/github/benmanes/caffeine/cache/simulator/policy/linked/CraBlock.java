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


import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
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
 */
public final class CraBlock {

  final Long2ObjectMap<Node> data;
  final Node[] lists;
  final Set<Integer> activeLists;
  final long maximumSize;
  private final long resetCount;
  private double normalizationBias;
  private double normalizationFactor;
  private final double k;
  private final int maxLists;
  private int reqCount;
  private int currOp;
  private long lastReset;
  private int currentSize;
  private final LatencyEstimator<Long> latencyEstimator;

  public CraBlock(double k, int maxLists, long maximumSize, LatencyEstimator<Long> latencyEstimator) {
    this.maximumSize = maximumSize;
    this.activeLists = new HashSet<>();
    this.currOp = 1;
    this.data = new Long2ObjectOpenHashMap<>();
    this.lists = new Node[maxLists+1];
    for (int i = 0; i <= maxLists; i++) {
      this.lists[i] = new Node(i);
    }
    this.resetCount = maximumSize;
    this.maxLists = maxLists;
    this.lastReset = System.nanoTime();
    this.reqCount = 0;
    this.currentSize = 0;
    this.k = k;
    this.latencyEstimator = latencyEstimator;
  }

  public List<Long> record(AccessEvent event) {
    final int weight = event.weight();
    final long key = event.key();
    Node old = data.get(key);
    reqCount++;
    if (reqCount > resetCount) {
      reqCount = 0;
      lastReset = System.nanoTime();
      currOp >>= 1;
    }
    if (old == null) {
      if (weight > maximumSize) {
        return new ArrayList<>();
      }
      currentSize += weight;
      return evict(event);
    } else {
      return onAccess(old);
    }
  }


  public void setNormalization(double normalizationBias, double normalizationFactor) {
    this.normalizationBias = normalizationBias;
    this.normalizationFactor = normalizationFactor;
  }

  private int findList(AccessEvent candidate) {
    int listNum = 0;
    double delta = latencyEstimator.getDelta(candidate.key());

    if (delta < 0) {
      int expectedListNum = (int) ((delta - normalizationBias) / normalizationFactor);
      listNum = Math.max(1, Math.min(expectedListNum, maxLists));
    }

    return listNum;
  }

  /**
   * Evicts while the map exceeds the maximum capacity.
   */
  public List<Long> evict(AccessEvent candidate) {
    int listIndex;
    Node victim;
    Node inSentinel;
    Node victimSent;
    List<Long> evictions = new ArrayList<>();
    long currKey;
    listIndex = findList(candidate);
    inSentinel = lists[listIndex];
    if (currentSize > maximumSize) {
      while (currentSize > maximumSize) {
        victim = findVictim();
        int victimListIndex = victim.sentinel.index;
        currentSize -= victim.event.weight();
        data.remove(victim.key);
        currKey = victim.key;
        victimSent = victim.sentinel;
        victim.remove();
        if (victimSent.size <= 0 && victimSent != inSentinel) {
          activeLists.remove(victimListIndex);
        }
        evictions.add(currKey);
      }
      addToList(candidate, inSentinel);
      activeLists.add(listIndex);
      return evictions;
    } else {
      activeLists.add(listIndex);
      addToList(candidate, inSentinel);
      return new ArrayList<>();
    }
  }

  private Node addToList(AccessEvent candidate, Node inSentinel) {
    Node node = new Node(candidate, candidate.weight(), inSentinel);
    data.put(candidate.key(), node);
    node.appendToTail();
    node.updateOp(currOp++);
    return node;
  }

  private Node addToList(Node node, Node inSentinel) {
    node.sentinel = inSentinel;
    data.put(node.key, node);
    node.appendToTail();
    node.updateOp(currOp++);
    return node;
  }

  public Node findVictim() {
    double rank;
    Node currSentinel;
    Node victim = null;
    double currMaxDelta = -1;
    double minRank = Double.MAX_VALUE;
    if (activeLists.contains(0)){
      currSentinel = lists[0];
      if (currSentinel.next != currSentinel) {
        return currSentinel.next;
      }
    }
    for (int i : activeLists) {
      currSentinel = lists[i];
      if (currSentinel.size == 0) {
        continue;
      }
      Node currVictim = currSentinel.next;
      double currDelta = latencyEstimator.getLatencyEstimation(currVictim.event.key());
      currMaxDelta = Math.max(currDelta, currMaxDelta);

      if (currVictim.lastTouch < lastReset) {
        currVictim.resetOp();
      }

      rank = Math.signum(currDelta) * Math.pow(Math.abs(currDelta), Math.pow((double) currOp - currVictim.lastOp, -k));
      if (rank < minRank || victim == null || (rank == minRank
              && (double) currVictim.lastOp / currOp < (double) victim.lastOp / currOp)) {
        minRank = rank;
        victim = currVictim;
      }
    }
    checkState(victim != null, "CRA Block - maxlists: %s\n\n victim is null! activeLists = %s\nlists=%s", maxLists, java.util.Arrays.toString(activeLists.toArray()),java.util.Arrays.toString(lists));
    return victim;
  }
  public void remove(long key){
    data.remove(key);
  }

  public Node addEntry(AccessEvent event){
    int listIndex = findList(event);
    activeLists.add(listIndex);
    return addToList(event,lists[listIndex]);
  }

  public Node addEntry(Node node){
    int listIndex = findList(node.event());
    activeLists.add(listIndex);
    return addToList(node,lists[listIndex]);
  }

  public boolean isHit(long key){
    return data.containsKey(key);
  }

  List<Long> onAccess(Node node) {
    Node head = node.sentinel;
    if (latencyEstimator.getDelta(node.event.key()) < 0) {
      data.remove(node.key);
      long key = node.key;
      node.remove();
      if (head.size <= 0) {
        activeLists.remove(head.index);
      }
      List<Long> l = new ArrayList<>();
      l.add(key);
      return l;
    } else {
      int index = findList(node.event);
      if (index != head.index){
        node.remove();
        if (head.size == 0) {
          activeLists.remove(head.index);
        }
        node.sentinel = lists[index];
        lists[index].size += 1;
        node.moveToTail(currOp++);
      } else {
        node.moveToTail(currOp++);
      }
      return new ArrayList<>();
    }
  }
  public String type() {
    return (maxLists == 1) ? "LRU" : "LRBB";
  }


  /**
   * A node on the double-linked list.
   */
  public static final class Node {

    Node sentinel;
    int size;
    Node prev;
    Node next;
    long key;
    int weight;
    AccessEvent event;
    long lastOp;
    long lastTouch;
    double totalBenefit;
    int index;

    /**
     * Creates a new sentinel node.
     */
    public Node(int index) {
      this.key = Long.MIN_VALUE;
      this.sentinel = this;
      this.prev = this;
      this.next = this;
      this.event = null;
      this.lastOp = 1;
      this.size = 0;
      this.totalBenefit = 0;
      this.index = index;
    }

    /**
     * Creates a new, unlinked node.
     */
    public Node(AccessEvent event, int weight, Node sentinel) {
      this.sentinel = sentinel;
      this.key = event.key();
      this.weight = weight;
      this.event = event;
      this.lastOp = 1;
    }

    /**
     * Appends the node to the tail of the list.
     */
    public void appendToTail() {
      Node tail = sentinel.prev;
      sentinel.prev = this;
      tail.next = this;
      next = sentinel;
      prev = tail;
      sentinel.size += 1;
    }


    /**
     * Removes the node from the list.
     */
    public void remove() {
      sentinel.size -= 1;
      if (prev != null) {
        prev.next = next;
      }

      if (next != null) {
        next.prev = prev;
      }

      prev = next = null;
    }

    /**
     * Moves the node to the tail.
     */
    public void moveToTail() {
      // unlink
      prev.next = next;
      next.prev = prev;

      // link
      next = sentinel;
      prev = sentinel.prev;
      sentinel.prev = this;
      prev.next = this;

    }

    /**
     * Moves the node to the tail.
     */
    public void moveToTail(long op) {
      // unlink
      prev.next = next;
      next.prev = prev;

      // link
      next = sentinel;
      prev = sentinel.prev;
      sentinel.prev = this;
      prev.next = this;
      lastOp = op;
      sentinel.updateOp(op);
      lastTouch = System.nanoTime();
    }

    /**
     * Updates the node's lastop without moving it
     */
    public void updateOp(long op) {
      lastOp = op;
      lastTouch = System.nanoTime();
    }

    /**
     * Updates the node's lastop without moving it
     */
    public void resetOp() {
      lastOp = Math.max(1, lastOp >> 1);
      lastTouch = System.nanoTime();
    }

    public int getSize() {
      return this.sentinel.size;
    }

    public AccessEvent event() {
      return this.event;
    }

    public long key() {
      return key;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
              .add("key", key)
              .add("weight", weight)
              .add("size",size)
              .toString();
    }
  }

}

