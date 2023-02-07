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

  private final Long2ObjectMap<Node> data;
  private final Node[] lists;
  private final Set<Integer> activeLists;
  private final long maximumSize;
  private long size;
  private double normalizationBias;
  private double normalizationFactor;
  private final double decayFactor;
  private final int maxLists;
  private int currOp;
  private final LatencyEstimator<Long> latencyEstimator;

  public CraBlock(double decayFactor, int maxLists, long maximumSize, LatencyEstimator<Long> latencyEstimator) {
    this.data = new Long2ObjectOpenHashMap<>();

    this.lists = new Node[maxLists+1];
    for (int i = 0; i <= maxLists; i++) {
      this.lists[i] = new Node();
    }
    this.activeLists = new HashSet<>();

    this.maximumSize = maximumSize;
    this.size = 0;

    this.decayFactor = decayFactor;
    this.maxLists = maxLists;
    this.currOp = 1;
    this.latencyEstimator = latencyEstimator;
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

  private Node addToList(AccessEvent candidate, Node inSentinel) {
    Node node = new Node(candidate, inSentinel);
    data.put(candidate.key(), node);
    node.appendToTail();
    node.updateOp(currOp++);
    ++size;
    return node;
  }

  private Node addToList(Node node, Node inSentinel) {
    node.sentinel = inSentinel;
    data.put(node.key, node);
    node.appendToTail();
    node.updateOp(currOp++);
    ++size;
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

      rank = Math.signum(currDelta) * Math.pow(Math.abs(currDelta), Math.pow((double) currOp - currVictim.lastOp, -decayFactor));
      if (rank < minRank || victim == null || (rank == minRank
              && (double) currVictim.lastOp / currOp < (double) victim.lastOp / currOp)) {
        minRank = rank;
        victim = currVictim;
      }
    }
    checkState(victim != null,
               "CRA Block - maxlists: %s\n\n victim is null! activeLists = %s\nlists=%s",
               maxLists,
               java.util.Arrays.toString(activeLists.toArray()),
               java.util.Arrays.toString(lists));

    return victim;
  }
  public void remove(long key){
    Node node = data.get(key);
    node.remove();
    data.remove(key);
    --size;
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

  public boolean isFull() { return size >= maximumSize; }

  /**
   * A node on the double-linked list.
   */
  public static final class Node {

    Node sentinel;
    int size;
    Node prev;
    Node next;
    long key;
    AccessEvent event;
    long lastOp;
    long lastTouch;

    /**
     * Creates a new sentinel node.
     */
    public Node() {
      this.sentinel = this;
      this.size = 0;
      this.prev = this;
      this.next = this;
      this.key = Long.MIN_VALUE;
      this.event = null;
      this.lastOp = 1;
    }

    /**
     * Creates a new, unlinked node.
     */
    public Node(AccessEvent event, Node sentinel) {
      this.sentinel = sentinel;
      this.key = event.key();
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
      checkState(prev != null && next != null, "Node already detached");
      --sentinel.size;

      prev.next = next;
      next.prev = prev;

      prev = null;
      next = null;
    }

    /**
     * Moves the node to the tail.
     */
    public void moveToTail() {
      this.remove();
      this.appendToTail();
    }

    /**
     * Updates the node's lastop without moving it
     */
    public void updateOp(long op) {
      lastOp = op;
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
              .add("size",size)
              .toString();
    }
  }

}

