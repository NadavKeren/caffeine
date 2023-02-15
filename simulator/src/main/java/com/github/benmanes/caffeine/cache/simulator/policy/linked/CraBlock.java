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
import com.github.benmanes.caffeine.cache.simulator.policy.EntryData;
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

  private int findList(long key) {
    int listNum = 0;
    double delta = latencyEstimator.getDelta(key);

    if (delta < 0) {
      int expectedListNum = (int) ((delta - normalizationBias) / normalizationFactor);
      listNum = Math.max(1, Math.min(expectedListNum, maxLists));
    }

    return listNum;
  }

  private void addToList(EntryData entry, Node inSentinel) {
    Node newNode = new Node(entry, inSentinel);
    data.put(entry.key(), newNode);
    newNode.appendToTail();
    newNode.data.recordOperation(currOp++);
    ++size;
  }

  public void remove(long key){
    Node node = data.get(key);
    node.remove();
    data.remove(key);
    --size;
  }
  public EntryData addEntry(AccessEvent event){
    EntryData newEntry = new EntryData(event);
    return addEntry(newEntry);
  }

  public EntryData addEntry(EntryData entry){
    int listIndex = findList(entry.key());
    activeLists.add(listIndex);
    addToList(entry, lists[listIndex]);

    return entry;
  }

  private Node getVictim() {
    Node currSentinel;
    Node victim = null;
    double minScore = Double.MAX_VALUE;
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

      double currScore = score(currVictim);
      if (currScore < minScore
          || victim == null
          || (currScore == minScore
              && (double) currVictim.data.lastOpNum() / currOp < (double) victim.data.lastOpNum() / currOp)) {
        minScore = currScore;
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

  /**
   *
   * @param node Non-null node to be scored.
   * @return The score of the node according to the numerical recency estimation defined in the article.
   */
  private double score(Node node) {
    final double delta = latencyEstimator.getLatencyEstimation(node.key());
    final long numOfOpsSinceModified = currOp - node.data.lastOpNum(); // For all nodes lastOpNum < currOp
    final double numericalRecencyScore = Math.pow((double) numOfOpsSinceModified, -decayFactor);

    return Math.signum(delta) * Math.pow(Math.abs(delta), numericalRecencyScore);
  }

  public EntryData findVictim() {
    return getVictim().data();
  }

  public EntryData removeVictim() {
    Node victim = getVictim();
    EntryData res = victim.data;
    remove(victim.key());
    return res;
  }

  public EntryData get(long key) {
    return data.get(key).data();
  }

  public boolean isHit(long key){
    return data.containsKey(key);
  }

  public void moveToTail(EntryData entry) {
    Node node = data.get(entry.key());
    checkState(node != null, "Illegal move to tail");

    node.moveToTail();
  }

  public boolean isFull() { return size > maximumSize; }

  public long size() { return size; }

  public long capacity() { return maximumSize; }

  /**
   * A node on the double-linked list.
   */
  protected static final class Node {

    Node sentinel;
    int size;
    Node prev;
    Node next;
    EntryData data;

    /**
     * Creates a new sentinel node.
     */
    public Node() {
      this.sentinel = this;
      this.size = 0;
      this.prev = this;
      this.next = this;
      this.data = new EntryData(null);
    }

    /**
     * Creates a new, unlinked node.
     */
    public Node(AccessEvent event, Node sentinel) {
      this(new EntryData(event), sentinel);
    }

    /**
     * Creates a new, unlinked node.
     */
    public Node(EntryData data, Node sentinel) {
      this.sentinel = sentinel;
      this.data = data;
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

    public int size() {
      return this.sentinel.size;
    }

    public AccessEvent event() {
      return data.event();
    }

    public long key() {
      return data.key();
    }

    public EntryData data() { return data; }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
              .add("key", key())
              .add("size", size())
              .toString();
    }
  }

}

