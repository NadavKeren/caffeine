/*
 * Copyright 2015 Ben Manes. All Rights Reserved.
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
package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import static com.google.common.base.Preconditions.checkState;
import static java.util.stream.Collectors.toSet;

import java.util.List;
import java.util.Set;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.admission.Admittor;
import com.github.benmanes.caffeine.cache.simulator.admission.TinyLfu;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy.PolicySpec;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.google.common.base.MoreObjects;
import com.google.common.primitives.Ints;
import com.typesafe.config.Config;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

/**
 * An adaption of the TinyLfu policy that adds a temporal admission window. This window allows the
 * policy to have a high hit rate when entries exhibit a high temporal / low frequency pattern.
 * <p>
 * A new entry starts in the window and remains there as long as it has high temporal locality.
 * Eventually an entry will slip from the end of the window onto the front of the main queue. If the
 * main queue is already full, then a historic frequency filter determines whether to evict the
 * newly admitted entry or the victim entry chosen by main queue's policy. This process ensures that
 * the entries in the main queue have both a high recency and frequency. The window space uses LRU
 * and the main uses Segmented LRU.
 * <p>
 * Scan resistance is achieved by means of the window. Transient data will pass through from the
 * window and not be accepted into the main queue. Responsiveness is maintained by the main queue's
 * LRU and the TinyLfu's reset operation so that expired long term entries fade away.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@PolicySpec(name = "sketch.WindowTinyLfu")
public class WindowTinyLfuPolicy implements Policy {
  protected final Long2ObjectMap<Node> data;
  protected final PolicyStats policyStats;
  protected final Admittor admittor;
  protected final int maximumSize;

  protected final Node headWindow;
  protected final Node headProbation;
  protected final Node headProtected;

  protected int maxWindow;
  protected int maxProtected;

  final protected int maxProbation;

  protected int sizeWindow;
  protected int sizeProtected;
  protected int sizeProbation;

  public WindowTinyLfuPolicy(double percentMain, WindowTinyLfuSettings settings) {
    this.policyStats = new PolicyStats(name() + " (%.0f%%)", 100 * (1.0d - percentMain));
    this.admittor = new TinyLfu(settings.config(), policyStats);
    this.maximumSize = Ints.checkedCast(settings.maximumSize());

    int maxMain = (int) (maximumSize * percentMain);
    this.maxProtected = (int) (maxMain * settings.percentMainProtected());
    this.maxProbation = maxMain - maxProtected;
    this.data = new Long2ObjectOpenHashMap<>();
    this.maxWindow = maximumSize - maxMain;
    this.headProtected = new Node();
    this.headProbation = new Node();
    this.headWindow = new Node();
  }

  public WindowTinyLfuPolicy(int maxMain, int maximumSize, WindowTinyLfuSettings settings, PolicyStats stats) {
    this.maximumSize = maximumSize;
    this.policyStats = stats;
    this.admittor = new TinyLfu(settings.config(), policyStats);

    this.maxProtected = (int) (maxMain * settings.percentMainProtected());
    this.maxProbation = maxMain - maxProtected;
    this.data = new Long2ObjectOpenHashMap<>();
    this.maxWindow = maximumSize - maxMain;
    this.headProtected = new Node();
    this.headProbation = new Node();
    this.headWindow = new Node();
  }

  public WindowTinyLfuPolicy(Admittor admittor,
                             int maximumSize,
                             int maxProtected,
                             int maxProbation,
                             int maxWindow,
                             PolicyStats stats) {
    this.policyStats = stats;
    this.admittor = admittor;
    this.maximumSize = maximumSize;

    this.maxProtected = maxProtected;
    this.maxProbation = maxProbation;
    this.data = new Long2ObjectOpenHashMap<>();
    this.maxWindow = maxWindow;
    this.headProtected = new Node();
    this.headProbation = new Node();
    this.headWindow = new Node();
  }



  /** Returns all variations of this policy based on the configuration parameters. */
  public static Set<Policy> policies(Config config) {
    WindowTinyLfuSettings settings = new WindowTinyLfuSettings(config);
    return settings.percentMain().stream()
        .map(percentMain -> new WindowTinyLfuPolicy(percentMain, settings))
        .collect(toSet());
  }

  @Override
  public PolicyStats stats() {
    return policyStats;
  }

  @Override
  public void record(AccessEvent event) {
    final long key = event.key();
    policyStats.recordOperation();
    Node node = data.get(key);
    if (node == null) {
      onMiss(event);
      policyStats.recordMiss();
      policyStats.recordMissPenalty(event.missPenalty());
      event.changeEventStatus(AccessEvent.EventStatus.MISS);
    } else {
      if (node.status == Status.WINDOW) {
        onWindowHit(node);
      } else if (node.status == Status.PROBATION) {
        onProbationHit(node);
      } else if (node.status == Status.PROTECTED) {
        onProtectedHit(node);
      } else {
        throw new IllegalStateException();
      }

      boolean isAvailable = node.event.isAvailableAt(event.getArrivalTime());
      if (isAvailable) {
        event.changeEventStatus(AccessEvent.EventStatus.HIT);
        policyStats.recordHit();
        policyStats.recordHitPenalty(event.hitPenalty());
      } else {
        event.changeEventStatus(AccessEvent.EventStatus.DELAYED_HIT);
        event.setDelayedHitPenalty(event.getAvailabilityTime());
        policyStats.recordDelayedHitPenalty(event.delayedHitPenalty());
        policyStats.recordDelayedHit();
      }
    }
  }

  /** Adds the entry to the admission window, evicting if necessary. */
  private void onMiss(AccessEvent event) {
    long key = event.key();
    admittor.record(key);

    Node node = new Node(key, Status.WINDOW, event);
    node.appendToTail(headWindow);
    data.put(key, node);
    sizeWindow++;
    evict();
  }

  /** Moves the entry to the MRU position in the admission window. */
  private void onWindowHit(Node node) {
    admittor.record(node.key);
    node.moveToTail(headWindow);
  }

  /** Promotes the entry to the protected region's MRU position, demoting an entry if necessary. */
  private void onProbationHit(Node node) {
    admittor.record(node.key);

    node.remove();
    node.status = Status.PROTECTED;
    node.appendToTail(headProtected);

    sizeProtected++;
    sizeProbation--;
    if (sizeProtected > maxProtected) {
      Node demote = headProtected.next;
      demote.remove();
      demote.status = Status.PROBATION;
      demote.appendToTail(headProbation);
      sizeProtected--;
      sizeProbation++;
    }
  }

  /** Moves the entry to the MRU position if it falls outside of the fast-path threshold. */
  private void onProtectedHit(Node node) {
    admittor.record(node.key);
    node.moveToTail(headProtected);
  }

  @Override
  public boolean isPenaltyAware() { return true; }

  /**
   * Evicts from the admission window into the probation space. If the size exceeds the maximum,
   * then the admission candidate and probation's victim are evaluated and one is evicted.
   */
  private void evict() {
    if (sizeWindow <= maxWindow) {
      return;
    }

    Node candidate = headWindow.next;
    sizeWindow--;

    candidate.remove();
    candidate.status = Status.PROBATION;
    candidate.appendToTail(headProbation);

    sizeProbation++;

    if (data.size() > maximumSize) {
      Node victim = headProbation.next;
      Node evict = admittor.admit(candidate.key, victim.key) ? victim : candidate;
      data.remove(evict.key);
      evict.remove();
      sizeProbation--;

      policyStats.recordEviction();
    }
  }

  @Override
  public void finished() {
    long windowCount = data.values().stream().filter(n -> n.status == Status.WINDOW).count();
    long probationCount = data.values().stream().filter(n -> n.status == Status.PROBATION).count();
    long protectedCount = data.values().stream().filter(n -> n.status == Status.PROTECTED).count();

    checkState(windowCount == sizeWindow,
             "Window size and amount of items in window mismatch: " + windowCount + " vs. " + sizeWindow);
    checkState(protectedCount == sizeProtected,
             "Protected size and amount of items in protected mismatch: " + protectedCount + " vs. " + sizeProtected);
    checkState(probationCount == sizeProbation,
             "Probation size and amount of items in probation mismatch: " + probationCount + " vs. "
                     + sizeProbation);

    checkState(data.size() <= maximumSize);
  }

  enum Status {
    WINDOW, PROBATION, PROTECTED, NONE
  }

  /** A node on the double-linked list. */
  static protected class Node {
    final long key;

    Status status = Status.NONE;
    Node prev = null;
    Node next = null;
    AccessEvent event = null;

    /** Creates a new sentinel node. */
    public Node() {
      this.key = Integer.MIN_VALUE;
      this.prev = this;
      this.next = this;
    }

    /** Creates a new, unlinked node. */
    public Node(long key, Status status, AccessEvent event) {
      this.status = status;
      this.key = key;
      this.event = event;
    }

    public void moveToTail(Node head) {
      remove();
      appendToTail(head);
    }

    /** Appends the node to the tail of the list. */
    public void appendToTail(Node sentinel) {
      Node tail = sentinel.prev;
      sentinel.prev = this;
      tail.next = this;
      next = sentinel;
      prev = tail;
    }

    public void appendToHead(Node sentinel) {
      Node oldHead = sentinel.next;
      sentinel.next = this;
      oldHead.prev = this;
      next = oldHead;
      prev = sentinel;
    }

    /** Removes the node from the list. */
    public void remove() {
      if (isEmpty()) {
        throw new IllegalStateException("Trying to delete the sentinel");
      }

      prev.next = next;
      next.prev = prev;
      next = prev = null;
    }

    public boolean isEmpty() {
      return (next == this) && (prev == this);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("key", key)
          .add("status", status)
          .toString();
    }
  }

  public static final class WindowTinyLfuSettings extends BasicSettings {
    public WindowTinyLfuSettings(Config config) {
      super(config);
    }
    public List<Double> percentMain() {
      return config().getDoubleList("window-tiny-lfu.percent-main");
    }
    public double percentMainProtected() {
      return config().getDouble("window-tiny-lfu.percent-main-protected");
    }
  }
}
