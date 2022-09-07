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

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.admission.Admission;
import com.github.benmanes.caffeine.cache.simulator.admission.Admittor;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.google.common.base.MoreObjects;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;


import java.util.*;

import static com.google.common.base.Preconditions.checkState;

/**
 * A cache that uses multiple linked lists, each holding entries with close range of benefit to
 * utilize access times to create a simple "latency aware" replacement algorithms.
 *
 * @author himelbrand@gmail.com (Omri Himelbrand)
 */
@Policy.PolicySpec(name = "linked.CRA")
public final class CraPolicy implements Policy {

    final Long2ObjectMap<Node> data;
    final Node[] lists;
    final Set<Integer> activeLists;
    final PolicyStats policyStats;
    final Admittor admittor;
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
    private double maxDelta;
    private int maxDeltaCounts;
    private int samplesCount;


    public CraPolicy(Admission admission, Config config, int k, int maxLists) {
        BasicSettings settings = new BasicSettings(config);
        this.k = k;
        this.activeLists = new HashSet<>();
        this.policyStats = new PolicyStats(admission.format(String.format("CRA(k=%d,maxLists=%d)", k, maxLists)));
        this.admittor = admission.from(config, policyStats);
        this.maximumSize = settings.maximumSize();
        this.currOp = 1;
        this.data = new Long2ObjectOpenHashMap<>();
        this.maxLists = maxLists;
        this.lists = new Node[maxLists+1];
        for (int i = 0; i <= maxLists; i++) {
            this.lists[i] = new Node(i);
        }
        this.resetCount = maximumSize;
        this.normalizationBias = 0;
        this.normalizationFactor = 0;
        this.lastReset = System.nanoTime();
        this.reqCount = 0;
        this.currentSize = 0;
        this.maxDelta = 0;
        this.maxDeltaCounts = 0;
        this.samplesCount = 0;

    }


    /**
     * Returns all variations of this policy based on the configuration parameters.
     */
    public static Set<Policy> policies(Config config) {
        BasicSettings settings = new BasicSettings(config);
        Set<Policy> policies = new HashSet<>();
        for (Admission admission : settings.admission()) {
            for (int k : settings.cra().kValues()) {
                for (int maxLists : settings.cra().maxLists()) {
                    policies.add(new CraPolicy(admission, config, k, maxLists));
                }
            }
        }
        return policies;
    }

    @Override
    public PolicyStats stats() {
        return policyStats;
    }


    @Override
    public void record(AccessEvent event) {
        final int weight = event.weight();
        final long key = event.key();
        Node old = data.get(key);
        admittor.record(event);
        reqCount++;

        if (reqCount > resetCount) {
            reqCount = 0;
            lastReset = System.nanoTime();
            currOp >>= 1;
        }

        if (old == null) {
            policyStats.recordWeightedMiss(weight);
            if (weight > maximumSize) {
                policyStats.recordOperation();
                return;
            }
            currentSize += weight;
            if (event.delta() > normalizationFactor){
                samplesCount++;
                maxDelta = (maxDelta*maxDeltaCounts + event.delta())/++maxDeltaCounts;
            }
            normalizationBias = normalizationBias > 0 ? Math.min(normalizationBias,Math.max(0,event.delta())) : Math.max(0,event.delta());
            if (samplesCount%1000 == 0 || normalizationFactor == 0){
                normalizationFactor = maxDelta;
                maxDeltaCounts = 1;
                samplesCount = 0;
            }
            evict(event);
        } else {
            old.event.updateHitPenalty(event.hitPenalty());
            policyStats.recordWeightedHit(weight);
            onAccess(old);
        }
    }

    private int findList(AccessEvent candidate) {
        return candidate.delta() < 0 ? 0 : Math.max(1,Math.min((int) (((candidate.delta() - normalizationBias) / normalizationFactor) * (maxLists+1)),maxLists));
    }

    /**
     * Evicts while the map exceeds the maximum capacity.
     */
    private void evict(AccessEvent candidate) {
        if (currentSize > maximumSize) {
            while (currentSize > maximumSize) {
                Node victim = findVictim();
                findList(victim.event);
                int victimListIndex = victim.sentinel.index;
                policyStats.recordEviction();
                boolean admit = admittor.admit(candidate, victim.event);
                if (admit) {
                    int listIndex = findList(candidate);
                    Node inSentinel = lists[listIndex];
                    activeLists.add(listIndex);

                    currentSize -= victim.event.weight();
                    data.remove(victim.key);
                    Node victimSentinel = victim.sentinel;
                    try {
                        victim.remove();
                    } catch (RuntimeException e){
                        System.out.println(victim);
                        System.out.println(victim.event);
                        System.out.println(victimListIndex);
                        throw e;
                    }
                    if (victimSentinel.size == 0 && victimSentinel != inSentinel) {
                        activeLists.remove(victimListIndex);
                    }
                    addToList(candidate, inSentinel);

                } else {
                    currentSize -= candidate.weight();
                }
            }
        } else {
            int listIndex = findList(candidate);
            Node inSentinel = lists[listIndex];
            activeLists.add(listIndex);
            policyStats.recordOperation();
            addToList(candidate, inSentinel);
        }
    }

    private void addToList(AccessEvent candidate, Node inSentinel) {
        Node node = new Node(candidate, candidate.weight(), inSentinel);
        data.put(candidate.key(), node);
        node.appendToTail();
        node.updateOp(currOp++);
    }

    private Node findVictim() {
        double rank;
        Node victim = null;
        Node currSentinel;
        Node currVictim;
        double minRank = Double.MAX_VALUE;
        if (activeLists.contains(0)){
            currSentinel = lists[0];
            return currSentinel.next;
        }
        for (int i : activeLists) {
            currSentinel = lists[i];
            if (currSentinel.size == 0) {
                continue;
            }
            currVictim = currSentinel.next;
            if (currVictim.lastTouch < lastReset) {
                currVictim.resetOp();
            }

            rank = Math.signum(currVictim.event.delta()) * Math.pow(Math.abs(currVictim.event.delta()), Math.pow((double) currOp - currVictim.lastOp, -k));
            if (rank < minRank || victim == null || (rank == minRank
                    && (double) currVictim.lastOp / currOp < (double) victim.lastOp / currOp)) {
                minRank = rank;
                victim = currVictim;
            }
        }
        checkState(victim != null, "CRA - victim is null");
        return victim;
    }

    void onAccess(Node node) {
        policyStats.recordOperation();
        Node victimSentinel = node.sentinel;
        if (node.event.delta() < 0) {
            data.remove(node.key);

            node.remove();
            if (victimSentinel.size == 0) {
                activeLists.remove(victimSentinel.index);
            }
        } else {
            int index = findList(node.event);
            if (index != victimSentinel.index) {
                node.remove();
                if (victimSentinel.size == 0) {
                    activeLists.remove(victimSentinel.index);
                }
                node.sentinel = lists[index];
                lists[index].size += 1;
                node.moveToTail(currOp++);
            } else {
                node.moveToTail(currOp++);
            }
        }

    }

    /**
     * A node on the double-linked list.
     */
    static final class Node {

        Node sentinel;
        int size;
        Node prev;
        Node next;
        long key;
        int weight;
        AccessEvent event;
        long lastOp;
        long lastTouch;
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
            prev = null;
            next = null;
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
         * Appends the node to the tail of the list.
         */
        public void appendToTail(long op) {
            Node tail = sentinel.prev;
            sentinel.prev = this;
            tail.next = this;
            next = sentinel;
            prev = tail;
            lastOp = op;
            lastTouch = System.nanoTime();
        }

        /**
         * Removes the node from the list.
         */
        public void remove() {
            sentinel.size -= 1;
            if (this == sentinel) {
                System.out.println(toString());
            }
            prev.next = next;
            next.prev = prev;
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
            if(prev != null) {
                // unlink
                prev.next = next;
                next.prev = prev;
            }
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

