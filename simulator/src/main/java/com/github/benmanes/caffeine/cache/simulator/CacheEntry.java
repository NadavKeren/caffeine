package com.github.benmanes.caffeine.cache.simulator;

import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;

public class CacheEntry {
    private AccessEvent event;
    private long lastOperationNum;
    private long lastOperationTime;

    public CacheEntry() {
        this(null);
    }
    public CacheEntry(AccessEvent event) {
        this.event = event;
    }

    public AccessEvent getEvent() { return event; }

    public long getLastOperationNum() { return lastOperationNum; }

    public long getLastOperationTime() { return lastOperationTime; }

    public void recordOperation(long operationNum) {
        lastOperationNum = operationNum;
        lastOperationTime = System.nanoTime();
    }

    public void halveOperationCount() {
        recordOperation(Math.max(1, lastOperationNum >> 1));
    }
}
