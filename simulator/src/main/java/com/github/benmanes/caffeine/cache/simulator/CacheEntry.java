package com.github.benmanes.caffeine.cache.simulator;

import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;

public class CacheEntry {
    private AccessEvent event;
    private long lastOperationNum;
    private long lastOperationTime;

    public CacheEntry(AccessEvent event, long operationNum, double benefitScore) {
        this.event = event;
        this.lastOperationNum = operationNum;
        this.lastOperationTime = System.nanoTime();
    }

    public AccessEvent getEvent() { return event; }

    public long getLastOperationNum() { return lastOperationNum; }

    public long getLastOperationTime() { return lastOperationTime; }

    public void recordOperation(long operationNum) {
        this.lastOperationNum = operationNum;
        this.lastOperationTime = System.nanoTime();
    }
}
