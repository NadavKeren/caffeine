package com.github.benmanes.caffeine.cache.simulator.policy;

public class EntryData {
    private AccessEvent event;
    private long lastOpNum;
    private long lastOpTime;

    public EntryData(AccessEvent event) {
        this.event = event;
    }

    public void recordOperation(long opNum) {
        this.lastOpNum = opNum;
        this.lastOpTime = System.nanoTime();
    }

    public AccessEvent event() {
        return event;
    }

    public long lastOpNum() {
        return lastOpNum;
    }

    public long lastOpTime() {
        return lastOpTime;
    }

    public long key() {
        return event.key();
    }
}
