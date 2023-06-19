package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;

public class EntryData {
    private final AccessEvent event;
    private final long key;
    @SuppressWarnings("unused")
    private long lastOpNum;
    @SuppressWarnings("unused")
    private long lastOpTime;

    public EntryData(AccessEvent event) {
        this.event = event;
        this.key = event.key();
        this.lastOpNum = 0;
    }

    public void recordOperation(long opNum) {
        this.lastOpNum = opNum;
    }

    public AccessEvent event() { return event; }
    public long key() { return key; }
    public long lastOpNum() { return lastOpTime; }
}
