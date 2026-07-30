package com.github.benmanes.caffeine.cache.simulator.policy;

public class EntryData {
    private final AccessEvent event;
    private long lastOpNum;

    public EntryData(AccessEvent event) {
        this.event = event;
        this.lastOpNum = event().eventNum();
    }

    public void recordOperation(long opNum) {
        this.lastOpNum = opNum;
    }

    public AccessEvent event() {
        return event;
    }

    public long lastOpNum() {
        return lastOpNum;
    }

    public long key() {
        return event.key();
    }
}
