package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline;

import com.github.benmanes.caffeine.cache.simulator.policy.EntryData;

import javax.annotation.Nullable;
import java.util.List;

public interface PipelineBlock {
    void increaseSize(List<EntryData> items);

    List<EntryData> decreaseSize();

    PipelineBlock createCopy();


    @Nullable EntryData getEntry(long key);

    default void onMiss(long key) {}

    @Nullable EntryData insert(EntryData data);

    EntryData getVictim();
    int size();

    int capacity();

    void validate();

    default void bookkeeping(long key) {}

    void clear();
}
