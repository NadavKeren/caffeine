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

    void copyInto(PipelineBlock other);

    /***
     * Resizes the block in anticipation that it will be filled as part of a copy process.
     * This is a destructive operation available only on sections that where cleared.
     * This isn't intended as a way to increase/decrease the size of the block as part of the pipeline change.
     * Use with caution.
     *
     * @param size - a non-negative number smaller or equal to the total cache size.
     */
    void setSize(int size);
}
