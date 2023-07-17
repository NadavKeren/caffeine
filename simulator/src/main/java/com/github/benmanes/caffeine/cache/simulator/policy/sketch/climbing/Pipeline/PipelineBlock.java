package com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing.Pipeline;

import com.github.benmanes.caffeine.cache.simulator.policy.EntryData;

import javax.annotation.Nullable;
import java.util.List;

public interface PipelineBlock {
    void increaseSize(List<EntryData> items);

    List<EntryData> decreaseSize();

    @Nullable EntryData getEntry(long key);

    @Nullable EntryData insert(EntryData data);
    EntryData getVictim();

    int size();
    int capacity();

    void validate();

    double getExpansionBenefit();
    double getShrinkCost();
    void resetStats();
}
