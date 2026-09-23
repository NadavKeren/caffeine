package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.rankmap;

import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;

/***
 * Where the controller gets its shadow rankings from: either boards maintained live, or a recording
 * of the boards made by an earlier run over the same trace and cache size.
 */
public interface RankSource {
    int stageCount();

    int capacity();

    /*** The pre-access rank of the key in every board, best-first and one-based. */
    int[] preAccessRanks(long key);

    /*** The best {@code count} keys of a board, in order. Used to recompute the reach depths. */
    long[] snapshotDescending(int stage, int count);

    /*** Advances the boards past this request. */
    void update(AccessEvent event);

    /*** Whether the boards have seen as many distinct keys as the cache can hold. */
    boolean isWarm();

    default void close() {}
}
