package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.rankmap;

import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;

import java.util.function.LongConsumer;

/***
 * A shadow ranking: one component policy run at the full cache capacity, exposing the position of a
 * key in its own eviction order.
 * <p>
 * Each implementation copies the admission and eviction discipline of the matching
 * {@link com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.PipelineBlock},
 * and replaces its internal ordering structure with an {@link OrderStatisticTree} so that a rank
 * costs {@code O(log capacity)} instead of a list walk.
 * <p>
 * A board is a sensor. It never hands object identities to the real cache; only ranks and ordered
 * snapshots leave it.
 */
public interface RankedBlock {
    /*** The block type string, matching the one the pipeline configuration uses. */
    String type();

    /***
     * The one-based position of the key in this block's eviction order, best first. A key that the
     * block does not hold reports {@code capacity() + 1}.
     */
    int rank(long key);

    /*** Whether the board currently holds the key. */
    boolean contains(long key);

    /*** The best {@code count} keys in order, best first. Shorter when the board is not yet full. */
    long[] snapshotDescending(int count);

    /*** Pre-access bookkeeping, mirroring {@code PipelineBlock.bookkeeping}. */
    default void bookkeeping(long key) {}

    /*** Post-access update: reorder on a hit, admit and evict on a miss. */
    void update(AccessEvent event);

    /***
     * Receives every key this board drops: a victim it evicts, and a requested key it declines to
     * admit. Lets a consumer follow the board's membership without scanning it.
     */
    void onDeparture(LongConsumer listener);

    int size();

    int capacity();
}
