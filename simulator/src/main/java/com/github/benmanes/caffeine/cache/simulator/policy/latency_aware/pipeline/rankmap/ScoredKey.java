package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.rankmap;

import java.util.Comparator;

/***
 * An entry of a shadow ranking: a cache key together with the score its block orders by.
 * The key breaks score ties, which is what lets the order-statistic tree treat entries as unique.
 */
public record ScoredKey(double score, long key) {
    public static final Comparator<ScoredKey> ORDER =
            Comparator.comparingDouble(ScoredKey::score).thenComparingLong(ScoredKey::key);
}
