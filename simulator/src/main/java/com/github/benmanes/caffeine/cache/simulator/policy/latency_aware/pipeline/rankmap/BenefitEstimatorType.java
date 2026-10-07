package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.rankmap;

import java.util.Locale;

/***
 * How the latency objective values a request to a key that a candidate would have held.
 */
public enum BenefitEstimatorType {
    /*** {@link SequenceBenefitEstimator}: each virtual fetch window's total, credited once to the ranks it opened with. */
    WINDOW,
    /*** {@link ItemBurstCostEstimator}: the item's cost per burst request over everything since it entered the boards. */
    ITEM_CUMULATIVE,
    /*** {@link ItemBurstCostEstimator}: a moving average of the item's cost per burst request, window by window. */
    ITEM_EWMA;

    public static BenefitEstimatorType parse(String value) {
        return switch (value.trim().toLowerCase(Locale.US)) {
            case "window" -> WINDOW;
            case "item-cumulative" -> ITEM_CUMULATIVE;
            case "item-ewma" -> ITEM_EWMA;
            default -> throw new IllegalArgumentException("No such benefit estimator: " + value);
        };
    }
}
