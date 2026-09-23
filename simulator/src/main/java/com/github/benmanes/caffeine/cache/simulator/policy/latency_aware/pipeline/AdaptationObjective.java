package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline;

import java.util.Locale;

/***
 * What an adaptive pipeline controller is trying to optimize.
 * <p>
 * Both values are scored in the same direction - lower is better - so that a controller can pick
 * its allocation with a single argmin and keep one sentinel for an infeasible candidate.
 */
public enum AdaptationObjective {
    /** Minimize the average request penalty, which is equivalent to maximizing the latency saved. */
    LATENCY,
    /** Minimize the miss ratio, which is equivalent to maximizing the hit ratio. */
    HIT_RATIO;

    public static AdaptationObjective parse(String value) {
        return switch (value.trim().toLowerCase(Locale.US)) {
            case "latency", "average-penalty", "penalty" -> LATENCY;
            case "hit-ratio", "hit-rate", "hits" -> HIT_RATIO;
            default -> throw new IllegalArgumentException("No such adaptation objective: " + value);
        };
    }
}
