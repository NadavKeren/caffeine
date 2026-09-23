package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.rankmap;

import static com.google.common.truth.Truth.assertThat;

import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * The point of the estimator is that a dense run of requests to one key is worth less than the same
 * number of independent misses, because everything after the first one is a delayed hit.
 */
final class SequenceBenefitEstimatorTest {
    private static final int PARTITIONS = 4;
    private static final double LATENCY = 100;

    private static Config config() {
        return ConfigFactory.parseString("""
            pipeline {
              num-of-blocks = 2
              num-of-quanta = 4
              quantum-size = 4
              burst {
                aging-window-size = 50
                age-smoothing = 0.0025
                number-of-partitions = %d
                type = "sketch"
                sketch { eps = 0.0001, confidence = 0.99 }
              }
              blocks {
                0 { type = "LRU", quota = 2 }
                1 { type = "LFU", quota = 2 }
              }
            }
            """.formatted(PARTITIONS));
    }

    private record Emission(int[] ranks, double benefit) {}

    @Test
    void aBurstInsideOneFetchWindowPaysOnlyTheRemainders() {
        var estimator = new SequenceBenefitEstimator(config(), 16);
        var emitted = new ArrayList<Emission>();
        SequenceBenefitEstimator.Sink sink = (ranks, benefit) -> emitted.add(new Emission(ranks, benefit));

        double[] arrivals = {0, 10, 20};
        int[] ranks = {1, 1};

        for (double arrival : arrivals) {
            estimator.onArrival(event(arrival), ranks, sink);
        }

        assertThat(emitted).isEmpty(); // the window is still open
        estimator.flush(sink);

        double elapsedTotal = 0 + 10 + 20;
        double expected = arrivals.length * LATENCY - elapsedTotal;

        assertThat(emitted).hasSize(1);
        assertThat(emitted.get(0).benefit()).isWithin(1e-9).of(expected);
        assertThat(expected).isLessThan(arrivals.length * LATENCY);
    }

    @Test
    void requestsBeyondTheWindowStartAFreshFetch() {
        var estimator = new SequenceBenefitEstimator(config(), 16);
        var emitted = new ArrayList<Emission>();
        SequenceBenefitEstimator.Sink sink = (ranks, benefit) -> emitted.add(new Emission(ranks, benefit));

        int[] first = {1, 1};
        int[] second = {2, 2};

        estimator.onArrival(event(0), first, sink);
        // Well past the latency, so the first window has expired and a new one opens.
        estimator.onArrival(event(500), second, sink);

        assertThat(emitted).hasSize(1);
        assertThat(emitted.get(0).benefit()).isWithin(1e-9).of(LATENCY);
        assertThat(emitted.get(0).ranks()).isEqualTo(first);

        estimator.flush(sink);
        assertThat(emitted).hasSize(2);
        assertThat(emitted.get(1).benefit()).isWithin(1e-9).of(LATENCY);
        assertThat(emitted.get(1).ranks()).isEqualTo(second);
    }

    @Test
    void theEmittedRanksAreTheOnesTheWindowOpenedWith() {
        var estimator = new SequenceBenefitEstimator(config(), 16);
        List<Emission> emitted = new ArrayList<>();
        SequenceBenefitEstimator.Sink sink = (ranks, benefit) -> emitted.add(new Emission(ranks, benefit));

        int[] atOpen = {3, 7};
        estimator.onArrival(event(0), atOpen, sink);
        estimator.onArrival(event(5), new int[] {1, 1}, sink);
        estimator.flush(sink);

        assertThat(emitted).hasSize(1);
        assertThat(emitted.get(0).ranks()).isEqualTo(atOpen);
    }

    private static AccessEvent event(double arrivalTime) {
        return AccessEvent.forKeyPenaltiesAndArrivalTime(42, 0, LATENCY, arrivalTime);
    }
}
