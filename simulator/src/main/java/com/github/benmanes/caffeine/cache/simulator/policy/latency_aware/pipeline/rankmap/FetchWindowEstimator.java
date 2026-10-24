package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.rankmap;

import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

/***
 * Values a request by what it costs a candidate that did not hold its key when the current fetch
 * window opened.
 * <p>
 * A candidate that misses a key pays the miss penalty once and then a delayed hit for every request
 * until the fetch completes: what is left of the fetch. A candidate that held the key at the window's
 * first request pays none of it. So the whole window is decided by the ranks at its first request, and
 * every request in it is credited to those ranks - the miss penalty for the first, the remainder of
 * the fetch for each later one.
 * <p>
 * This differs from the per-item average in where the credit goes. There, every request is credited to
 * its own ranks, and a key the boards moved to the top at the window's first request collects the rest
 * of the window from candidates that would have missed it. It differs from the partitioned windows of
 * {@link SequenceBenefitEstimator} in that there is one window per fetch, so no request is counted in
 * several overlapping windows.
 * <p>
 * Credits are handed out at once, so a window that spans a decision needs no flushing: the ranks are
 * only numbers, and each credit is scored against the reach depths of the interval it lands in.
 */
public final class FetchWindowEstimator {
    private static final class Window {
        double start;
        double latency;
        int[] ranks;

        Window(double start, double latency, int[] ranks) {
            this.start = start;
            this.latency = latency;
            this.ranks = ranks;
        }

        boolean coversTime(double time) {
            return time - start <= latency;
        }
    }

    final private Long2ObjectMap<Window> windows = new Long2ObjectOpenHashMap<>();

    /*** Credits the request to the ranks its window opened with, opening a window if none covers it. */
    public void onArrival(AccessEvent event, int[] ranks, SequenceBenefitEstimator.Sink sink) {
        final double latency = event.missPenalty();
        if (latency <= 0) {
            return;
        }

        final double time = event.getRequestTime();
        final Window window = windows.get(event.key());

        if (window != null && window.coversTime(time)) {
            sink.accept(window.ranks, window.latency - (time - window.start));
            return;
        }

        if (window == null) {
            windows.put(event.key(), new Window(time, latency, ranks));
        } else {
            window.start = time;
            window.latency = latency;
            window.ranks = ranks;
        }
        sink.accept(ranks, latency);
    }

    /*** Drops the windows that ended before this time, which keeps the map to the keys seen recently. */
    public void forgetExpired(double time) {
        windows.values().removeIf(window -> !window.coversTime(time));
    }

    int openWindows() {
        return windows.size();
    }
}
