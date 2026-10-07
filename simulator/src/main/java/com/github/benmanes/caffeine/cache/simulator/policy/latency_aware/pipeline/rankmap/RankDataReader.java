package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.rankmap;

import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.LongConsumer;

/***
 * Replays a recorded set of shadow rankings. No board is maintained, so the whole order-statistic
 * cost of the ranking disappears and only the depth solver and the candidate scoring remain.
 * <p>
 * Every run replays the same per-request ranks. Board snapshots are only consulted at a decision,
 * so a run whose decision interval is a multiple of the recorded snapshot interval skips the ones in
 * between rather than decoding them.
 */
public final class RankDataReader implements RankSource {
    final private Path path;
    final private DataInputStream in;
    final private int capacity;
    final private int stageCount;
    final private int snapshotInterval;
    final private int decisionInterval;
    final private int rankBytes;
    final private long warmAt;

    private long[][] snapshots;
    /*** Everything that may have left the boards since the last drain: the members then, and every key requested since. */
    private LongOpenHashSet mayHaveDeparted = new LongOpenHashSet();
    private long requestIndex = 0;
    private boolean closed = false;

    public RankDataReader(Config config, Path path, int decisionInterval) {
        this.path = path;
        this.decisionInterval = decisionInterval;

        try {
            this.in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path), 1 << 20));

            final int magic = in.readInt();
            if (magic != RankDataFormat.MAGIC) {
                throw new IllegalStateException("Not a shadow ranking recording: " + path);
            }

            final int version = in.readInt();
            if (version != RankDataFormat.VERSION) {
                throw new IllegalStateException(String.format(
                        "%s was written by version %d, this is version %d",
                        path, version, RankDataFormat.VERSION));
            }

            this.capacity = in.readInt();
            this.stageCount = in.readInt();
            this.snapshotInterval = in.readInt();
            this.rankBytes = in.readUnsignedByte();
            this.warmAt = in.readLong();

            final String recorded = in.readUTF();
            final String expected = RankDataFormat.fingerprint(config);
            if (!recorded.equals(expected)) {
                throw new IllegalStateException(String.format(
                        "%s does not match this run.%n  recorded: %s%n  this run: %s",
                        path, recorded, expected));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read the shadow ranking recording at " + path, e);
        }

        if (snapshotInterval <= 0 || decisionInterval % snapshotInterval != 0) {
            throw new IllegalStateException(String.format(
                    "%s has a snapshot every %d requests, which does not line up with a decision every %d",
                    path, snapshotInterval, decisionInterval));
        }

        this.snapshots = new long[stageCount][];
        readSnapshots(true);
    }

    public int snapshotInterval() {
        return snapshotInterval;
    }

    public Path path() {
        return path;
    }

    @Override
    public int stageCount() {
        return stageCount;
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public int[] preAccessRanks(long key) {
        mayHaveDeparted.add(key);

        // A fresh array each request: a benefit window holds on to the vector it opened with.
        int[] ranks = new int[stageCount];

        try {
            for (int stage = 0; stage < stageCount; ++stage) {
                ranks[stage] = rankBytes == 2 ? in.readUnsignedShort() : in.readInt();
            }
        } catch (EOFException e) {
            throw new IllegalStateException(String.format(
                    "%s ran out after %d requests; it was recorded over a shorter trace",
                    path, requestIndex), e);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read a rank vector from " + path, e);
        }

        return ranks;
    }

    @Override
    public long[] snapshotDescending(int stage, int count) {
        long[] snapshot = snapshots[stage];

        return count >= snapshot.length ? snapshot : java.util.Arrays.copyOf(snapshot, count);
    }

    @Override
    public void update(AccessEvent event) {
        ++requestIndex;

        if (requestIndex % snapshotInterval == 0) {
            readSnapshots(requestIndex % decisionInterval == 0);
        }
    }

    private void readSnapshots(boolean needed) {
        try {
            for (int stage = 0; stage < stageCount; ++stage) {
                final int length = in.readInt();

                if (!needed) {
                    in.skipNBytes((long) length * Long.BYTES);
                    continue;
                }

                long[] snapshot = new long[length];

                for (int idx = 0; idx < length; ++idx) {
                    snapshot[idx] = in.readLong();
                }

                snapshots[stage] = snapshot;
            }
        } catch (EOFException e) {
            throw new IllegalStateException(String.format(
                    "%s ran out of snapshots after %d requests; it was recorded over a shorter trace",
                    path, requestIndex), e);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read a board snapshot from " + path, e);
        }
    }

    /***
     * There is no eviction stream in a recording, so the departures are recovered from membership: a
     * key that was on a board at the last drain, or was requested since, and is in no snapshot now.
     * The live boards report exactly that set, since every such key must have been evicted or
     * declined on the way.
     */
    @Override
    public void drainDepartures(LongConsumer consumer) {
        var members = new LongOpenHashSet();
        for (long[] snapshot : snapshots) {
            for (long key : snapshot) {
                members.add(key);
            }
        }

        for (LongIterator iterator = mayHaveDeparted.iterator(); iterator.hasNext(); ) {
            final long key = iterator.nextLong();

            if (!members.contains(key)) {
                consumer.accept(key);
            }
        }

        mayHaveDeparted = members;
    }

    @Override
    public boolean isWarm() {
        return warmAt >= 0 && requestIndex >= warmAt;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        try {
            in.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot close " + path, e);
        }
    }
}
