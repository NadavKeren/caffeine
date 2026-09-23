package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.rankmap;

import com.typesafe.config.Config;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/***
 * Replays the benefit windows a {@link BenefitDataWriter} recorded, handing each one out at the
 * request it closed at - exactly where the live estimator would have emitted it.
 */
public final class BenefitDataReader {
    private static final long EXHAUSTED = Long.MAX_VALUE;

    final private Path path;
    final private DataInputStream in;
    final private int stageCount;
    final private int rankBytes;
    final private int flushInterval;

    private long nextIndex;
    private boolean closed = false;

    public BenefitDataReader(Config config, Path path, int expectedStages) {
        this.path = path;

        try {
            this.in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path), 1 << 20));

            if (in.readInt() != RankDataFormat.BENEFIT_MAGIC) {
                throw new IllegalStateException("Not a benefit window recording: " + path);
            }

            final int version = in.readInt();
            if (version != RankDataFormat.VERSION) {
                throw new IllegalStateException(String.format(
                        "%s was written by version %d, this is version %d",
                        path, version, RankDataFormat.VERSION));
            }

            this.stageCount = in.readInt();
            this.rankBytes = in.readUnsignedByte();
            this.flushInterval = in.readInt();

            final String recorded = in.readUTF();
            final String expected = RankDataFormat.fingerprint(config);
            if (!recorded.equals(expected) || stageCount != expectedStages) {
                throw new IllegalStateException(String.format(
                        "%s does not match this run.%n  recorded: %s%n  this run: %s",
                        path, recorded, expected));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read the benefit recording at " + path, e);
        }

        this.nextIndex = readIndex();
    }

    /*** How often the recording closed every open window, whether or not a decision fell there. */
    public int flushInterval() {
        return flushInterval;
    }

    /*** Hands out every window that closed at this request. */
    public void emit(long requestIndex, SequenceBenefitEstimator.Sink sink) {
        if (nextIndex < requestIndex) {
            throw new IllegalStateException(String.format(
                    "%s has a window closing at request %d, which this run passed without reading it",
                    path, nextIndex));
        }

        while (nextIndex == requestIndex) {
            try {
                int[] ranks = new int[stageCount];
                for (int stage = 0; stage < stageCount; ++stage) {
                    ranks[stage] = rankBytes == 2 ? in.readUnsignedShort() : in.readInt();
                }
                final double benefit = in.readDouble();

                sink.accept(ranks, benefit);
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot read a benefit window from " + path, e);
            }

            nextIndex = readIndex();
        }
    }

    private long readIndex() {
        try {
            return in.readLong();
        } catch (EOFException e) {
            return EXHAUSTED;
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read a benefit window from " + path, e);
        }
    }

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
