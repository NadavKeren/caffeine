package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.rankmap;

import com.typesafe.config.Config;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/***
 * Records the latency side of a recording: every benefit window the {@link SequenceBenefitEstimator}
 * closes, with the request it closed at, the ranks it opened with and the latency it would have
 * saved. It sits beside the rank recording, which is all the hit-ratio objective needs, so that
 * one recording run serves both objectives and a latency replay needs neither the boards nor the
 * estimator.
 * <p>
 * The file is a header followed by one record per closed window, in the order they closed:
 * <pre>
 *   requestIndex (long), rank per stage, benefit (double)
 * </pre>
 */
public final class BenefitDataWriter {
    final private Path path;
    final private Path temporary;
    final private DataOutputStream out;
    final private int rankBytes;
    private boolean closed = false;

    public BenefitDataWriter(Config config, Path path, int stageCount, int capacity, int flushInterval) {
        this.path = path;
        this.rankBytes = RankDataFormat.rankBytes(capacity);
        this.temporary = path.resolveSibling(path.getFileName() + ".partial");

        try {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }

            this.out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(temporary), 1 << 20));

            out.writeInt(RankDataFormat.BENEFIT_MAGIC);
            out.writeInt(RankDataFormat.VERSION);
            out.writeInt(stageCount);
            out.writeByte(rankBytes);
            out.writeInt(flushInterval);
            out.writeUTF(RankDataFormat.fingerprint(config));
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot record the benefit windows to " + temporary, e);
        }
    }

    public void write(long requestIndex, int[] ranks, double benefit) {
        try {
            out.writeLong(requestIndex);
            for (int rank : ranks) {
                if (rankBytes == 2) {
                    out.writeShort(rank);
                } else {
                    out.writeInt(rank);
                }
            }
            out.writeDouble(benefit);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot record a benefit window", e);
        }
    }

    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        try {
            out.close();
            Files.move(temporary, path, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot finish the benefit recording at " + path, e);
        }
    }
}
