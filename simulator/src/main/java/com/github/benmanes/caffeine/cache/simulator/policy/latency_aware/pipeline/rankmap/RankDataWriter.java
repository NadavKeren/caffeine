package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.rankmap;

import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.typesafe.config.Config;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.LongConsumer;

/***
 * Maintains the shadow rankings live and records them, so that later runs over the same trace and
 * cache size can replay them instead of rebuilding them.
 */
public final class RankDataWriter implements RankSource {
    final private RankedPipeline boards;
    final private Path path;
    final private Path temporary;
    final private DataOutputStream out;
    final private int snapshotInterval;
    final private int rankBytes;
    final private int stageCount;
    final private int capacity;

    private long requestIndex = 0;
    private long warmAt = -1;
    private boolean closed = false;

    public RankDataWriter(Config config, RankedPipeline boards, Path path, int snapshotInterval) {
        this.boards = boards;
        this.path = path;
        this.snapshotInterval = snapshotInterval;
        this.stageCount = boards.stageCount();
        this.capacity = boards.capacity();
        this.rankBytes = RankDataFormat.rankBytes(capacity);
        // Write beside the target and rename at the end, so an interrupted run never leaves behind a
        // truncated recording that a later run would happily replay.
        this.temporary = path.resolveSibling(path.getFileName() + ".partial");

        try {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }

            this.out = new DataOutputStream(new BufferedOutputStream(
                    Files.newOutputStream(temporary), 1 << 20));

            writeHeader(config);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot record the shadow rankings to " + temporary, e);
        }

        writeSnapshots();
    }

    private void writeHeader(Config config) throws IOException {
        out.writeInt(RankDataFormat.MAGIC);
        out.writeInt(RankDataFormat.VERSION);
        out.writeInt(capacity);
        out.writeInt(stageCount);
        out.writeInt(snapshotInterval);
        out.writeByte(rankBytes);
        out.writeLong(warmAt);
        out.writeUTF(RankDataFormat.fingerprint(config));
    }

    public Path path() {
        return path;
    }

    public int snapshotInterval() {
        return snapshotInterval;
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
        int[] ranks = boards.preAccessRanks(key);

        try {
            for (int rank : ranks) {
                if (rankBytes == 2) {
                    out.writeShort(rank);
                } else {
                    out.writeInt(rank);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot record a rank vector", e);
        }

        return ranks;
    }

    @Override
    public long[] snapshotDescending(int stage, int count) {
        return boards.snapshotDescending(stage, count);
    }

    @Override
    public void update(AccessEvent event) {
        boards.update(event);
        ++requestIndex;

        if (requestIndex % snapshotInterval == 0) {
            writeSnapshots();
        }
    }

    private void writeSnapshots() {
        try {
            for (int stage = 0; stage < stageCount; ++stage) {
                long[] snapshot = boards.snapshotDescending(stage, capacity);
                out.writeInt(snapshot.length);

                for (long key : snapshot) {
                    out.writeLong(key);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot record a board snapshot", e);
        }
    }

    @Override
    public void drainDepartures(LongConsumer consumer) {
        boards.drainDepartures(consumer);
    }

    @Override
    public boolean isWarm() {
        boolean warm = boards.isWarm();

        if (warm && warmAt < 0) {
            warmAt = requestIndex;
        }

        return warm;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        try {
            out.close();

            // The warm-up point is only known once the run has passed it, so patch it in now.
            try (var file = new RandomAccessFile(temporary.toFile(), "rw")) {
                file.seek(RankDataFormat.WARM_AT_OFFSET);
                file.writeLong(warmAt);
            }

            Files.move(temporary, path, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot finish the shadow ranking recording at " + path, e);
        }
    }
}
