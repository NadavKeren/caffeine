package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.rankmap;

import static com.google.common.truth.Truth.assertThat;

import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.EntryData;
import com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.LfuBlock;
import com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.LruBlock;
import com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.PipelineBlock;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

import java.util.Locale;
import java.util.Random;

/**
 * A board is only a useful sensor if it holds what the matching block would hold. These tests drive a
 * board and its block through the same request stream and compare what each of them keeps, which is
 * what turns "the board is the block's eviction order" from an assumption into a checked property.
 */
final class RankedBlockTest {
    private static final int QUANTA = 8;
    private static final int QUANTUM_SIZE = 8;
    private static final int CAPACITY = QUANTA * QUANTUM_SIZE;

    /**
     * Built on top of reference.conf rather than by hand, so the board and the block are configured
     * by exactly the same defaults the simulator would give them.
     */
    private static Config config() {
        Config reference = ConfigFactory.parseResources("reference.conf").getConfig("caffeine.simulator");

        String overrides = String.format(Locale.US, """
            maximum-size = %d
            pipeline {
              num-of-blocks = 2
              num-of-quanta = %d
              quantum-size = %d
              blocks {
                0 { type = "LRU", quota = 4 }
                1 { type = "LFU", quota = 4 }
              }
            }
            """, CAPACITY, QUANTA, QUANTUM_SIZE);

        return ConfigFactory.parseString(overrides).withFallback(reference).resolve();
    }

    @Test
    void theRecencyBoardKeepsWhatTheRecencyBlockKeeps() {
        agreesWithBlock(new LruBlock(QUANTA, QUANTUM_SIZE), new RankedLruBlock(CAPACITY));
    }

    @Test
    void theFrequencyBoardKeepsWhatTheFrequencyBlockKeeps() {
        var config = config();
        agreesWithBlock(new LfuBlock(QUANTA, QUANTUM_SIZE, config),
                        new RankedLfuBlock(CAPACITY, QUANTUM_SIZE, config));
    }

    @Test
    void ranksRunFromOneToTheBoardSize() {
        var board = new RankedLruBlock(CAPACITY);

        for (int request = 0; request < 4 * CAPACITY; ++request) {
            board.update(event(request));
        }

        assertThat(board.size()).isEqualTo(CAPACITY);

        long[] snapshot = board.snapshotDescending(CAPACITY);
        assertThat(snapshot).hasLength(CAPACITY);

        for (int position = 0; position < snapshot.length; ++position) {
            assertThat(board.rank(snapshot[position])).isEqualTo(position + 1);
        }

        // Anything the board dropped reports the off-board sentinel.
        assertThat(board.contains(0)).isFalse();
        assertThat(board.rank(0)).isEqualTo(CAPACITY + 1);
    }

    /**
     * Drives both through the same stream, in the order {@code PipelinePolicy} uses: every block does
     * its bookkeeping on every key, then the holder is looked up, then a miss is inserted.
     */
    private static void agreesWithBlock(PipelineBlock block, RankedBlock board) {
        var random = new Random(0xBA5EBA11);

        for (int request = 0; request < 20_000; ++request) {
            // A working set several times the capacity, skewed so that eviction decisions matter.
            long key = (long) (Math.abs(random.nextGaussian()) * (double) CAPACITY);
            var event = event(key);

            block.bookkeeping(key);
            board.bookkeeping(key);

            if (block.getEntry(key) == null) {
                block.insert(new EntryData(event));
            }
            board.update(event);

            assertThat(board.size()).isEqualTo(block.size());

            if (block.size() > 0) {
                long[] snapshot = board.snapshotDescending(board.size());
                long boardVictim = snapshot[snapshot.length - 1];

                assertThat(boardVictim).isEqualTo(block.getVictim().key());
            }
        }
    }

    private static AccessEvent event(long key) {
        return AccessEvent.forKeyPenaltiesAndArrivalTime(key, 0, 100, key);
    }
}
