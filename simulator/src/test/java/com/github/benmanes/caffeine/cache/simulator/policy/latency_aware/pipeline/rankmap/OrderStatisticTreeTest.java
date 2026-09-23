package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.rankmap;

import static com.google.common.truth.Truth.assertThat;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.NavigableSet;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

/**
 * Cross-checks the augmented tree against a {@link TreeSet} plus a linear scan, which is the
 * obviously-correct reference for every operation the shadow rankings use.
 */
final class OrderStatisticTreeTest {
    /** Mirrors the (score, key) shape the ranked blocks use, so score ties are exercised. */
    private record Entry(long score, long key) {}

    private static final Comparator<Entry> ORDER =
            Comparator.comparingLong(Entry::score).thenComparingLong(Entry::key);

    @Test
    void matchesReferenceUnderRandomOperations() {
        var random = new Random(0x5eed);
        var tree = new OrderStatisticTree<>(ORDER);
        var reference = new TreeSet<>(ORDER);

        for (int op = 0; op < 20_000; ++op) {
            // A small score range forces heavy tie-breaking on the key.
            var entry = new Entry(random.nextInt(16), random.nextInt(400));

            if (random.nextBoolean()) {
                assertThat(tree.add(entry)).isEqualTo(reference.add(entry));
            } else {
                assertThat(tree.remove(entry)).isEqualTo(reference.remove(entry));
            }

            assertThat(tree.size()).isEqualTo(reference.size());

            if (op % 50 == 0) {
                assertRanksMatch(tree, reference);
            }
        }

        assertRanksMatch(tree, reference);
    }

    @Test
    void selectAndDescendingScanAgree() {
        var tree = new OrderStatisticTree<>(ORDER);
        var reference = new TreeSet<>(ORDER);

        for (int i = 0; i < 500; ++i) {
            var entry = new Entry(i % 7, i);
            tree.add(entry);
            reference.add(entry);
        }

        var descending = new ArrayList<>(reference.descendingSet());

        for (int i = 0; i < descending.size(); ++i) {
            assertThat(tree.select(i)).isEqualTo(descending.get(i));
        }

        var scanned = new ArrayList<Entry>();
        tree.forEachDescending(30, scanned::add);
        assertThat(scanned).isEqualTo(descending.subList(0, 30));

        var everything = new ArrayList<Entry>();
        tree.forEachDescending(Integer.MAX_VALUE, everything::add);
        assertThat(everything).isEqualTo(descending);

        assertThat(tree.min()).isEqualTo(reference.first());
    }

    @Test
    void absentKeyHasNoRank() {
        var tree = new OrderStatisticTree<>(ORDER);
        tree.add(new Entry(5, 1));

        assertThat(tree.rankDescending(new Entry(5, 2))).isEqualTo(0);
        assertThat(tree.rankDescending(new Entry(5, 1))).isEqualTo(1);
        assertThat(tree.remove(new Entry(5, 2))).isFalse();
    }

    private static void assertRanksMatch(OrderStatisticTree<Entry> tree, NavigableSet<Entry> reference) {
        List<Entry> descending = new ArrayList<>(reference.descendingSet());

        for (int i = 0; i < descending.size(); ++i) {
            Entry entry = descending.get(i);
            assertThat(tree.rankDescending(entry)).isEqualTo(i + 1);
            assertThat(tree.select(i)).isEqualTo(entry);
            assertThat(tree.contains(entry)).isTrue();
        }
    }
}
