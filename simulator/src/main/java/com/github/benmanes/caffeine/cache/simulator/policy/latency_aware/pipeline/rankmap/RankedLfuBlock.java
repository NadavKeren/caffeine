package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.rankmap;

import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.admission.TinyLfu;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

/***
 * The frequency board: {@code LfuBlock}'s discipline at the full cache capacity - its own TinyLFU
 * sketch deciding admission, and the same segmented probation / protected structure.
 * <p>
 * The rank is the position in that block's eviction order. Eviction always takes the probation end
 * first, so every protected entry ranks ahead of every probation entry.
 * <p>
 * Note what is deliberately not done here: the board does not rank on the sketch value itself. The
 * four-bit count-min estimate has sixteen levels, so a capacity-sized board would be almost entirely
 * ties, and its periodic reset and hash collisions move other keys' estimates without those keys
 * being touched - which would silently corrupt an ordering structure keyed on them.
 */
public final class RankedLfuBlock implements RankedBlock {
    final private int capacity;
    final private int protectedCapacity;
    final private TinyLfu admittor;

    final private OrderStatisticTree<ScoredKey> probation = new OrderStatisticTree<>(ScoredKey.ORDER);
    final private OrderStatisticTree<ScoredKey> protectedSegment = new OrderStatisticTree<>(ScoredKey.ORDER);
    final private Long2DoubleMap probationScores;
    final private Long2DoubleMap protectedScores;

    private long opCounter = 0;

    /***
     * {@code probationSize} is the probation segment LfuBlock would keep: one quantum. It is a
     * parameter rather than the run's quantum so that one board can serve a sweep over quanta; see
     * {@link RankedPipeline#lfuProbationSize}.
     */
    public RankedLfuBlock(int capacity, int probationSize, Config config) {
        this.capacity = capacity;
        // The same split LfuBlock uses: everything but the probation segment is protected, unless
        // that would leave nothing protected.
        this.protectedCapacity = probationSize > 0 && probationSize < capacity ? capacity - probationSize : capacity;

        this.admittor = new TinyLfu(config, new PolicyStats("fake"));

        this.probationScores = new Long2DoubleOpenHashMap(capacity * 2);
        this.probationScores.defaultReturnValue(Double.NaN);
        this.protectedScores = new Long2DoubleOpenHashMap(capacity * 2);
        this.protectedScores.defaultReturnValue(Double.NaN);
    }

    @Override
    public String type() {
        return "LFU";
    }

    @Override
    public boolean contains(long key) {
        return !Double.isNaN(protectedScores.get(key)) || !Double.isNaN(probationScores.get(key));
    }

    @Override
    public int rank(long key) {
        double protectedScore = protectedScores.get(key);
        if (!Double.isNaN(protectedScore)) {
            return protectedSegment.rankDescending(new ScoredKey(protectedScore, key));
        }

        double probationScore = probationScores.get(key);
        if (!Double.isNaN(probationScore)) {
            return protectedSegment.size() + probation.rankDescending(new ScoredKey(probationScore, key));
        }

        return capacity + 1;
    }

    @Override
    public long[] snapshotDescending(int count) {
        final int wanted = Math.min(count, size());
        long[] snapshot = new long[wanted];
        int[] filled = {0};

        protectedSegment.forEachDescending(wanted, entry -> snapshot[filled[0]++] = entry.key());
        probation.forEachDescending(wanted - filled[0], entry -> snapshot[filled[0]++] = entry.key());

        return snapshot;
    }

    @Override
    public void bookkeeping(long key) {
        admittor.record(key);
    }

    @Override
    public void update(AccessEvent event) {
        final long key = event.key();

        if (!Double.isNaN(protectedScores.get(key))) {
            refreshProtected(key);
        } else if (!Double.isNaN(probationScores.get(key))) {
            promoteToProtected(key);
        } else {
            admit(key);
        }

        validate();
    }

    private void refreshProtected(long key) {
        protectedSegment.remove(new ScoredKey(protectedScores.get(key), key));
        addToProtected(key);
    }

    private void promoteToProtected(long key) {
        probation.remove(new ScoredKey(probationScores.get(key), key));
        probationScores.remove(key);
        addToProtected(key);

        if (protectedSegment.size() > protectedCapacity) {
            ScoredKey demotee = protectedSegment.min();
            protectedSegment.remove(demotee);
            protectedScores.remove(demotee.key());
            addToProbation(demotee.key());
        }
    }

    private void admit(long key) {
        if (capacity == 0) {
            return;
        }

        if (size() >= capacity) {
            ScoredKey victim = probation.size() > 0 ? probation.min() : protectedSegment.min();

            if (!admittor.admit(key, victim.key())) {
                return;
            }

            if (!Double.isNaN(probationScores.get(victim.key()))) {
                probation.remove(victim);
                probationScores.remove(victim.key());
            } else {
                protectedSegment.remove(victim);
                protectedScores.remove(victim.key());
            }
        }

        addToProbation(key);
    }

    private void addToProbation(long key) {
        final double score = ++opCounter;
        probationScores.put(key, score);
        probation.add(new ScoredKey(score, key));
    }

    private void addToProtected(long key) {
        final double score = ++opCounter;
        protectedScores.put(key, score);
        protectedSegment.add(new ScoredKey(score, key));
    }

    private void validate() {
        Assert.assertCondition(protectedSegment.size() <= protectedCapacity, "LFU board: protected overflow");
        Assert.assertCondition(size() <= capacity, "LFU board: capacity overflow");
        Assert.assertCondition(probation.size() == probationScores.size()
                               && protectedSegment.size() == protectedScores.size(),
                               "LFU board: order and score maps diverged");
    }

    @Override
    public int size() {
        return probation.size() + protectedSegment.size();
    }

    @Override
    public int capacity() {
        return capacity;
    }
}
