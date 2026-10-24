package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.rankmap;

import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.AdaptationObjective;
import com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.PipelinePolicy;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.util.function.LongConsumer;

import javax.annotation.Nullable;

/***
 * The shadow rankings of a pipeline: one board per stage, each run at the full cache capacity.
 * <p>
 * A top-capacity board per stage is always enough, whatever the stage count or the allocation
 * quantum: a stage's reach depth is bounded by the total capacity, since at most the slots of all
 * the stages ahead of it can be claimed before it and it needs only its own quota after that.
 * <p>
 * Nothing here depends on how the cache is currently split between the stages. The boards are a
 * function of the request stream and the cache size alone, which is what makes them worth recording
 * once per trace and replaying across a configuration sweep.
 */
public final class RankedPipeline implements RankSource {
    final private RankedBlock[] boards;
    final private int capacity;
    final private LongOpenHashSet departed = new LongOpenHashSet();
    @Nullable final private BoardFetchStage[] fetching;

    public RankedPipeline(Config config) {
        var settings = new PipelinePolicy.PipelineSettings(config);

        this.capacity = settings.numOfQuanta() * settings.quantumSize();
        final var blockConfigs = settings.blocksConfigs();
        this.boards = new RankedBlock[settings.numOfBlocks()];

        for (int idx = 0; idx < boards.length; ++idx) {
            var blockSettings = new PipelinePolicy.PipelineBlockSettings(blockConfigs.get(idx));
            boards[idx] = createBoard(blockSettings.type(), config);
            boards[idx].onDeparture(departed::add);
        }

        this.fetching = insertsOnFetch(config) ? new BoardFetchStage[boards.length] : null;
        if (fetching != null) {
            for (int idx = 0; idx < boards.length; ++idx) {
                fetching[idx] = new BoardFetchStage();
            }
        }
    }

    /***
     * Whether the boards insert a missed key when its fetch completes, as the pipeline does, rather than
     * at the miss. The latency objective needs it: there the fetch is what a request pays for, and a
     * board that skips it credits hits during the fetch and times recency from the wrong moment.
     * The hit-ratio boards keep inserting at the miss.
     */
    public static boolean insertsOnFetch(Config config) {
        final String path = "rank-map.objective";
        return config.hasPath(path) && AdaptationObjective.parse(config.getString(path)) == AdaptationObjective.LATENCY;
    }

    /*** Whether some board is still fetching the key. */
    public boolean fetches(long key) {
        return isFetching(fetching, key);
    }

    /*** Whether a board is still fetching the key, which counts as being on it. */
    public static boolean isFetching(@Nullable BoardFetchStage[] fetching, long key) {
        if (fetching != null) {
            for (BoardFetchStage stage : fetching) {
                if (stage.contains(key)) {
                    return true;
                }
            }
        }

        return false;
    }

    /***
     * The LFU board's probation segment. LfuBlock keeps exactly one quantum on probation, which would
     * make the LFU board - and every rank recorded from it - a function of the quantum, so a recording
     * could only serve the resolution it was made at. {@code rank-map.lfu-board-probation-size} pins it
     * in items so that a sweep over quanta shares one board; 0 falls back to one quantum of this run.
     * It is part of a recording's fingerprint either way.
     */
    public static int lfuProbationSize(Config config) {
        final String path = "rank-map.lfu-board-probation-size";
        final int pinned = config.hasPath(path) ? config.getInt(path) : 0;

        return pinned > 0 ? pinned : new PipelinePolicy.PipelineSettings(config).quantumSize();
    }

    private RankedBlock createBoard(String type, Config config) {
        return switch (type) {
            case "LRU" -> new RankedLruBlock(capacity);
            case "LFU" -> new RankedLfuBlock(capacity, lfuProbationSize(config), config);
            case "LBU" -> new RankedLbuBlock(capacity, config);
            // CRA's score depends on the time since the last access, so two keys swap places without
            // either being touched and no fixed ordering can hold it. Recency is the approximation.
            case "LA-LRU" -> new RankedLruBlock(capacity);
            case "LA-LFU" -> new RankedLaLfuBlock(capacity, config);
            default -> throw new IllegalStateException("No shadow ranking for block type: " + type);
        };
    }

    @Override
    public int stageCount() {
        return boards.length;
    }

    @Override
    public int capacity() {
        return capacity;
    }

    public String type(int stage) {
        return boards[stage].type();
    }

    /***
     * The pre-access rank of the key in every board. This must be read before anything else touches
     * the boards or the cache: an access typically moves the key to the best rank, which destroys the
     * counterfactual the projection is built on.
     */
    @Override
    public int[] preAccessRanks(long key) {
        int[] ranks = new int[boards.length];

        for (int idx = 0; idx < boards.length; ++idx) {
            ranks[idx] = boards[idx].rank(key);
        }

        return ranks;
    }

    @Override
    public long[] snapshotDescending(int stage, int count) {
        return boards[stage].snapshotDescending(count);
    }

    @Override
    public void advanceTo(double time) {
        if (fetching == null) {
            return;
        }

        for (int idx = 0; idx < boards.length; ++idx) {
            fetching[idx].completeBefore(time, boards[idx]::insert);
        }
    }

    @Override
    public void update(AccessEvent event) {
        final long key = event.key();

        for (int idx = 0; idx < boards.length; ++idx) {
            final RankedBlock board = boards[idx];

            // Every board performs its bookkeeping on every request, the way the pipeline lets every
            // block see every key regardless of which one holds it.
            board.bookkeeping(key);

            if (fetching == null) {
                board.update(event);
                continue;
            }

            // The pipeline's order: a key being fetched is a delayed hit, a held key a hit, and anything
            // else a miss that starts a fetch and joins the board only when the fetch completes.
            if (fetching[idx].contains(key)) {
                board.onDelayedHit(event, fetching[idx].availabilityTime(key));
            } else if (board.contains(key)) {
                board.onHit(event);
            } else {
                board.onMiss(event);
                fetching[idx].add(key, event.getAvailabilityTime());
            }

            board.afterRequest();
        }
    }

    /***
     * Whether every board has filled up, which happens exactly once the trace has requested as many
     * distinct keys as the cache can hold. Projections before that are meaningless.
     */
    @Override
    public boolean isWarm() {
        for (RankedBlock board : boards) {
            if (board.size() < capacity) {
                return false;
            }
        }

        return true;
    }

    @Override
    public void drainDepartures(LongConsumer consumer) {
        for (LongIterator iterator = departed.iterator(); iterator.hasNext(); ) {
            final long key = iterator.nextLong();

            if (!onAnyBoard(key) && !isFetching(fetching, key)) {
                consumer.accept(key);
            }
        }

        departed.clear();
    }

    private boolean onAnyBoard(long key) {
        for (RankedBlock board : boards) {
            if (board.contains(key)) {
                return true;
            }
        }

        return false;
    }

    public void validate() {
        for (RankedBlock board : boards) {
            Assert.assertCondition(board.capacity() == capacity,
                                   () -> String.format("Board %s has capacity %d instead of %d",
                                                       board.type(),
                                                       board.capacity(),
                                                       capacity));
        }
    }
}
