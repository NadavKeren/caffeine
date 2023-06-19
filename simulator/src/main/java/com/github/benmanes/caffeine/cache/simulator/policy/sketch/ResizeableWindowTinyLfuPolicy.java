package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import static com.google.common.base.Preconditions.checkState;

import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.ConsoleColors;
import com.github.benmanes.caffeine.cache.simulator.admission.Admittor;
import com.github.benmanes.caffeine.cache.simulator.admission.UneditableAdmittorProxy;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;


@Policy.PolicySpec(name = "sketch.ResizableWindowTinyLfu")
public class ResizeableWindowTinyLfuPolicy extends WindowTinyLfuPolicy {
    private static int id = 0;
    final private static boolean DEBUG = true;
    final private int quantaSize;
    final private String name;

    public ResizeableWindowTinyLfuPolicy(int mainQuota,
                                         int quantaSize,
                                         int maximumSize,
                                         WindowTinyLfuSettings settings,
                                         String name) {
        super(mainQuota * quantaSize,
              maximumSize,
              settings,
              new WindowTinyLFUStats("sketch.ResizableWindowTinyLfu (LFU: %d)", mainQuota));
        this.quantaSize = quantaSize;
        this.name = name + "@" + id;
        ++id;

        checkState(maxProtected >= 0);
        checkState(maxWindow >= 0);
        checkState(maxProtected > 0 || maxWindow > 0);
        checkState(maximumSize > 0);
    }

    public static ResizeableWindowTinyLfuPolicy ghostCopyOf(ResizeableWindowTinyLfuPolicy other, String name) {
        return new ResizeableWindowTinyLfuPolicy(other, name);
    }

    private ResizeableWindowTinyLfuPolicy(ResizeableWindowTinyLfuPolicy other, String name) {
        super(new UneditableAdmittorProxy(other.admittor),
              other.maximumSize,
              other.maxProtected,
              other.maxProbation,
              other.maxWindow,
              new WindowTinyLFUStats("sketch.ResizableWindowTinyLfu (LFU: %d)", other.maxProtected));

        this.quantaSize = other.quantaSize;
        this.name = name + "@" + id;
        ++id;

        copyList(other.headWindow, super.headWindow, Status.WINDOW);
        copyList(other.headProbation, super.headProbation, Status.PROBATION);
        copyList(other.headProtected, super.headProtected, Status.PROTECTED);


        checkState(maxProtected >= 0);
        checkState(maxWindow >= 0);
        checkState(maxProtected > 0 || maxWindow > 0);
        checkState(maximumSize > 0);
    }

    protected ResizeableWindowTinyLfuPolicy(Admittor admittor,
                                          int maximumSize,
                                          int maxProtected,
                                          int maxProbation,
                                          int maxWindow,
                                          PolicyStats stats,
                                          int quantaSize,
                                          String name) {
        super(admittor, maximumSize, maxProtected, maxProbation, maxWindow, stats);
        this.quantaSize = quantaSize;
        this.name = name + "@" + id;
        ++id;
    }

    private void copyList(Node originSentinel,
                          Node copySentinel,
                          Status listType) {
        Node node = originSentinel.prev;
        int items = 0;
        while (node != originSentinel) {
            Node copy = new Node(node.key, listType);
            copy.appendToTail(copySentinel);
            node = node.prev;
            ++items;
            this.data.put(copy.key, copy);
        }

        switch (listType) {
            case WINDOW:
                sizeWindow = items;
                break;
            case PROTECTED:
                sizeProtected = items;
                break;
            case PROBATION:
                sizeProbation = items;
                break;
            case NONE:
            default:
                throw new IllegalStateException("No such list");
        }

        if (DEBUG) {
            System.out.printf("%s: Copied list %s with %d items%n",
                              ConsoleColors.colorString(name, ConsoleColors.CYAN_BOLD),
                              listType,
                              items);
        }
    }

    public boolean canExtendLFU() {
        return (maxWindow > 0) && (maxProbation + maxProtected < maximumSize);
    }

    public boolean canExtendLRU() {
        return (maxProtected > 0) && (maxProbation + maxWindow < maximumSize);
    }

    public void increaseLFU() {
        final int numOfItemsToMove = Math.min(quantaSize, maxWindow);

        maxWindow -= numOfItemsToMove;
        maxProtected += numOfItemsToMove;

        int itemsMoved = 0;
        for (int i = 0; i < numOfItemsToMove && !this.headWindow.isEmpty(); ++i) {
            Node victimNode = this.headWindow.next;
            victimNode.remove();
            victimNode.status = Status.PROBATION;
            victimNode.appendToTail(headProbation);
            --sizeWindow;
            ++sizeProbation;
            ++itemsMoved;
        }

        if (DEBUG) {
            System.out.printf("%s: %s\tMoved %d items%n",
                              ConsoleColors.colorString(name, ConsoleColors.CYAN_BOLD),
                              ConsoleColors.colorString(
                                      String.format(
                                              "Decreased window to max of %d, current %d, protected max: %d with %d items, probation max: %d with %d items",
                                              maxWindow,
                                              sizeWindow,
                                              maxProtected,
                                              sizeProtected,
                                              maxProbation,
                                              sizeProbation),
                                      ConsoleColors.PURPLE),
                              itemsMoved);
        }

        this.finished();
    }

    public void decreaseLFU() {
        final int numOfItemsToMove = Math.min(quantaSize, maxProtected);

        maxWindow += numOfItemsToMove;
        maxProtected -= numOfItemsToMove;

        int itemsMoved = 0;

        for (int i = 0; i < numOfItemsToMove && !this.headProtected.isEmpty(); ++i, ++itemsMoved) {
            Node protectedVictim = this.headProtected.next;
            protectedVictim.remove();
            protectedVictim.status = Status.PROBATION;
            protectedVictim.appendToTail(this.headProbation);

            Node probationVictim = this.headProbation.next;
            probationVictim.remove();
            probationVictim.status = Status.WINDOW;
            probationVictim.appendToTail(headWindow);
        }

        this.sizeProtected -= itemsMoved;
        this.sizeWindow += itemsMoved;

        for (int i = 0; i < (numOfItemsToMove - itemsMoved) && sizeProbation > maxProbation; ++i) {
            Node probationVictim = this.headProbation.next;
            probationVictim.remove();
            probationVictim.status = Status.WINDOW;
            probationVictim.appendToTail(headWindow);
            --this.sizeProbation;
            ++this.sizeWindow;
        }

        if (DEBUG) {
            System.out.printf(
                    "%s: %s\tMoved %d items%n",
                    ConsoleColors.colorString(name, ConsoleColors.CYAN_BOLD),
                    ConsoleColors.colorString(
                            String.format("Increased window to max of %d, current %d, protected max: %d with %d items, probation max: %d with %d items",
                                          maxWindow,
                                          sizeWindow,
                                          maxProtected,
                                          sizeProtected,
                                          maxProbation,
                                          sizeProbation),
                            ConsoleColors.PURPLE),
                    numOfItemsToMove);
        }

        this.finished();
    }

    public void resetStats() {
        var castedStats = (WindowTinyLFUStats) this.stats();
        castedStats.resetStats();
    }

    public WindowTinyLFUStats getWindowStats() {
        return (WindowTinyLFUStats) this.stats();
    }

    public static class WindowTinyLFUStats extends PolicyStats {
        private int timeframeHitCount;
        private int timeframeMissCount;
        private double timeframeHitPenalty;
        private double timeframeMissPenalty;

        public WindowTinyLFUStats(String format, Object... args) {
            super(format, args);
        }

        public void resetStats() {
            this.timeframeHitCount = 0;
            this.timeframeMissCount = 0;
            this.timeframeHitPenalty = 0;
            this.timeframeMissPenalty = 0;
        }

        @Override
        public void recordHit() {
            super.recordHit();
            ++this.timeframeHitCount;
        }

        @Override
        public void recordMiss() {
            super.recordMiss();
            ++this.timeframeMissCount;
        }

        @Override
        public void recordHitPenalty(double penalty) {
            super.recordHitPenalty(penalty);
            this.timeframeHitPenalty += penalty;
        }

        @Override
        public void recordMissPenalty(double penalty) {
            super.recordMissPenalty(penalty);
            this.timeframeMissPenalty += penalty;
        }

        public double timeframeHitRate() {
            return this.timeframeOperationNumber() > 0 ? (double) this.timeframeHitCount / this.timeframeOperationNumber() : 0;
        }

        public int timeframeOperationNumber() {
            return this.timeframeHitCount + this.timeframeMissCount;
        }

        public double timeframeHitPenalty() {
            return this.timeframeOperationNumber() > 0 ? this.timeframeHitPenalty : Double.MAX_VALUE;
        }

        public double timeframeMissPenalty() {
            return this.timeframeOperationNumber() > 0 ?  this.timeframeMissPenalty : Double.MAX_VALUE;
        }
    }

    protected static class Dummy extends ResizeableWindowTinyLfuPolicy {
        public Dummy() {
            super(null,
                  100,
                  10,
                  80,
                  10,
                  new WindowTinyLFUStats("Dummy"),
                  10,
                  "Dummy");
        }

        @Override
        public String name() {
            return "Dummy cache";
        }

        @Override
        public void record(AccessEvent event) {}

        @Override
        public boolean canExtendLFU() {
            return false;
        }

        @Override
        public boolean canExtendLRU() {
            return false;
        }

        @Override
        public void increaseLFU() { }

        @Override
        public void decreaseLFU() { }

        @Override
        public void resetStats() { }

        @Override
        public WindowTinyLFUStats getWindowStats() {
            return super.getWindowStats();
        }

        @Override
        public PolicyStats stats() {
            return super.stats();
        }

        @Override
        public void record(long key) { }

        @Override
        public void finished() { }
    }
}
