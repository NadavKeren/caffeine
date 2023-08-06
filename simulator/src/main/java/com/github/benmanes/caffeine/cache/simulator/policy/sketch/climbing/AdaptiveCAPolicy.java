/*
 * Copyright 2020 Omri Himelbrand. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.BucketLatencyEstimation;
import com.github.benmanes.caffeine.cache.simulator.admission.Admittor;
import com.github.benmanes.caffeine.cache.simulator.admission.LATinyLfu;
import com.github.benmanes.caffeine.cache.simulator.policy.*;
import com.github.benmanes.caffeine.cache.simulator.policy.linked.CraBlock;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.LatestLatencyEstimator;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.TrueAverageEstimator;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing.LAHillClimber.QueueType;
import com.typesafe.config.Config;
import org.checkerframework.checker.nullness.qual.Nullable;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing.LAHillClimber.AdaptationType;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Locale.US;
import static java.util.stream.Collectors.toSet;

/**
 * The WindowLA algorithm where the size of the admission window is adjusted using the latency
 * aware hill climbing algorithm.
 *
 * @author himelbrand@gmail.com (Omri Himelbrand)
 */
@SuppressWarnings("PMD.TooManyFields")
@Policy.PolicySpec(name = "sketch.ACA")
public final class AdaptiveCAPolicy implements Policy {

  static final boolean DEBUG = false;
  static final boolean DUMP = true;
  private final double initialPercentMain;
  private final PolicyStats policyStats;
  private final LAHillClimber climber;
  private final LatencyEstimator<Long> latencyEstimator;
  private final Admittor admittor;
  private final int cacheCapacity;

  private final CraBlock probationBlock;
  private final CraBlock protectedBlock;
  private final CraBlock windowBlock;

  private final List<DumpEntry> dumpInfo;

  private int windowCapacity;
  private int protectedCapacity;

  private double windowSize;
  private double protectedSize;


  private double normalizationBias;
  private double normalizationFactor;
  private double maxDelta;
  private int maxDeltaCounts;
  private int samplesCount;

  private double timeframePenalty;
  private int timeframeHitCount;
  private int timeframeOpCount;



  public AdaptiveCAPolicy(
          LAHillClimberType strategy, double percentMain, AdaptiveCASettings settings,
          double decayFactor, int maxLists) {
    this.latencyEstimator = createEstimator(settings.config());
    this.cacheCapacity = (int) settings.maximumSize();
    int mainCacheCapacity = (int) (cacheCapacity * percentMain);
    this.protectedCapacity = (int) (mainCacheCapacity * settings.percentMainProtected());
    this.windowCapacity = cacheCapacity - mainCacheCapacity;
    this.protectedBlock = new CraBlock(decayFactor, maxLists, this.protectedCapacity, latencyEstimator, "protected-block");
    this.probationBlock = new CraBlock(decayFactor, maxLists, mainCacheCapacity - this.protectedCapacity, latencyEstimator, "probation-block");
    this.windowBlock = new CraBlock(decayFactor, maxLists, this.windowCapacity, latencyEstimator, "window-block");
    this.dumpInfo = new ArrayList<>();
    this.dumpInfo.add(new DumpEntry(0, 1 - (double) this.windowCapacity / this.cacheCapacity, 1, 0));
    this.initialPercentMain = percentMain;
    this.policyStats = new PolicyStats("CAHillClimberWindow (%s)(k=%.2f,maxLists=%d)",
                                       strategy.name().toLowerCase(US), decayFactor, maxLists);
    this.admittor = new LATinyLfu(settings.config(), policyStats, latencyEstimator);
    this.climber = strategy.create(settings.config());
    this.normalizationBias = 0;
    this.normalizationFactor = 0;
    this.maxDelta = 0;
    this.maxDeltaCounts = 0;
    this.samplesCount = 0;

    resetTimeFrameCounters();

    printSegmentSizes();
  }

  private void resetTimeFrameCounters() {
    this.timeframePenalty = 0;
    this.timeframeHitCount = 0;
    this.timeframeOpCount = 0;
  }

  private LatencyEstimator<Long> createEstimator(Config config) {
    BasicSettings settings = new BasicSettings(config);
    BasicSettings.LatencyEstimationSettings latencySettings = settings.latencyEstimationSettings();
    String estimationType = latencySettings.estimationType();

    LatencyEstimator<Long> estimator;
    switch (estimationType) {
      case "latest":
        estimator = new LatestLatencyEstimator<>();
        break;
      case "true-average":
        estimator = new TrueAverageEstimator<>();
        break;
      case "buckets":
        estimator = new BucketLatencyEstimation<>(latencySettings.numOfBuckets(), latencySettings.epsilon());
        break;
      default:
        throw new IllegalStateException("Unknown estimator type: " + estimationType);
    }



    return estimator;
  }

  /**
   * Returns all variations of this policy based on the configuration parameters.
   */
  public static Set<Policy> policies(Config config) {
    AdaptiveCASettings settings = new AdaptiveCASettings(config);
    Set<Policy> policies = new HashSet<>();
    int decayFactor = settings.decayFactor();
    int maxLists = settings.maxLists();
    for (LAHillClimberType climber : settings.strategy()) {
      for (double percentMain : settings.percentMain()) {
              policies
                  .add(new AdaptiveCAPolicy(climber, percentMain, settings, decayFactor, maxLists));
      }
    }
    return policies;
  }

  @Override
  public PolicyStats stats() {
    return policyStats;
  }

  @Override
  public void record(AccessEvent event) {
    final long key = event.key();
    policyStats.recordOperation();
    EntryData entry = null;
    admittor.record(event);

    QueueType queue = null;
    if (windowBlock.isHit(key)) {
      entry = windowBlock.get(key);
      onWindowHit(entry);
      queue = QueueType.WINDOW;
    } else if (probationBlock.isHit(key)) {
      entry = probationBlock.get(key);
      onProbationHit(entry);
      queue = QueueType.PROBATION;
    } else if (protectedBlock.isHit(key)) {
      entry = protectedBlock.get(key);
      onProtectedHit(entry);
      queue = QueueType.PROTECTED;
    } else {
      updateNormalization(event.key());
      onMiss(event);
      latencyEstimator.record(event.key(), event.missPenalty(), event.getArrivalTime());
      policyStats.recordMiss();
      policyStats.recordMissPenalty(event.missPenalty());
    }

    if (entry != null) {
       recordAccordingToAvailability(entry, event);
    }

    ++timeframeOpCount;

    final boolean isFull = (size() >= cacheCapacity);
    climb(event, queue, isFull);
  }

  private void recordAccordingToAvailability(EntryData entry, AccessEvent currEvent) {
    boolean isAvailable = entry.event().isAvailableAt(currEvent.getArrivalTime());
    if (isAvailable) {
      currEvent.changeEventStatus(AccessEvent.EventStatus.HIT);
      policyStats.recordHit();
      policyStats.recordHitPenalty(currEvent.hitPenalty());

      latencyEstimator.recordHit(currEvent.hitPenalty());

      timeframePenalty += currEvent.hitPenalty();
      ++timeframeHitCount;
    } else {
      currEvent.changeEventStatus(AccessEvent.EventStatus.DELAYED_HIT);
      currEvent.setDelayedHitPenalty(entry.event().getAvailabilityTime());
      policyStats.recordDelayedHitPenalty(currEvent.delayedHitPenalty());
      policyStats.recordDelayedHit();
      latencyEstimator.addValueToRecord(currEvent.key(), currEvent.delayedHitPenalty(), currEvent.getArrivalTime());

      timeframePenalty += currEvent.delayedHitPenalty();
    }
  }

  private void updateNormalization(long key) {
    double delta = latencyEstimator.getDelta(key);

    if (delta > normalizationFactor){
      ++samplesCount;
      ++maxDeltaCounts;

      maxDelta = (maxDelta * maxDeltaCounts + delta) / maxDeltaCounts;
    }

    normalizationBias = normalizationBias > 0
                      ? Math.min(normalizationBias, Math.max(0, delta))
                      : Math.max(0, delta);

    if (samplesCount % 1000 == 0 || normalizationFactor == 0){
      normalizationFactor = maxDelta;
      maxDeltaCounts = 1;
      samplesCount = 0;
    }

    protectedBlock.setNormalization(normalizationBias, normalizationFactor);
    probationBlock.setNormalization(normalizationBias, normalizationFactor);
    windowBlock.setNormalization(normalizationBias, normalizationFactor);
  }

  /**
   * Adds the entry to the admission window, evicting if necessary.
   */
  private void onMiss(AccessEvent event) {
    event.changeEventStatus(AccessEvent.EventStatus.MISS);
    timeframePenalty += event.missPenalty();
    windowBlock.addEntry(event);
    windowSize++;
    evict();
  }

  /**
   * Moves the entry to the MRU position in the admission window.
   */
  private void onWindowHit(EntryData entryData) {
    windowBlock.moveToTail(entryData);
  }

  /**
   * Promotes the entry to the protected region's MRU position, demoting an entry if necessary.
   */
  private void onProbationHit(EntryData entry) {
    probationBlock.remove(entry.key());
    protectedBlock.addEntry(entry);
    protectedSize++;
    demoteProtected();
  }

  private void demoteProtected() {
    if (protectedSize > protectedCapacity) {
      EntryData demote = protectedBlock.removeVictim();
      probationBlock.addEntry(demote);
      protectedSize--;
    }
  }

  /**
   * Moves the entry to the MRU position, if it falls outside the fast-path threshold.
   */
  private void onProtectedHit(EntryData entry) {
    protectedBlock.moveToTail(entry);
  }

  private int size() { return windowBlock.size() + protectedBlock.size() + probationBlock.size(); }

  /**
   * Evicts from the admission window into the probation space. If the size exceeds the maximum,
   * then the admission candidate and probation's victim are evaluated and one is evicted.
   */
  private void evict() {
    if (windowSize <= windowCapacity) {
      return;
    }

    EntryData candidate = windowBlock.removeVictim();
    windowSize--;

    probationBlock.addEntry(candidate);
    if (size() > cacheCapacity) {
      EntryData probationBlockVictim = probationBlock.findVictim();
      EntryData evict = admittor.admit(candidate.event(), probationBlockVictim.event())
                        ? probationBlockVictim
                        : candidate;

      probationBlock.remove(evict.key());
      policyStats.recordEviction();
    }
  }

  /**
   * Performs the hill climbing process.
   */
  private void climb(AccessEvent event, @Nullable QueueType queue, boolean isFull) {
    if (queue == null) {
      climber.onMiss(event, isFull);
    } else {
      climber.onHit(event, queue, isFull);
    }

    double probationSize = cacheCapacity - windowSize - protectedSize;
    LAHillClimber.Adaptation adaptation = climber
        .adapt(windowSize, probationSize, protectedSize, isFull);
    if (adaptation.type == AdaptationType.INCREASE_WINDOW) {
      increaseWindow(adaptation.amount);
    } else if (adaptation.type == AdaptationType.DECREASE_WINDOW) {
      decreaseWindow(adaptation.amount);
    }

    if (adaptation.type != AdaptationType.HOLD) {
      DumpEntry dumpEntry = new DumpEntry(event.eventNum(),
                                          1 - (double) this.windowCapacity / this.cacheCapacity,
                                          (double) timeframeHitCount / timeframeOpCount,
                                          timeframePenalty / timeframeOpCount);
      resetTimeFrameCounters();
      this.dumpInfo.add(dumpEntry);
    }
  }

  private void increaseWindow(double amount) {
    checkState(amount >= 0.0);
    if (protectedCapacity == 0) {
      return;
    }

    double increaseAmount = Math.min(amount, (double) protectedCapacity);
    int numOfItemsToMove = (int) (windowSize + increaseAmount) - (int) windowSize;
    windowSize += increaseAmount;

    for (int i = 0; i < numOfItemsToMove; i++) {
      ++windowCapacity;
      --protectedCapacity;

      demoteProtected();
      EntryData candidate = probationBlock.removeVictim();
      windowBlock.addEntry(candidate);
    }
    checkState(windowSize >= 0);
    checkState(windowCapacity >= 0);
    checkState(protectedCapacity >= 0);

    if (DEBUG) {
      System.out.printf("+%,d (%,d -> %,d)%n", numOfItemsToMove, windowCapacity - numOfItemsToMove, windowCapacity);
    }
  }

  private void decreaseWindow(double amount) {
    checkState(amount >= 0.0);
    if (windowCapacity == 0) {
      return;
    }

    double quota = Math.min(amount, windowSize);
    int steps = Math.min((int) windowSize - (int) (windowSize - quota), windowCapacity);
    windowSize -= quota;

    for (int i = 0; i < steps; i++) {
      windowCapacity--;
      protectedCapacity++;
      EntryData candidate = windowBlock.removeVictim();
      probationBlock.addEntry(candidate);
    }
    checkState(windowSize >= 0);
    checkState(windowCapacity >= 0);
    checkState(protectedCapacity >= 0);

    if (DEBUG) {
      System.out.printf("-%,d (%,d -> %,d)%n", steps, windowCapacity + steps, windowCapacity);
    }
  }

  private void printSegmentSizes() {
    if (DEBUG) {
      System.out.printf(
              "maxWindow=%d, maxProtected=%d, percentWindow=%.1f",
              windowCapacity, protectedCapacity, (100.0 * windowCapacity) / cacheCapacity);
    }
  }

  private PrintWriter prepareFileWriter() {
    LocalDateTime currentTime = LocalDateTime.now(ZoneId.systemDefault());
    DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("dd-MM-HH-mm-ss");
    PrintWriter writer = null;
    try {
      FileWriter fwriter = new FileWriter("/tmp/adaptive-CA-adaptions-" + currentTime.format(timeFormatter) + ".dump", StandardCharsets.UTF_8);
      writer = new PrintWriter(fwriter);
    } catch (IOException e) {
      System.err.println("Error creating the log file handler");
      e.printStackTrace();
      System.exit(1);
    }

    return writer;
  }

  @Override
  public void dump() {
    if (DUMP) {
      PrintWriter writer = prepareFileWriter();

      for (var dumpEntry : dumpInfo) {
        writer.println(dumpEntry);
      }

      writer.close();
    }
  }

  @Override
  public void finished() {
    policyStats.setPercentAdaption(
            (windowCapacity / (double) cacheCapacity) - (1.0 - initialPercentMain));
    printSegmentSizes();

    checkState(Math.abs(windowSize - windowBlock.size()) < 2,
               "Window: %s != %s",
               (long) windowSize,
               windowBlock.size());

    checkState(Math.abs(protectedSize - protectedBlock.size()) < 2,
               "Protected: %s != %s",
               (long) protectedSize,
               protectedBlock.size());

    checkState(size() <= cacheCapacity, "Maximum: %s > %s", size(), cacheCapacity);
  }

  private static final class AdaptiveCASettings extends BasicSettings {

    public AdaptiveCASettings(Config config) {
      super(config);
    }

    public List<Double> percentMain() {
      return config().getDoubleList("ca-hill-climber-window.percent-main");
    }

    public double percentMainProtected() {
      return config().getDouble("ca-hill-climber-window.percent-main-protected");
    }

    public Set<LAHillClimberType> strategy() {
      return config().getStringList("ca-hill-climber-window.strategy").stream()
          .map(strategy -> strategy.replace('-', '_').toUpperCase(US))
          .map(LAHillClimberType::valueOf)
          .collect(toSet());
    }

    public int decayFactor() {
      return config().getInt("ca-hill-climber-window.cra.decay-factor");
    }

    public int maxLists() {
      return config().getInt("ca-hill-climber-window.cra.max-lists");
    }

  }
  
  private static final class DumpEntry {
    private int eventNum;
    private double mainPercentage;
    private double hitRate;
    private double avgPenalty;

    private DumpEntry(int eventNum, double mainPercentage, double hitRate, double avgPenalty) {
      this.eventNum = eventNum;
      this.mainPercentage = mainPercentage;
      this.hitRate = hitRate;
      this.avgPenalty = avgPenalty;
    }

    @Override
    public String toString() {
      return String.format("%d,%.1f,%.1f,%.1f,%.1f",
                           eventNum,
                           (1 - mainPercentage) * 100,
                           mainPercentage * 100,
                           100 * hitRate,
                           avgPenalty);
    }
  }
}
