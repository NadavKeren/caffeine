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
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing.LAHillClimber.AdaptationType;

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

  private final double initialPercentMain;
  private final Long2ObjectMap<EntryData> data;
  private final PolicyStats policyStats;
  private final LAHillClimber climber;
  private final LatencyEstimator<Long> latencyEstimator;
  private final Admittor admittor;
  private final long maximumSize;

  private final CraBlock probationBlock;
  private final CraBlock protectedBlock;
  private final CraBlock windowBlock;

  private long maxWindow;
  private long maxProtected;

  private double windowSize;
  private double protectedSize;

  static final boolean debug = false;
  static final boolean trace = false;
  double k;

  private double normalizationBias;
  private double normalizationFactor;
  private double maxDelta;
  private int maxDeltaCounts;
  private int samplesCount;


  public AdaptiveCAPolicy(
      LAHillClimberType strategy, double percentMain, AdaptiveCASettings settings,
      double k, int maxLists) {
    this.latencyEstimator = createEstimator(settings.config());
    int maxMain = (int) (settings.maximumSize() * percentMain);
    this.maxProtected = (int) (maxMain * settings.percentMainProtected());
    this.maxWindow = settings.maximumSize() - maxMain;
    this.data = new Long2ObjectOpenHashMap<>();
    this.maximumSize = settings.maximumSize();
    this.protectedBlock = new CraBlock(k, maxLists, this.maxProtected, latencyEstimator);
    this.probationBlock = new CraBlock(k, maxLists, maxMain - this.maxProtected, latencyEstimator);
    this.windowBlock = new CraBlock(k, maxLists, this.maxWindow, latencyEstimator);
    this.initialPercentMain = percentMain;
    this.policyStats = new PolicyStats("CAHillClimberWindow (%s)(k=%.2f,maxLists=%d)",
            strategy.name().toLowerCase(US), k, maxLists);
    this.admittor = new LATinyLfu(settings.config(), policyStats, latencyEstimator);
    this.climber = strategy.create(settings.config());
    this.k = k;
    this.normalizationBias = 0;
    this.normalizationFactor = 0;
    this.maxDelta = 0;
    this.maxDeltaCounts = 0;
    this.samplesCount = 0;

    printSegmentSizes();
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
    for (LAHillClimberType climber : settings.strategy()) {
      for (double percentMain : settings.percentMain()) {
        for (double k : settings.kValues()) {
          for (int ml : settings.maxLists()) {
              policies
                  .add(new AdaptiveCAPolicy(climber, percentMain, settings, k, ml));
          }
        }
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
    long key = event.key();
    boolean isFull = (data.size() >= maximumSize);
    policyStats.recordOperation();
    EntryData entry = data.get(key);
    admittor.record(event);

    QueueType queue = null;
    if (entry == null) {
      updateNormalization(event.key());
      onMiss(event);
      policyStats.recordMiss();
      policyStats.recordMissPenalty(event.missPenalty());
    } else {
      if (entry.event().isAvailableAt(event.getArrivalTime())) {
        entry.event().updateHitPenalty(event.hitPenalty());
        event.changeEventStatus(AccessEvent.EventStatus.HIT);
        policyStats.recordHit();
        policyStats.recordHitPenalty(event.hitPenalty());
      } else {
        event.changeEventStatus(AccessEvent.EventStatus.DELAYED_HIT);
        event.setDelayedHitPenalty(entry.event().getAvailabilityTime());
        policyStats.recordDelayedHit();
        policyStats.recordDelayedHitPenalty(event.delayedHitPenalty());
      }

      if (windowBlock.isHit(key)) {
        onWindowHit(entry);
        queue = QueueType.WINDOW;
      } else if (probationBlock.isHit(key)) {
        onProbationHit(entry);
        queue = QueueType.PROBATION;
      } else if (protectedBlock.isHit(key)) {
        onProtectedHit(entry);
        queue = QueueType.PROTECTED;
      } else {
        throw new IllegalStateException();
      }
    }

    climb(event, queue, isFull);
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
    long key = event.key();
    EntryData entry = windowBlock.addEntry(event);
    data.put(key, entry);
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
    if (protectedSize > maxProtected) {
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

  /**
   * Evicts from the admission window into the probation space. If the size exceeds the maximum,
   * then the admission candidate and probation's victim are evaluated and one is evicted.
   */
  private void evict() {
    if (windowSize <= maxWindow) {
      return;
    }

    EntryData candidate = windowBlock.removeVictim();
    windowSize--;

    probationBlock.addEntry(candidate);
    if (data.size() > maximumSize) {
      EntryData probationBlockVictim = probationBlock.findVictim();
      EntryData evict = admittor.admit(candidate.event(), probationBlockVictim.event())
                        ? probationBlockVictim
                        : candidate;

      data.remove(evict.key());
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

    double probationSize = maximumSize - windowSize - protectedSize;
    LAHillClimber.Adaptation adaptation = climber
        .adapt(windowSize, probationSize, protectedSize, isFull);
    if (adaptation.type == AdaptationType.INCREASE_WINDOW) {
      increaseWindow(adaptation.amount);
    } else if (adaptation.type == AdaptationType.DECREASE_WINDOW) {
      decreaseWindow(adaptation.amount);
    }
  }

  private void increaseWindow(double amount) {
    checkState(amount >= 0.0);
    if (maxProtected == 0) {
      return;
    }

    double quota = Math.min(amount, (double)maxProtected);
    int steps = (int) (windowSize + quota) - (int) windowSize;
    windowSize += quota;

    for (int i = 0; i < steps; i++) {
      maxWindow++;
      maxProtected--;

      demoteProtected();
      EntryData candidate = probationBlock.removeVictim();
      windowBlock.addEntry(candidate);
    }
    checkState(windowSize >= 0);
    checkState(maxWindow >= 0);
    checkState(maxProtected >= 0);

    if (trace) {
      System.out.printf("+%,d (%,d -> %,d)%n", steps, maxWindow - steps, maxWindow);
    }
  }

  private void decreaseWindow(double amount) {
    checkState(amount >= 0.0);
    if (maxWindow == 0) {
      return;
    }

    double quota = Math.min(amount, windowSize);
    int steps = (int) Math.min((int) windowSize - (int) (windowSize - quota), maxWindow);
    windowSize -= quota;

    for (int i = 0; i < steps; i++) {
      maxWindow--;
      maxProtected++;
      EntryData candidate = windowBlock.removeVictim();
      probationBlock.addEntry(candidate);
    }
    checkState(windowSize >= 0);
    checkState(maxWindow >= 0);
    checkState(maxProtected >= 0);

    if (trace) {
      System.out.printf("-%,d (%,d -> %,d)%n", steps, maxWindow + steps, maxWindow);
    }
  }

  private void printSegmentSizes() {
    if (debug) {
      System.out.printf(
          "maxWindow=%d, maxProtected=%d, percentWindow=%.1f",
          maxWindow, maxProtected, (100.0 * maxWindow) / maximumSize);
    }
  }

  @Override
  public void finished() {
    policyStats.setPercentAdaption(
            (maxWindow / (double) maximumSize) - (1.0 - initialPercentMain));
    printSegmentSizes();

    long actualWindowSize = data.values().stream().filter(n -> windowBlock.isHit(n.key())).count();
    long actualProbationSize = data.values().stream().filter(n -> probationBlock.isHit(n.key()))
        .count();
    long actualProtectedSize = data.values().stream().filter(n -> protectedBlock.isHit(n.key()))
        .count();
    long calculatedProbationSize = data.size() - actualWindowSize - actualProtectedSize;

    checkState(
        (long) windowSize == actualWindowSize,
        "Window: %s != %s",
        (long) windowSize,
        actualWindowSize);
    checkState(
        (long) protectedSize == actualProtectedSize,
        "Protected: %s != %s",
        (long) protectedSize,
        actualProtectedSize);
    checkState(
        actualProbationSize == calculatedProbationSize,
        "Probation: %s != %s",
        actualProbationSize,
        calculatedProbationSize);
    checkState(data.size() <= maximumSize, "Maximum: %s > %s", data.size(), maximumSize);
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

    public List<Integer> kValues() {
      return config().getIntList("ca-hill-climber-window.cra.k");
    }

    public List<Integer> maxLists() {
      return config().getIntList("ca-hill-climber-window.cra.max-lists");
    }

  }
}
