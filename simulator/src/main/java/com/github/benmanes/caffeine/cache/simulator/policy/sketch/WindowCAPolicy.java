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
package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.admission.Admittor;
import com.github.benmanes.caffeine.cache.simulator.admission.LATinyLfu;
import com.github.benmanes.caffeine.cache.simulator.policy.*;
import com.github.benmanes.caffeine.cache.simulator.policy.linked.CraBlock;
import com.typesafe.config.Config;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static java.util.stream.Collectors.toSet;

import static java.lang.System.Logger;

/**
 * An access time or latency aware implementation of the WindowTinyLFU policy. Using LRBB blocks
 * instead of LRU building blocks, can be adjusted using the config file, as to make the PROBATION
 * and PROTECTED function as LRU.
 *
 * @author himelbrand@gmail.com (Omri Himelbrand)
 */
@Policy.PolicySpec(name = "sketch.WindowCA")
public final class WindowCAPolicy implements Policy {

  private final PolicyStats policyStats;
  private final LatencyEstimator<Long> latencyEstimator;
  private final Admittor admittor;
  private final long cacheCapacity;

  private final CraBlock windowBlock;
  private final CraBlock probationBlock;
  private final CraBlock protectedBlock;

  private double normalizationBias;
  private double normalizationFactor;
  private double maxDelta;
  private int maxDeltaCounts;
  private int samplesCount;

  private static final Logger logger = System.getLogger(LatencyEstimator.class.getName());

  public WindowCAPolicy(double percentMain, WindowCASettings settings, int decayFactor,
                        int maxLists) {
    this.policyStats = new PolicyStats("sketch.WindowCATinyLfu (%.0f%%,decayFactor=%d,maxLists=%d)", 100 * (1.0d - percentMain),
                                       decayFactor,
                                       maxLists);
    this.latencyEstimator = createEstimator(settings.config());
    this.admittor = new LATinyLfu(settings.config(), policyStats, latencyEstimator);
    int mainCacheSize = (int) (settings.maximumSize() * percentMain);
    int protectedCapacity = (int) (mainCacheSize * settings.percentMainProtected());
    int windowCapacity = (int) settings.maximumSize() - mainCacheSize;
    this.cacheCapacity = settings.maximumSize();
    this.protectedBlock = new CraBlock(decayFactor, maxLists, protectedCapacity, latencyEstimator, "protected-block");
    this.probationBlock = new CraBlock(decayFactor, maxLists, mainCacheSize - protectedCapacity, latencyEstimator, "probation-block");
    this.windowBlock = new CraBlock(decayFactor, maxLists, windowCapacity, latencyEstimator, "window-block");
    this.normalizationBias = 0;
    this.normalizationFactor = 0;
    this.maxDelta = 0;
    this.maxDeltaCounts = 0;
    this.samplesCount = 0;
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

    logger.log(Logger.Level.DEBUG,
               String.format("Created estimator of type %s, class: %s",
                             estimationType,
                             estimator.getClass().getSimpleName()));

    return estimator;
  }

  /**
   * Returns all variations of this policy based on the configuration parameters.
   */
  public static Set<Policy> policies(Config config) {
    WindowCASettings settings = new WindowCASettings(config);
    int decayFactor = settings.decayFactor();
    int maxLists = settings.maxLists();

    return settings.percentMain().stream()
        .map(percentMain -> new WindowCAPolicy(percentMain, settings, decayFactor, maxLists))
        .collect(toSet());
  }

  @Override
  public PolicyStats stats() {
    return policyStats;
  }

  /**
   * Adds the entry to the admission window, evicting if necessary.
   */
  private void onMiss(AccessEvent event) {
    event.changeEventStatus(AccessEvent.EventStatus.MISS);
    admittor.record(event);
    windowBlock.addEntry(event);
    evict();
  }

  /**
   * Moves the entry to the MRU position in the admission window.
   */
  private void onWindowHit(EntryData entry) {
    windowBlock.moveToTail(entry);
  }

  /**
   * Promotes the entry to the protected region's MRU position, demoting an entry if necessary.
   */
  private void onProbationHit(EntryData entry) {
    probationBlock.remove(entry.key());
    protectedBlock.addEntry(entry);

    if (protectedBlock.isFull()) {
      EntryData demote = protectedBlock.findVictim();
      protectedBlock.remove(demote.key());
      probationBlock.addEntry(demote);
    }
  }

  /**
   * Moves the entry to the MRU position, if it falls outside the fast-path threshold.
   */
  private void onProtectedHit(EntryData entry) {
    protectedBlock.moveToTail(entry);
  }

  private void updateNormalization(long key) {
    double delta = latencyEstimator.getDelta(key);

    if (delta > normalizationFactor) {
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
   * Evicts from the admission window into the probation space. If the size exceeds the maximum,
   * then the admission candidate and probation's victim are evaluated and one is evicted.
   */
  private void evict() {
    if (!windowBlock.isFull()) {
      return;
    }

    EntryData windowBlockVictim = windowBlock.removeVictim();
    probationBlock.addEntry(windowBlockVictim);
    if (size() > cacheCapacity) {
      EntryData probationBlockVictim = probationBlock.findVictim();
      EntryData evict = admittor.admit(windowBlockVictim.event(), probationBlockVictim.event())
                        ? probationBlockVictim
                        : windowBlockVictim;

      probationBlock.remove(evict.key());
      policyStats.recordEviction();
    }
  }

  private int size() {
    return windowBlock.size() + protectedBlock.size() + probationBlock.size();
  }

  @Override
  public void record(AccessEvent event) {
    final long key = event.key();
    policyStats.recordOperation();
    EntryData entry = null;

    if (windowBlock.isHit(key)) {
      entry = windowBlock.get(key);
      onWindowHit(entry);
    } else if (probationBlock.isHit(key)) {
      entry = probationBlock.get(key);
      onProbationHit(entry);
    } else if (protectedBlock.isHit(key)) {
      entry = protectedBlock.get(key);
      onProtectedHit(entry);
    } else {
      onMiss(event);
      latencyEstimator.record(event.key(), event.missPenalty(), event.getArrivalTime());
      policyStats.recordMiss();
      policyStats.recordMissPenalty(event.missPenalty());
      updateNormalization(key);
    }

    if (entry != null) {
      admittor.record(event.key());
      recordAccordingToAvailability(entry, event);
    }
  }

  private void recordAccordingToAvailability(EntryData entry, AccessEvent currEvent) {
    boolean isAvailable = entry.event().isAvailableAt(currEvent.getArrivalTime());
    if (isAvailable) {
      currEvent.changeEventStatus(AccessEvent.EventStatus.HIT);
      policyStats.recordHit();
      policyStats.recordHitPenalty(currEvent.hitPenalty());
      latencyEstimator.recordHit(currEvent.hitPenalty());
    } else {
      currEvent.changeEventStatus(AccessEvent.EventStatus.DELAYED_HIT);
      currEvent.setDelayedHitPenalty(entry.event().getAvailabilityTime());
      policyStats.recordDelayedHitPenalty(currEvent.delayedHitPenalty());
      policyStats.recordDelayedHit();
      latencyEstimator.addValueToRecord(currEvent.key(), currEvent.delayedHitPenalty(), currEvent.getArrivalTime());
    }
  }

  @Override
  public void finished() {
    checkState(size() <= cacheCapacity);
  }

  @Override
  public boolean isPenaltyAware() { return true; }

  enum Status {
    WINDOW, PROBATION, PROTECTED
  }

  public static final class WindowCASettings extends BasicSettings {

    public WindowCASettings(Config config) {
      super(config);
    }

    public List<Double> percentMain() {
      return config().getDoubleList("ca-window.percent-main");
    }

    public double percentMainProtected() {
      return config().getDouble("ca-window.percent-main-protected");
    }

    public int decayFactor() {
      return config().getInt("ca-window.cra.decay-factor");
    }

    public int maxLists() {
      return config().getInt("ca-window.cra.max-lists");
    }
  }
}
