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
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

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

  private final Long2ObjectMap<EntryData> data;
  private final PolicyStats policyStats;
  private final LatencyEstimator<Long> latencyEstimator;
  private final Admittor admittor;
  private final long maximumSize;

  private final CraBlock windowBlock;
  private final CraBlock probationBlock;
  private final CraBlock protectedBlock;

  private final long maxWindow;
  private final long maxProtected;

  private int sizeWindow;
  private int sizeProtected;
  private double normalizationBias;
  private double normalizationFactor;
  private double maxDelta;
  private int maxDeltaCounts;
  private int samplesCount;

  private static final Logger logger = System.getLogger(LatencyEstimator.class.getName());

  public WindowCAPolicy(double percentMain, WindowCASettings settings, int k,
                        int maxLists) {
    this.policyStats = new PolicyStats("sketch.WindowCATinyLfu (%.0f%%,k=%d,maxLists=%d)", 100 * (1.0d - percentMain), k,
            maxLists);
    this.latencyEstimator = createEstimator(settings.config());
    this.admittor = new LATinyLfu(settings.config(), policyStats, latencyEstimator);
    int maxMain = (int) (settings.maximumSize() * percentMain);
    this.maxProtected = (int) (maxMain * settings.percentMainProtected());
    this.maxWindow = settings.maximumSize() - maxMain;
    this.data = new Long2ObjectOpenHashMap<>();
    this.maximumSize = settings.maximumSize();
    this.protectedBlock = new CraBlock(k, maxLists, this.maxProtected, latencyEstimator);
    this.probationBlock = new CraBlock(k, maxLists, maxMain - this.maxProtected, latencyEstimator);
    this.windowBlock = new CraBlock(k, maxLists, this.maxWindow, latencyEstimator);
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
                             estimator.getClass().getName()));

    return estimator;
  }

  /**
   * Returns all variations of this policy based on the configuration parameters.
   */
  public static Set<Policy> policies(Config config) {
    WindowCASettings settings = new WindowCASettings(config);
    return settings.percentMain().stream()
        .flatMap(percentMain ->
            settings.kValues().stream()
                .map(k -> settings.maxLists().stream()
                                .map(ml -> new WindowCAPolicy(percentMain, settings, k,ml)
                                )))
        .flatMap(x -> x)
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
    long key = event.key();
    event.changeEventStatus(AccessEvent.EventStatus.MISS);
    admittor.record(event);
    EntryData n = windowBlock.addEntry(event);
    data.put(key, n);
    sizeWindow++;
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

    sizeProtected++;
    if (sizeProtected > maxProtected) {
      EntryData demote = protectedBlock.findVictim();
      protectedBlock.remove(demote.key());
      probationBlock.addEntry(demote);
      sizeProtected--;
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
    if (sizeWindow <= maxWindow) {
      return;
    }

    EntryData windowBlockVictim = windowBlock.removeVictim();
    sizeWindow--;
    probationBlock.addEntry(windowBlockVictim);
    if (data.size() > maximumSize) {
      EntryData victim = probationBlock.findVictim();
      EntryData evict = admittor.admit(windowBlockVictim.event(), victim.event()) ? victim : windowBlockVictim;
      data.remove(evict.key());
      probationBlock.remove(evict.key());
      policyStats.recordEviction();
    }
  }

  @Override
  public void record(AccessEvent event) {
    long key = event.key();
    policyStats.recordOperation();
    EntryData entry = data.get(key);

    if (entry == null) {
      onMiss(event);
      latencyEstimator.record(event.key(), event.missPenalty());
      policyStats.recordMiss();
      policyStats.recordMissPenalty(event.missPenalty());
      updateNormalization(key);
    } else {
      boolean isAvailable = entry.event().isAvailableAt(event.getArrivalTime());
      if (isAvailable) {
        event.changeEventStatus(AccessEvent.EventStatus.HIT);
        policyStats.recordHit();
        policyStats.recordHitPenalty(event.hitPenalty());
      } else {
        event.changeEventStatus(AccessEvent.EventStatus.DELAYED_HIT);
        event.setDelayedHitPenalty(entry.event().getAvailabilityTime());
        policyStats.recordDelayedHitPenalty(event.delayedHitPenalty());
        policyStats.recordDelayedHit();
        latencyEstimator.addValueToRecord(event.key(), event.delayedHitPenalty());
      }

      admittor.record(event.key());

      if (windowBlock.isHit(key)) {
        onWindowHit(entry);
      } else if (probationBlock.isHit(key)) {
        onProbationHit(entry);
      } else if (protectedBlock.isHit(key)) {
        onProtectedHit(entry);
      } else {
        throw new IllegalStateException();
      }
    }
  }

  @Override
  public void finished() {
    long windowSize = data.values().stream().filter(n -> windowBlock.isHit(n.key())).count();
    long probationSize = data.values().stream().filter(n -> probationBlock.isHit(n.key())).count();
    long protectedSize = data.values().stream().filter(n -> protectedBlock.isHit(n.key())).count();
    checkState(windowSize == sizeWindow);
    checkState(protectedSize == sizeProtected);
    checkState(probationSize == data.size() - windowSize - protectedSize);

    checkState(data.size() <= maximumSize);
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

    public List<Integer> kValues() {
      return config().getIntList("ca-window.cra.k");
    }

    public List<Integer> maxLists() {
      return config().getIntList("ca-window.cra.max-lists");
    }
  }
}
