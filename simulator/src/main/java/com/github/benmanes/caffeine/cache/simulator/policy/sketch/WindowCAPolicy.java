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
import com.github.benmanes.caffeine.cache.simulator.CacheEntry;
import com.github.benmanes.caffeine.cache.simulator.admission.Admittor;
import com.github.benmanes.caffeine.cache.simulator.admission.LATinyLfu;
import com.github.benmanes.caffeine.cache.simulator.policy.*;
import com.github.benmanes.caffeine.cache.simulator.policy.linked.CraBlock;
import com.typesafe.config.Config;

import java.util.List;
import java.util.Set;
import java.util.function.DoubleSupplier;

import static com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats.Metric.MetricType.PERCENT;
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

  private static boolean debug = true;

  private final WindowCAStats policyStats;
  private final LatencyEstimator<Long> latencyEstimator;
  private final Admittor admittor;
  private final long cacheSize;

  private final CraBlock windowBlock;
  private final CraBlock probationBlock;
  private final CraBlock protectedBlock;
  private double normalizationBias;
  private double normalizationFactor;
  private double maxDelta;
  private int maxDeltaCounts;
  private int samplesCount;

  private static final Logger logger = System.getLogger(LatencyEstimator.class.getName());

  public WindowCAPolicy(double percentMain, WindowCASettings settings, int k,
                        int maxLists) {
    this.policyStats = new WindowCAStats("sketch.WindowCATinyLfu (%.0f%%,k=%d,maxLists=%d)",
                                         100 * (1.0d - percentMain),
                                         k,
                                         maxLists);
    this.latencyEstimator = createEstimator(settings.config());
    this.admittor = new LATinyLfu(settings.config(), policyStats, latencyEstimator);
    this.cacheSize = settings.maximumSize();
    final int mainCacheSize = (int) (cacheSize * percentMain);
    final long protectedBlockSize = (int) (mainCacheSize * settings.percentMainProtected());
    final long windowSize = this.cacheSize - mainCacheSize;
    this.protectedBlock = new CraBlock(k, maxLists, protectedBlockSize, latencyEstimator, true);
    this.probationBlock = new CraBlock(k, maxLists, mainCacheSize - protectedBlockSize, latencyEstimator, true);
    this.windowBlock = new CraBlock(k, maxLists, windowSize, latencyEstimator, true);
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
      case "latest-with-delayed-hits":
        estimator = new BurstLatencyEstimator<>();
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
                                               .map(k ->
                                                            settings.maxLists().stream()
                                                                    .map(ml -> new WindowCAPolicy(percentMain,
                                                                                                  settings,
                                                                                                  k,
                                                                                                  ml))))
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
    event.changeEventStatus(AccessEvent.EventStatus.MISS);
    updateNormalization(event.key());
    windowBlock.addEntry(new CacheEntry(event));
    evict();
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

  private void recordHit(CacheEntry entry, AccessEvent currEvent, BlockType queue) {
    if (entry.getEvent().isAvailableAt(currEvent.getArrivalTime())) {
      entry.getEvent().updateHitPenalty(currEvent.hitPenalty());
      currEvent.changeEventStatus(AccessEvent.EventStatus.HIT);
      policyStats.recordHit();
      policyStats.recordHitPenalty(currEvent.hitPenalty());
    } else {
      currEvent.changeEventStatus(AccessEvent.EventStatus.DELAYED_HIT);
      currEvent.setDelayedHitPenalty(entry.getEvent().getAvailabilityTime());
      policyStats.recordDelayedHit();
      policyStats.recordDelayedHitPenalty(currEvent.delayedHitPenalty());
    }

    switch (queue) {
      case WINDOW:
        policyStats.recordWindowHit();
        break;
      case PROBATION:
        policyStats.recordProbationHit();
        break;
      case PROTECTED:
        policyStats.recordProtectedHit();
        break;
    }
  }

  private void demoteProtected() {
    CacheEntry demotedEntry = protectedBlock.findVictim();
    protectedBlock.remove(demotedEntry.getEvent().key());
    probationBlock.addEntry(demotedEntry);
  }

  @Override
  public void record(AccessEvent event) {
    long key = event.key();
    CacheEntry entry = null;
    policyStats.recordOperation();
    admittor.record(event);

    BlockType queue = null;
    if (windowBlock.isHit(key)) {
      windowBlock.moveToTail(key);
      entry = windowBlock.get(key);

      queue = BlockType.WINDOW;
    } else if (probationBlock.isHit(key)) {
      entry = probationBlock.remove(key);
      if (protectedBlock.isFull()) {
        demoteProtected();
      }
      protectedBlock.addEntry(entry);

      queue = BlockType.PROBATION;
    } else if (protectedBlock.isHit(key)) {
      protectedBlock.moveToTail(key);
      entry = protectedBlock.get(key);

      queue = BlockType.PROTECTED;
    } else {
      onMiss(event);
    }

    if (entry != null) {
      recordHit(entry, event, queue);
    } else {
      policyStats.recordMiss();
      policyStats.recordMissPenalty(event.missPenalty());
    }
  }

  /**
   * Evicts from the admission window into the probation space. If the size exceeds the maximum,
   * then the admission candidate and probation's victim are evaluated and one is evicted.
   */
  private void evict() {
    if (windowBlock.isFull()) {
      CacheEntry candidate = windowBlock.removeVictim();
      if (probationBlock.isFull()) {
        CacheEntry probationVictim = probationBlock.findVictim();
        boolean shouldAdmitCandidate = admittor.admit(candidate.getEvent(), probationVictim.getEvent());
        if (shouldAdmitCandidate) {
          probationBlock.removeVictim();
          probationBlock.addEntry(candidate);
          policyStats.recordEviction();
        }
      } else {
        probationBlock.addEntry(candidate);
      }
    }
  }

  @Override
  public void finished() {
    final long totalSize = windowBlock.getMaximumSize()
                           + protectedBlock.getMaximumSize()
                           + probationBlock.getMaximumSize();

    // Allowing rounding errors that can be up to +-3
    checkState(Math.abs(totalSize - cacheSize) <= 3 ,
               "Maximum size mismatch: expected: %s got: %s",
               cacheSize,
               totalSize);
  }

  private void printSegmentSizes() {
    if (debug) {
      final long windowSize = windowBlock.getMaximumSize();
      final long protectedSize = protectedBlock.getMaximumSize();
      final long probationSize = probationBlock.getMaximumSize();
      System.out.printf("windowSize=%d, protectedSize=%d, probationSize=%d, percentWindow=%.1f, "
                        + "percentProtected=%.1f, percentProbation=%.1f\n",
                        windowSize,
                        protectedSize,
                        probationSize,
                        (100.0 * windowSize) / cacheSize,
                        (100.0 * protectedSize) / cacheSize,
                        (100.0 * probationSize) / cacheSize);
    }
  }

  @Override
  public boolean isPenaltyAware() { return true; }

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

  public static class WindowCAStats extends PolicyStats {
    private long windowHitCount = 0;
    private long probationHitCount = 0;
    private long protectedHitCount = 0;

    public WindowCAStats(String format, Object... args) {
      super(format, args);
      addMetric(Metric.of("Window Hit Rate", (DoubleSupplier) this::windowHitRate, PERCENT, true));
      addMetric(Metric.of("Probation Hit Rate", (DoubleSupplier) this::probationHitRate, PERCENT, true));
      addMetric(Metric.of("Protected Hit Rate", (DoubleSupplier) this::protectedHitRate, PERCENT, true));
    }

    public double windowHitRate() { return (double) windowHitCount / super.requestCount(); }

    public double probationHitRate() { return (double) probationHitCount / super.requestCount(); }

    public double protectedHitRate() { return (double) protectedHitCount / super.requestCount(); }

    public void recordWindowHit() { ++this.windowHitCount; }

    public void recordProbationHit() { ++this.probationHitCount; }

    public void recordProtectedHit() { ++this.protectedHitCount; }
  }

  protected enum BlockType {
    WINDOW, PROBATION, PROTECTED
  }
}
