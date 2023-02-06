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
import com.github.benmanes.caffeine.cache.simulator.CacheEntry;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.*;
import com.github.benmanes.caffeine.cache.simulator.admission.Admittor;
import com.github.benmanes.caffeine.cache.simulator.admission.LATinyLfu;
import com.github.benmanes.caffeine.cache.simulator.policy.*;
import com.github.benmanes.caffeine.cache.simulator.policy.linked.CraBlock;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing.LAHillClimber.QueueType;
import com.typesafe.config.Config;
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
 * Both the Window and Protected blocks are adjustable, while the probation is fixed-sized throughout the run.
 *
 * @author himelbrand@gmail.com (Omri Himelbrand)
 * @author nadav.keren@gmail.com (Nadav Keren)
 */
@SuppressWarnings("PMD.TooManyFields")
@Policy.PolicySpec(name = "sketch.ACA")
public final class AdaptiveCAPolicy implements Policy {

  private final double initialPercentMain;
  private final WindowCAPolicy.WindowCAStats policyStats;
  private final LAHillClimber climber;
  private final LatencyEstimator<Long> latencyEstimator;
  private final Admittor admittor;
  private final long cacheSize;

  private final CraBlock probationBlock;
  private final CraBlock protectedBlock;
  private final CraBlock windowBlock;


  private double protectedPercentage;

  static final boolean debug = false;
  static final boolean trace = true;
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
    this.cacheSize = settings.maximumSize();
    final long mainCacheSize = (int) (cacheSize * percentMain);
    final long windowBlockSize = cacheSize - mainCacheSize;
    this.protectedPercentage = settings.percentMainProtected();
    final long protectedBlockSize = (int) (mainCacheSize * protectedPercentage);
    final long probationBlockSize = mainCacheSize - protectedBlockSize;
    this.protectedBlock = new CraBlock(k, maxLists, protectedBlockSize, latencyEstimator, false);
    this.probationBlock = new CraBlock(k, maxLists, probationBlockSize, latencyEstimator, true);
    this.windowBlock = new CraBlock(k, maxLists, windowBlockSize, latencyEstimator, false);
    this.initialPercentMain = percentMain;
    this.policyStats = new WindowCAPolicy.WindowCAStats("CAHillClimberWindow (%s)(k=%.2f,maxLists=%d)",
                                                        strategy.name().toLowerCase(US),
                                                        k,
                                                        maxLists);
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
              policies.add(new AdaptiveCAPolicy(climber, percentMain, settings, k, ml));
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
    boolean isFull = windowBlock.isFull() && probationBlock.isFull() && protectedBlock.isFull();
    CacheEntry entry = null;
    policyStats.recordOperation();
    admittor.record(event);

    QueueType queue = null;
    if (windowBlock.isHit(key)) {
      windowBlock.moveToTail(key);
      entry = windowBlock.get(key);

      queue = QueueType.WINDOW;
    } else if (probationBlock.isHit(key)) {
      entry = probationBlock.remove(key);
      if (protectedBlock.isFull()) {
        demoteProtected();
      }
      protectedBlock.addEntry(entry);

      queue = QueueType.PROBATION;
    } else if (protectedBlock.isHit(key)) {
      protectedBlock.moveToTail(key);
      entry = protectedBlock.get(key);

      queue = QueueType.PROTECTED;
    } else {
      onMiss(event);
    }

    if (entry != null) {
      recordHit(entry, event, queue);
    } else {
      policyStats.recordMiss();
      policyStats.recordMissPenalty(event.missPenalty());
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

    protectedBlock.setNormalization(normalizationBias,normalizationFactor);
    probationBlock.setNormalization(normalizationBias,normalizationFactor);
    windowBlock.setNormalization(normalizationBias,normalizationFactor);
  }

  /**
   * Adds the entry to the admission window, evicting if necessary.
   */
  private void onMiss(AccessEvent event) {
    updateNormalization(event.key());
    windowBlock.addEntry(new CacheEntry(event));
    evict();
  }

  private void recordHit(CacheEntry entry, AccessEvent currEvent, QueueType queue) {
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

  /**
   * Performs the hill climbing process.
   */
  private void climb(AccessEvent event, @Nullable QueueType queue, boolean isFull) {
    if (queue == null) {
      climber.onMiss(event, isFull);
    } else {
      climber.onHit(event, queue, isFull);
    }

    double probationSize = cacheSize - windowBlock.getMaximumSize() - protectedBlock.getMaximumSize();
    LAHillClimber.Adaptation adaptation = climber.adapt((double) windowBlock.getMaximumSize(),
                                                        probationSize,
                                                        (double) protectedBlock.getMaximumSize(),
                                                        isFull);
    if (adaptation.type == AdaptationType.INCREASE_WINDOW) {
      increaseWindow(adaptation.amount);
    } else if (adaptation.type == AdaptationType.DECREASE_WINDOW) {
      decreaseWindow(adaptation.amount);
    }

    if (adaptation.type != AdaptationType.HOLD) {
      policyStats.recordAdaption((double) windowBlock.getMaximumSize() / this.cacheSize);
    }
  }

  private void increaseWindow(double amount) {
    checkState(amount >= 0.0);
    final long originalProtectedSize = protectedBlock.getMaximumSize();
    if (originalProtectedSize > 0) {
      final long increaseAmount = (long) Math.min(amount, (double) originalProtectedSize);

      final long originalWindowSize = windowBlock.getMaximumSize();
      final long newWindowSize = originalWindowSize + increaseAmount;
      final long newMainCacheSize = cacheSize - newWindowSize;
      final long newProtectedSize = (long) (newMainCacheSize * protectedPercentage);
      final long newProbationSize = newMainCacheSize - newProtectedSize;

      final long protectedSizeChange = originalProtectedSize - newProtectedSize;
      probationBlock.changeMaximalSize(probationBlock.getMaximumSize() + protectedSizeChange);
      for (int i = 0; i < protectedSizeChange; ++i) {
        demoteProtected();
      }

      protectedBlock.changeMaximalSize(newProtectedSize);

      windowBlock.changeMaximalSize(newWindowSize);
      for (int i = 0; i < increaseAmount; ++i) {
        CacheEntry entry = probationBlock.removeVictim();
        windowBlock.addEntry(entry);
      }

      probationBlock.changeMaximalSize(newProbationSize);

      checkState(protectedBlock.getMaximumSize() == newProtectedSize);
      checkState(probationBlock.getMaximumSize() == newProbationSize);
      checkState(windowBlock.getMaximumSize() == newWindowSize);
      checkState(newProbationSize + newProtectedSize + newWindowSize == cacheSize);

      if (trace) {
        System.out.printf("Increased by %,d; Window Size: (%,d -> %,d)%n", increaseAmount, originalWindowSize, newWindowSize);
      }
    } else if (trace) {
      System.out.printf("No Increase done; Window Size: %,d%n", windowBlock.getMaximumSize());
    }
  }

  private void decreaseWindow(double amount) {
    checkState(amount >= 0.0);
    final long originalWindowSize = windowBlock.getMaximumSize();
    if (originalWindowSize > 0) {
      final long decreaseAmount = (long) Math.abs(Math.min(amount, (double) originalWindowSize));
      final long oldProbationSize = probationBlock.getMaximumSize();
      final long newWindowSize = originalWindowSize - decreaseAmount;
      final long newMainCacheSize = cacheSize - newWindowSize;
      final long newProtectedSize = (long) (newMainCacheSize * protectedPercentage);
      final long newProbationSize = newMainCacheSize - newProtectedSize;

      protectedBlock.changeMaximalSize(newProtectedSize);
      probationBlock.changeMaximalSize(newProbationSize);

      final long numOfItemsToDemote = newProbationSize - oldProbationSize;

      for (int i = 0; i < numOfItemsToDemote; ++i) {
        demoteProtected();
      }

      for (int i = 0; i < decreaseAmount; ++i) {
        CacheEntry entry = windowBlock.removeVictim();
        protectedBlock.addEntry(entry);
      }

      checkState(protectedBlock.getMaximumSize() == newProtectedSize);
      checkState(probationBlock.getMaximumSize() == newProbationSize);
      checkState(windowBlock.getMaximumSize() == newWindowSize);
      checkState(newProbationSize + newProtectedSize + newWindowSize == cacheSize);

      if (trace) {
        System.out.printf("Decreased by %,d; (%,d -> %,d)%n", decreaseAmount, originalWindowSize, newWindowSize);
      }
    } else if (trace) {
      System.out.printf("No Decrease done; Window Size: %,d%n", windowBlock.getMaximumSize());
    }
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
  public void finished() {
    policyStats.setPercentAdaption(
            (windowBlock.getMaximumSize() / (double) cacheSize) - (1.0 - initialPercentMain));
    printSegmentSizes();

    // Allowing rounding errors that can be up to +-3
    final long totalSize = windowBlock.getMaximumSize()
                         + protectedBlock.getMaximumSize()
                         + probationBlock.getMaximumSize();
    checkState(Math.abs(totalSize - cacheSize) <= 3 ,
               "Maximum size mismatch: expected: %s got: %s",
               cacheSize,
               totalSize);
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
