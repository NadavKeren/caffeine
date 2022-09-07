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
import com.github.benmanes.caffeine.cache.simulator.admission.Admittor;
import com.github.benmanes.caffeine.cache.simulator.admission.LATinyLfu;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.linked.CraBlock.Node;
import com.github.benmanes.caffeine.cache.simulator.policy.linked.CraBlock;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing.LAHillClimber.QueueType;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;

import static com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing.LAHillClimber.Adaptation.Type.DECREASE_WINDOW;
import static com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing.LAHillClimber.Adaptation.Type.INCREASE_WINDOW;
import static com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing.LAHillClimber.QueueType.*;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Locale.US;
import static java.util.stream.Collectors.toSet;

/**
 * The WindowLA algorithm where the size of the admission window is adjusted using the a latency
 * aware hill climbing algorithm.
 *
 * @author himelbrand@gmail.com (Omri Himelbrand)
 */
@SuppressWarnings("PMD.TooManyFields")
@Policy.PolicySpec(name = "sketch.ACA")
public final class AdaptiveCAPolicy implements Policy {

  private final double initialPercentMain;
  private final Long2ObjectMap<Node> data;
  private final PolicyStats policyStats;
  private final LAHillClimber climber;
  private final Admittor admittor;
  private final long maximumSize;

  private final CraBlock headProbation;
  private final CraBlock headProtected;
  private final CraBlock headWindow;

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

    int maxMain = (int) (settings.maximumSize() * percentMain);
    this.maxProtected = (int) (maxMain * settings.percentMainProtected());
    this.maxWindow = settings.maximumSize() - maxMain;
    this.data = new Long2ObjectOpenHashMap<>();
    this.maximumSize = settings.maximumSize();
    this.headProtected = new CraBlock(k, maxLists, this.maxProtected);
    this.headProbation = new CraBlock(k, maxLists, maxMain - this.maxProtected);
    this.headWindow = new CraBlock(k, maxLists, this.maxWindow);
    this.initialPercentMain = percentMain;
    this.policyStats = new PolicyStats("CAHillClimberWindow (%s)(k=%.2f,maxLists=%d)",
            strategy.name().toLowerCase(US), k, maxLists);
    this.admittor = new LATinyLfu(settings.config(), policyStats);
    this.climber = strategy.create(settings.config());
    this.k = k;
    this.normalizationBias = 0;
    this.normalizationFactor = 0;
    this.maxDelta = 0;
    this.maxDeltaCounts = 0;
    this.samplesCount = 0;

    printSegmentSizes();
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
    Node node = data.get(key);
    admittor.record(event);

    QueueType queue = null;
    if (node == null) {

      if (event.delta() > normalizationFactor){
        samplesCount++;
        maxDelta = (maxDelta*maxDeltaCounts + event.delta())/++maxDeltaCounts;
      }
      normalizationBias = normalizationBias > 0 ? Math.min(normalizationBias,Math.max(0,event.delta())) : Math.max(0,event.delta());
      if (samplesCount%1000 == 0 || normalizationFactor == 0){
        normalizationFactor = maxDelta;
        maxDeltaCounts = 1;
        samplesCount = 0;
      }
      updateNormalization();
      onMiss(event);
      policyStats.recordMiss();
    } else {
      node.event().updateHitPenalty(event.hitPenalty());
      if (headWindow.isHit(key)) {
        onWindowHit(node);
        policyStats.recordHit();
        queue = WINDOW;
      } else if (headProbation.isHit(key)) {
        onProbationHit(node);
        policyStats.recordHit();
        queue = PROBATION;
      } else if (headProtected.isHit(key)) {
        onProtectedHit(node);
        policyStats.recordHit();
        queue = PROTECTED;
      } else {
        throw new IllegalStateException();
      }
    }
    climb(event, queue, isFull);
  }

  private void updateNormalization() {
    headProtected.setNormalization(normalizationBias,normalizationFactor);
    headProbation.setNormalization(normalizationBias,normalizationFactor);
    headWindow.setNormalization(normalizationBias,normalizationFactor);
  }

  /**
   * Adds the entry to the admission window, evicting if necessary.
   */
  private void onMiss(AccessEvent event) {
    long key = event.key();
    Node node = headWindow.addEntry(event);
    data.put(key, node);
    windowSize++;
    evict();
  }

  /**
   * Moves the entry to the MRU position in the admission window.
   */
  private void onWindowHit(Node node) {
    node.moveToTail();
  }

  /**
   * Promotes the entry to the protected region's MRU position, demoting an entry if necessary.
   */
  private void onProbationHit(Node node) {
    node.remove();
    headProbation.remove(node.key());
    headProtected.addEntry(node);
    protectedSize++;
    demoteProtected();
  }

  private void demoteProtected() {
    if (protectedSize > maxProtected) {
      Node demote = headProtected.findVictim();
      demote.remove();
      headProtected.remove(demote.key());
      headProbation.addEntry(demote);
      protectedSize--;
    }
  }

  /**
   * Moves the entry to the MRU position, if it falls outside of the fast-path threshold.
   */
  private void onProtectedHit(Node node) {
    node.moveToTail();
  }

  /**
   * Evicts from the admission window into the probation space. If the size exceeds the maximum,
   * then the admission candidate and probation's victim are evaluated and one is evicted.
   */
  private void evict() {
    if (windowSize <= maxWindow) {
      return;
    }

    Node candidate = headWindow.findVictim();
    windowSize--;
    candidate.remove();
    headWindow.remove(candidate.key());
    headProbation.addEntry(candidate);
    if (data.size() > maximumSize) {
      Node victim = headProbation.findVictim();
      Node evict = admittor.admit(candidate.event(), victim.event()) ? victim : candidate;
      data.remove(evict.key());
      evict.remove();
      headProbation.remove(evict.key());
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
    if (adaptation.type == INCREASE_WINDOW) {
      increaseWindow(adaptation.amount);
    } else if (adaptation.type == DECREASE_WINDOW) {
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
      Node candidate = headProbation.findVictim();
      candidate.remove();
      headProbation.remove(candidate.key());
      headWindow.addEntry(candidate);
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
    int steps = (int) windowSize - (int) (windowSize - quota);
    windowSize -= quota;

    for (int i = 0; i < steps; i++) {
      maxWindow--;
      maxProtected++;
      Node candidate = headWindow.findVictim();
      candidate.remove();
      headWindow.remove(candidate.key());
      headProbation.addEntry(candidate);
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

    long actualWindowSize = data.values().stream().filter(n -> headWindow.isHit(n.key())).count();
    long actualProbationSize = data.values().stream().filter(n -> headProbation.isHit(n.key()))
        .count();
    long actualProtectedSize = data.values().stream().filter(n -> headProtected.isHit(n.key()))
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

  public static final class AdaptiveCASettings extends BasicSettings {

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
