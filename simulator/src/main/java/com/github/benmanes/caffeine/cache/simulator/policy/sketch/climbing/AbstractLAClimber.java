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

import static com.google.common.base.Preconditions.checkState;

import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;

/**
 * A skeleton for latency aware hill climbers that walk using the access times.
 *
 * @author himelbrand@gmail.com (Omri Himelbrand)
 */
public abstract class AbstractLAClimber implements LAHillClimber {

  protected int sampleSize;
  protected double penaltiesInSample;
  protected double penaltiesInWindow;
  protected double penaltiesInMain;
  protected double previousAvgPenalty;
  protected int sampleCount;

  static final boolean debug = false;

  @Override
  public void onMiss(AccessEvent event, boolean isFull) {
    if (isFull) {
      sampleCount++;
      penaltiesInSample += event.missPenalty();
    }
  }

  @Override
  public void onHit(AccessEvent event, QueueType queueType, boolean isFull) {
    if (isFull) {
      sampleCount++;
      penaltiesInSample += event.hitPenalty();
      if (queueType == QueueType.WINDOW) {
        penaltiesInWindow += event.hitPenalty();
      } else {
        penaltiesInMain += event.hitPenalty();
      }
    }
  }

  @Override
  public Adaptation adapt(double windowSize, double probationSize,
      double protectedSize, boolean isFull) {
    if (!isFull) {
      return Adaptation.hold();
    }

    checkState(sampleSize > 0, "Sample size may not be zero");

    if (sampleCount < sampleSize) {
      return Adaptation.hold();
    }

    double avgPenalty = penaltiesInSample / sampleCount;
    Adaptation adaption = Adaptation.adaptBy(adjust(avgPenalty));
    resetSample(avgPenalty);

    if (debug) {
      System.out.printf("%.3f\t%.2f%n", avgPenalty, windowSize);
    }
    return adaption;
  }

  /**
   * Returns the amount to adapt by.
   */
  protected abstract double adjust(double avgPenalty);

  /**
   * Starts the next sample period.
   */
  protected void resetSample(double avgPenalty) {
    previousAvgPenalty = avgPenalty;
    sampleCount = 0;
    penaltiesInSample = 0;
    penaltiesInMain = 0;
    penaltiesInWindow = 0;
  }
}
