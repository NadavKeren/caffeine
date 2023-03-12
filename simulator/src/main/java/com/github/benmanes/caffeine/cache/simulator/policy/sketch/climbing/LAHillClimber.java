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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;

/**
 * A hill climbing algorithm to tune the admission window size.
 *
 * @author himelbrand@gmail.com (Omri Himelbrand)
 */
public interface LAHillClimber {

  /**
   * Records that a hit occurred with a full cache.
   *
   * @param event  the key accessed
   * @param queue  the queue the entry was found in
   * @param isFull if the cache is fully populated
   */
  void onHit(AccessEvent event, QueueType queue, boolean isFull);

  /**
   * Records that a miss occurred with a full cache.
   *
   * @param event  the key accessed
   * @param isFull if the cache is fully populated and had to evict
   */
  void onMiss(AccessEvent event, boolean isFull);

  /**
   * Determines how to adapt the segment sizes.
   *
   * @param windowSize    the current window size
   * @param probationSize the current probation size
   * @param protectedSize the current protected size
   * @param isFull        if the cache is fully populated
   * @return the adjustment to the segments
   */
  Adaptation adapt(double windowSize, double probationSize, double protectedSize, boolean isFull);

  enum QueueType {
    WINDOW, PROBATION, PROTECTED, OTHER
  }

  /**
   * The adaptation type and its magnitude.
   */
  enum AdaptationType {
    HOLD, INCREASE_WINDOW, DECREASE_WINDOW
  }

  final class Adaptation {


    private static final Adaptation HOLD = new Adaptation(0, AdaptationType.HOLD);

    public final double amount;
    public final AdaptationType type;

    private Adaptation(double amount, AdaptationType type) {
      checkArgument(amount >= 0, "Step size %s must be positive", amount);
      this.type = checkNotNull(type);
      this.amount = amount;
    }

    /**
     * Returns the adaption based on the amount, where a negative value decreases the window.
     */
    public static Adaptation adaptBy(double amount) {
      if (amount == 0) {
        return hold();
      } else if (amount < 0) {
        return decreaseWindow(Math.abs(amount));
      } else {
        return increaseWindow(amount);
      }
    }

    public static Adaptation hold() {
      return HOLD;
    }

    public static Adaptation increaseWindow(double amount) {
      return new Adaptation(amount, AdaptationType.INCREASE_WINDOW);
    }

    public static Adaptation decreaseWindow(double amount) {
      return new Adaptation(amount, AdaptationType.DECREASE_WINDOW);
    }

    @Override
    public String toString() {
      switch (type) {
        case HOLD:
          return "0";
        case INCREASE_WINDOW:
          return "+" + amount;
        case DECREASE_WINDOW:
          return "-" + amount;
        default:
          throw new IllegalStateException();
      }
    }
  }
}