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

import static java.util.Objects.requireNonNull;

import com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing.gradient.LAAdam;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing.gradient.LANadam;
import com.github.benmanes.caffeine.cache.simulator.policy.sketch.climbing.hill.LASimpleClimber;
import com.typesafe.config.Config;
import java.util.function.Function;

/**
 * The latency aware hill climbers.
 *
 * @author himelbrand@gmail.com (Omri Himelbrand)
 */
@SuppressWarnings("ImmutableEnumChecker")
enum LAHillClimberType {
  SIMPLE(LASimpleClimber::new),
  NADAM(LANadam::new),
  ADAM(LAAdam::new);

  private final Function<Config, LAHillClimber> factory;

  LAHillClimberType(Function<Config, LAHillClimber> factory) {
    this.factory = requireNonNull(factory);
  }

  public LAHillClimber create(Config config) {
    return factory.apply(config);
  }
}
