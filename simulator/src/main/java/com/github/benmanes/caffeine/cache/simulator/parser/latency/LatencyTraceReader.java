/*
 * Copyright 2019 Omri Himelbrand. All Rights Reserved.
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
package com.github.benmanes.caffeine.cache.simulator.parser.latency;

import com.github.benmanes.caffeine.cache.simulator.parser.TextTraceReader;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy.Characteristic;
import com.google.common.collect.ImmutableSet;
import java.math.BigInteger;
import java.util.stream.Stream;

/**
 * A reader for the trace files of mp-trace-maker. Traces & format can be found at: git repo
 * address
 *
 * @author himelbrand@gmail.com (Omri Himelbrand)
 */
public final class LatencyTraceReader extends TextTraceReader {
  private final static BigInteger MAX_LONG = new BigInteger(Long.toString(Long.MAX_VALUE));

  public LatencyTraceReader(String filePath) {
    super(filePath);
  }

  @Override
  public ImmutableSet<Characteristic> characteristics() {
    return ImmutableSet.of();
  }

  private long parseKey(String uuid) {
    BigInteger num = new BigInteger(uuid);
    return num.compareTo(MAX_LONG) <= 0 ? num.longValue() :  num.mod(MAX_LONG).longValue();
  }

  @Override
  public Stream<AccessEvent> events() {
    return lines()
        .map(line -> line.split(" ", 4))
        .map(
            split ->
                AccessEvent.forKeyPenaltiesAndArrivalTime(
                    parseKey(split[1]),
                    Double.parseDouble(split[2]),
                    Double.parseDouble(split[3]),
                    Double.parseDouble(split[0])));
  }
}