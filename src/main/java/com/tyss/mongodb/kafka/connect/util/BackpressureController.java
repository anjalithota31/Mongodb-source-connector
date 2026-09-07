/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tyss.mongodb.kafka.connect.util;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackpressureController {
  private static final Logger LOGGER = LoggerFactory.getLogger(BackpressureController.class);

  private final long maxMessagesPerSecond;
  private final long windowSizeMillis;
  private final boolean enabled;

  private final LongAdder messageCount = new LongAdder();
  private final AtomicLong windowStartTime = new AtomicLong(System.currentTimeMillis());
  private volatile boolean isThrottling = false;

  public BackpressureController(
      final long maxMessagesPerSecond, final long windowSizeMillis, final boolean enabled) {
    if (maxMessagesPerSecond <= 0) {
      throw new IllegalArgumentException(
          "Max messages per second must be positive: " + maxMessagesPerSecond);
    }
    if (windowSizeMillis <= 0) {
      throw new IllegalArgumentException(
          "Window size millis must be positive: " + windowSizeMillis);
    }
    this.maxMessagesPerSecond = maxMessagesPerSecond;
    this.windowSizeMillis = windowSizeMillis;
    this.enabled = enabled;
    LOGGER.info(
        "BackpressureController initialized: maxMessagesPerSecond={}, windowSizeMillis={}, enabled={}",
        maxMessagesPerSecond,
        windowSizeMillis,
        enabled);
  }

  public boolean allowMessage() {
    if (!enabled) {
      return true;
    }

    long currentTime = System.currentTimeMillis();
    long windowStart = windowStartTime.get();

    // Reset window if expired
    if (currentTime - windowStart >= windowSizeMillis) {
      if (windowStartTime.compareAndSet(windowStart, currentTime)) {
        messageCount.reset();
        if (isThrottling) {
          isThrottling = false;
          LOGGER.info("Backpressure throttling ended");
        }
      }
    }

    long currentCount = messageCount.sum();
    long maxInWindow = (maxMessagesPerSecond * windowSizeMillis) / 1000;

    if (currentCount >= maxInWindow) {
      if (!isThrottling) {
        isThrottling = true;
        LOGGER.warn(
            "Backpressure throttling activated: {}/{} messages in current window",
            currentCount,
            maxInWindow);
      }
      return false;
    }

    messageCount.increment();
    return true;
  }

  public void recordMessage() {
    messageCount.increment();
  }

  public long getMessageCount() {
    return messageCount.sum();
  }

  public boolean isThrottling() {
    return isThrottling;
  }

  public long getMaxMessagesPerSecond() {
    return maxMessagesPerSecond;
  }

  public void reset() {
    messageCount.reset();
    windowStartTime.set(System.currentTimeMillis());
    isThrottling = false;
    LOGGER.info("BackpressureController reset");
  }
}
