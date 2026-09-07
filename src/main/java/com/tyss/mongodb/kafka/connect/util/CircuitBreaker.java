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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CircuitBreaker {
  private static final Logger LOGGER = LoggerFactory.getLogger(CircuitBreaker.class);

  private final int failureThreshold;
  private final long timeoutMillis;
  private final String name;

  private final AtomicInteger failureCount = new AtomicInteger(0);
  private final AtomicLong lastFailureTime = new AtomicLong(0);
  private volatile boolean isOpen = false;
  private volatile long lastStateChangeTime = 0;

  public CircuitBreaker(final String name, final int failureThreshold, final long timeoutMillis) {
    this.name = name;
    this.failureThreshold = failureThreshold;
    this.timeoutMillis = timeoutMillis;
    LOGGER.info(
        "CircuitBreaker initialized: name={}, failureThreshold={}, timeoutMillis={}",
        name,
        failureThreshold,
        timeoutMillis);
  }

  public boolean allowRequest() {
    if (!isOpen) {
      return true;
    }

    long timeSinceLastFailure = System.currentTimeMillis() - lastFailureTime.get();
    if (timeSinceLastFailure > timeoutMillis) {
      // Attempt to transition to half-open state
      LOGGER.info(
          "CircuitBreaker '{}' attempting to reset after {} ms timeout",
          name,
          timeSinceLastFailure);
      reset();
      return true;
    }

    LOGGER.debug("CircuitBreaker '{}' is OPEN, blocking request", name);
    return false;
  }

  public void recordSuccess() {
    if (isOpen) {
      LOGGER.info("CircuitBreaker '{}' transitioning to CLOSED after successful request", name);
      reset();
    } else {
      failureCount.set(0);
    }
  }

  public void recordFailure() {
    int failures = failureCount.incrementAndGet();
    lastFailureTime.set(System.currentTimeMillis());

    if (failures >= failureThreshold && !isOpen) {
      open();
    }

    LOGGER.warn(
        "CircuitBreaker '{}' recorded failure (count: {}, threshold: {}, state: {})",
        name,
        failures,
        failureThreshold,
        isOpen ? "OPEN" : "CLOSED");
  }

  private void open() {
    isOpen = true;
    lastStateChangeTime = System.currentTimeMillis();
    LOGGER.error("CircuitBreaker '{}' opened after {} failures", name, failureCount.get());
  }

  private void reset() {
    isOpen = false;
    failureCount.set(0);
    lastStateChangeTime = System.currentTimeMillis();
    LOGGER.info("CircuitBreaker '{}' reset to CLOSED state", name);
  }

  public boolean isOpen() {
    return isOpen;
  }

  public int getFailureCount() {
    return failureCount.get();
  }

  public long getLastFailureTime() {
    return lastFailureTime.get();
  }

  public String getState() {
    return isOpen ? "OPEN" : "CLOSED";
  }

  public static class State {
    private final boolean isOpen;
    private final int failureCount;
    private final long lastFailureTime;
    private final String state;

    public State(
        final boolean isOpen,
        final int failureCount,
        final long lastFailureTime,
        final String state) {
      this.isOpen = isOpen;
      this.failureCount = failureCount;
      this.lastFailureTime = lastFailureTime;
      this.state = state;
    }

    public boolean isOpen() {
      return isOpen;
    }

    public int getFailureCount() {
      return failureCount;
    }

    public long getLastFailureTime() {
      return lastFailureTime;
    }

    public String getState() {
      return state;
    }
  }
}
