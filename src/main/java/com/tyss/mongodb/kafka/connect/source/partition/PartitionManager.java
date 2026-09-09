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

package com.tyss.mongodb.kafka.connect.source.partition;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionManager.class);

  private final AdminClient adminClient;
  private final long messageThreshold;
  private final int maxPartitions;
  private final boolean enabled;
  private final Map<String, TopicPartitionInfo> topicPartitionInfoMap;

  public PartitionManager(
      final AdminClient adminClient,
      final long messageThreshold,
      final int maxPartitions,
      final boolean enabled) {
    this.adminClient = adminClient;
    this.messageThreshold = messageThreshold;
    this.maxPartitions = maxPartitions;
    this.enabled = enabled;
    this.topicPartitionInfoMap = new ConcurrentHashMap<>();
    LOGGER.info(
        "PartitionManager initialized: enabled={}, threshold={}, maxPartitions={}",
        enabled,
        messageThreshold,
        maxPartitions);
  }

  public void recordMessage(final String topicName) {
    recordMessages(topicName, 1);
  }

  public void recordMessages(final String topicName, final int count) {
    if (!enabled || count <= 0) {
      LOGGER.debug(
          "PartitionManager is disabled or count is zero, ignoring messages for topic: {}",
          topicName);
      return;
    }

    TopicPartitionInfo info =
        topicPartitionInfoMap.computeIfAbsent(
            topicName,
            k -> {
              LOGGER.info("PartitionManager: Tracking new topic: {}", topicName);
              int initialPartitionCount = getTopicPartitionCount(topicName);
              LOGGER.info(
                  "PartitionManager: Initial partition count for topic {}: {}",
                  topicName,
                  initialPartitionCount);
              return new TopicPartitionInfo(
                  initialPartitionCount, new AtomicLong(CONFIGURED_PARTITIONS_UNKNOWN));
            });

    long messageCount = info.messageCount.addAndGet(count);
    long threshold = messageThreshold;

    LOGGER.debug(
        "PartitionManager: Topic={}, MessageCount={}, Threshold={}, CurrentPartitions={}",
        topicName,
        messageCount,
        threshold,
        info.currentPartitions);

    // Calculate the expected threshold based on the number of partitions that should exist
    // for the current message count. This ensures partitions are added at regular intervals
    // (every 'threshold' messages) regardless of the initial partition count.
    int expectedPartitionCount = (int) (messageCount / threshold) + 1;
    if (expectedPartitionCount > info.currentPartitions && messageCount > 0) {
      LOGGER.info(
          "PartitionManager: Threshold reached for topic {} - Message count: {}, Current partitions: {}, Expected partitions: {}",
          topicName,
          messageCount,
          info.currentPartitions,
          expectedPartitionCount);
      checkAndAddPartition(topicName, info, expectedPartitionCount);
    }
  }

  private int getTopicPartitionCount(final String topicName) {
    try {
      org.apache.kafka.clients.admin.DescribeTopicsResult describeTopicsResult =
          adminClient.describeTopics(Collections.singleton(topicName));
      org.apache.kafka.clients.admin.TopicDescription topicDescription =
          describeTopicsResult.all().get().get(topicName);
      return topicDescription.partitions().size();
    } catch (Exception e) {
      LOGGER.error(
          "Failed to get partition count for topic {}: {}. Defaulting to 1.",
          topicName,
          e.getMessage());
      return 1;
    }
  }

  private void checkAndAddPartition(final String topicName, final TopicPartitionInfo info, final int expectedPartitionCount) {
    if (info.currentPartitions >= maxPartitions) {
      LOGGER.warn(
          "Topic {} has reached maximum partitions ({}). No more partitions will be added.",
          topicName,
          maxPartitions);
      return;
    }

    try {
      // Add partitions incrementally to reach the expected count
      int newPartitionCount = Math.min(expectedPartitionCount, maxPartitions);
      LOGGER.info(
          "Topic {} has reached {} messages. Increasing partitions from {} to {}",
          topicName,
          info.messageCount.get(),
          info.currentPartitions,
          newPartitionCount);

      NewPartitions newPartitions = NewPartitions.increaseTo(newPartitionCount);
      Map<String, NewPartitions> partitionsMap = Collections.singletonMap(topicName, newPartitions);

      try {
        adminClient.createPartitions(partitionsMap).all().get();
        info.currentPartitions = newPartitionCount;
        LOGGER.info(
            "Successfully increased partitions for topic {}. New partition count: {}",
            topicName,
            newPartitionCount);
      } catch (Exception e) {
        if (e.getCause() instanceof TopicAuthorizationException) {
          LOGGER.error(
              "Authorization error while adding partition to topic {}: {}",
              topicName,
              e.getCause().getMessage());
        } else {
          LOGGER.error("Failed to add partition to topic {}: {}", topicName, e.getMessage(), e);
        }
      }
    } catch (Exception e) {
      LOGGER.error(
          "Error initiating partition addition for topic {}: {}", topicName, e.getMessage(), e);
    }
  }

  public void updateTopicPartitionCount(final String topicName, final int partitionCount) {
    if (!enabled) {
      LOGGER.debug(
          "PartitionManager is disabled, skipping partition count update for topic: {}", topicName);
      return;
    }
    TopicPartitionInfo info = topicPartitionInfoMap.get(topicName);
    if (info != null) {
      int oldCount = info.currentPartitions;
      info.currentPartitions = partitionCount;
      LOGGER.info(
          "Updated partition count for topic {} from {} to {}",
          topicName,
          oldCount,
          partitionCount);
    } else {
      LOGGER.warn(
          "Topic {} not found in partition map during partition count update to {}",
          topicName,
          partitionCount);
    }
  }

  public void close() {
    if (adminClient != null) {
      try {
        adminClient.close();
        LOGGER.debug("Partition manager closed");
      } catch (Exception e) {
        LOGGER.error("Error closing partition manager: {}", e.getMessage(), e);
      }
    }
  }

  private static final int CONFIGURED_PARTITIONS_UNKNOWN = -1;

  private static class TopicPartitionInfo {
    int currentPartitions;
    final AtomicLong messageCount;

    TopicPartitionInfo(final int currentPartitions, final AtomicLong messageCount) {
      this.currentPartitions = currentPartitions;
      this.messageCount = messageCount;
    }
  }
}
