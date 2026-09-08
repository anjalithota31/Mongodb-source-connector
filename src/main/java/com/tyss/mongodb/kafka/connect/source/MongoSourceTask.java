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
package com.tyss.mongodb.kafka.connect.source;

import static com.tyss.mongodb.kafka.connect.source.MongoSourceConfig.COLLECTION_CONFIG;
import static com.tyss.mongodb.kafka.connect.source.MongoSourceConfig.DATABASE_CONFIG;
import static com.tyss.mongodb.kafka.connect.source.MongoSourceConfig.PROVIDER_CONFIG;
import static com.tyss.mongodb.kafka.connect.util.Assertions.assertNotNull;
import static com.tyss.mongodb.kafka.connect.util.ConfigHelper.getMongoDriverInformation;
import static com.tyss.mongodb.kafka.connect.util.ServerApiConfig.setServerApi;
import static com.tyss.mongodb.kafka.connect.util.SslConfigs.setupSsl;
import static java.util.Collections.singletonMap;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandSucceededEvent;

import com.tyss.mongodb.kafka.connect.Versions;
import com.tyss.mongodb.kafka.connect.source.MongoSourceConfig.StartupConfig.StartupMode;
import com.tyss.mongodb.kafka.connect.source.notification.EmailNotificationService;
import com.tyss.mongodb.kafka.connect.source.partition.PartitionManager;
import com.tyss.mongodb.kafka.connect.source.statistics.JmxStatisticsManager;
import com.tyss.mongodb.kafka.connect.source.statistics.StatisticsManager;
import com.tyss.mongodb.kafka.connect.util.ResumeTokenUtils;
import com.tyss.mongodb.kafka.connect.util.VisibleForTesting;
import com.tyss.mongodb.kafka.connect.util.jmx.SourceTaskStatistics;

/**
 * A Kafka Connect source task that uses change streams to broadcast changes to the collection,
 * database or client.
 *
 * <h2>Copy Existing Data</h2>
 *
 * <p>If configured the connector will copy the existing data from the collection, database or
 * client. All namespaces that exist at the time of starting the task will be broadcast onto the
 * topic as insert operations. Only when all the data from all namespaces have been broadcast will
 * the change stream cursor start broadcasting new changes. The logic for copying existing data is
 * as follows:
 *
 * <ol>
 *   <li>Get the latest resumeToken from MongoDB
 *   <li>Create insert events for all configured namespaces using multiple threads. This step is
 *       completed only after <em>all</em> collections are successfully copied.
 *   <li>Start a change stream cursor from the saved resumeToken
 * </ol>
 *
 * <p>It should be noted that the reading of all the data during the copy and then the subsequent
 * change stream events may produce duplicated events. During the copy, clients can make changes to
 * the data in MongoDB, which may be represented both by the copying process and the change stream.
 * However, as the change stream events are idempotent the changes can be applied so that the data
 * is eventually consistent.
 *
 * <p>It should also be noted renaming a collection during the copying process is not supported.
 *
 * <h3>Restarts</h3>
 *
 * Restarting the connector during the copying phase, will cause the whole copy process to restart.
 * Restarts after the copying process will resume from the last seen resumeToken.
 */
public final class MongoSourceTask extends SourceTask {
  static final Logger LOGGER = LoggerFactory.getLogger(MongoSourceTask.class);
  private static final String CONNECTOR_TYPE = "source";
  public static final String ID_FIELD = "_id";
  public static final String DOCUMENT_KEY_FIELD = "documentKey";
  static final String COPY_KEY = "copy";
  private static final String NS_KEY = "ns";

  private StartedMongoSourceTask startedTask;
  private EmailNotificationService emailNotificationService;
  private PartitionManager partitionManager;

  @Override
  public String version() {
    return Versions.VERSION;
  }

  @SuppressWarnings("try")
  @Override
  public void start(final Map<String, String> props) {
    LOGGER.info("Starting MongoDB source task");
    LOGGER.info("Startup configuration properties count: {}", props.size());
    StatisticsManager statisticsManager = null;
    MongoClient mongoClient = null;
    MongoCopyDataManager copyDataManager = null;
    try {
      LOGGER.info("Step 1: Creating source configuration");
      MongoSourceConfig sourceConfig = new MongoSourceConfig(props);
      LOGGER.info("Source configuration created successfully");
      LOGGER.info("Step 2: Determining if data should be copied");
      boolean shouldCopyData = shouldCopyData(context, sourceConfig);
      LOGGER.info("Should copy data: {}", shouldCopyData);
      String connectorName = JmxStatisticsManager.getConnectorName(props);
      LOGGER.info("Connector name: {}", connectorName);
      LOGGER.info("Step 3: Initializing statistics manager");
      statisticsManager = new JmxStatisticsManager(shouldCopyData, connectorName);
      LOGGER.info("Statistics manager initialized successfully");
      StatisticsManager statsManager = statisticsManager;
      CommandListener statisticsCommandListener =
          new CommandListener() {
            @Override
            public void commandSucceeded(final CommandSucceededEvent event) {
              mongoCommandSucceeded(event, statsManager.currentStatistics());
            }

            @Override
            public void commandFailed(final CommandFailedEvent event) {
              mongoCommandFailed(event, statsManager.currentStatistics());
            }
          };

      MongoClientSettings.Builder builder =
          MongoClientSettings.builder()
              .applyConnectionString(sourceConfig.getConnectionString())
              .addCommandListener(statisticsCommandListener)
              .applyToSslSettings(sslBuilder -> setupSsl(sslBuilder, sourceConfig));

      if (sourceConfig.getCustomCredentialProvider() != null) {
        builder.credential(
            sourceConfig
                .getCustomCredentialProvider()
                .getCustomCredential(sourceConfig.originals()));
      }

      setServerApi(builder, sourceConfig);
      LOGGER.info(
          "Step 4: Creating MongoDB client with connection string: {}",
          maskPassword(sourceConfig.getConnectionString().toString()));
      mongoClient =
          MongoClients.create(
              builder.build(),
              getMongoDriverInformation(CONNECTOR_TYPE, sourceConfig.getString(PROVIDER_CONFIG)));
      LOGGER.info("MongoDB client created successfully");
      LOGGER.info("Step 5: Creating copy data manager");
      copyDataManager = shouldCopyData ? new MongoCopyDataManager(sourceConfig, mongoClient) : null;
      LOGGER.info("Copy data manager created: {}", copyDataManager != null);

      // Initialize email notification service before creating StartedMongoSourceTask
      LOGGER.info("Step 6: Checking email notification configuration");
      if (sourceConfig.isEmailNotificationEnabled()) {
        LOGGER.info("Email notification is enabled, initializing service");
        emailNotificationService =
            new EmailNotificationService(
                connectorName,
                sourceConfig.getEmailFrom(),
                sourceConfig.getEmailTo(),
                sourceConfig.getEmailSmtpHost(),
                sourceConfig.getEmailSmtpPort(),
                sourceConfig.getEmailSmtpUsername(),
                sourceConfig.getEmailSmtpPassword(),
                sourceConfig.getEmailSmtpSsl(),
                sourceConfig.getEmailSmtpTls(),
                true);
        LOGGER.info("Email notification service initialized successfully");
      } else {
        LOGGER.info("Email notification is disabled");
      }

      // Initialize partition manager for automatic partition rotation
      LOGGER.info("Step 7: Checking partition rotation configuration");
      if (sourceConfig.isPartitionRotationEnabled()) {
        LOGGER.info("Partition rotation is enabled, initializing partition manager");
        try {
          Properties adminProps = new Properties();
          adminProps.putAll(props);
          AdminClient adminClient = AdminClient.create(adminProps);
          LOGGER.info("Kafka AdminClient created successfully for partition management");
          partitionManager =
              new PartitionManager(
                  adminClient,
                  sourceConfig.getPartitionRotationThreshold(),
                  sourceConfig.getPartitionRotationMaxPartitions(),
                  true);
          LOGGER.info(
              "Partition manager initialized successfully for connector: {}", connectorName);
        } catch (Exception e) {
          LOGGER.error("Failed to initialize partition manager: {}", e.getMessage(), e);
        }
      } else {
        LOGGER.info("Partition rotation is disabled");
      }

      LOGGER.info("Step 8: Creating StartedMongoSourceTask");
      startedTask =
          new StartedMongoSourceTask(
              // It is safer to read the `context` reference each time we need it
              // in case it changes, because there is no
              // documentation stating that it cannot be changed.
              () -> context,
              sourceConfig,
              mongoClient,
              copyDataManager,
              statisticsManager,
              emailNotificationService,
              partitionManager);
      LOGGER.info("StartedMongoSourceTask created successfully");
    } catch (RuntimeException taskStartingException) {
      //noinspection EmptyTryBlock
      try (StatisticsManager autoCloseableStatisticsManager = statisticsManager;
          MongoClient autoCloseableMongoClient = mongoClient;
          MongoCopyDataManager autoCloseableCopyDataManager = copyDataManager) {
        // just using try-with-resources to ensure they all get closed, even in the case of
        // exceptions
      } catch (RuntimeException resourceReleasingException) {
        taskStartingException.addSuppressed(resourceReleasingException);
      }

      // Enhanced error logging for failure diagnosis
      LOGGER.error("========================================");
      LOGGER.error("MongoDB Source Task Startup Failure");
      LOGGER.error("========================================");
      LOGGER.error("Exception Type: {}", taskStartingException.getClass().getName());
      LOGGER.error("Exception Message: {}", taskStartingException.getMessage());
      LOGGER.error("Full Stack Trace:", taskStartingException);

      // Log connection details for debugging
      try {
        MongoSourceConfig sourceConfig = new MongoSourceConfig(props);
        LOGGER.error(
            "Connection URI: {}", maskPassword(sourceConfig.getConnectionString().toString()));
        LOGGER.error("Database: {}", sourceConfig.getString(DATABASE_CONFIG));
        LOGGER.error("Collection: {}", sourceConfig.getString(COLLECTION_CONFIG));
      } catch (Exception configException) {
        LOGGER.error("Failed to log configuration details: {}", configException.getMessage());
      }

      // Log suppressed exceptions if any
      if (taskStartingException.getSuppressed().length > 0) {
        LOGGER.error("Suppressed Exceptions:");
        for (Throwable suppressed : taskStartingException.getSuppressed()) {
          LOGGER.error("  - {}: {}", suppressed.getClass().getName(), suppressed.getMessage());
        }
      }
      LOGGER.error("========================================");

      // Send email notification on startup failure
      if (emailNotificationService != null) {
        String exceptionChain = buildExceptionChain(taskStartingException);
        emailNotificationService.sendFailureNotification(
            "STARTUP_FAILURE",
            "UNAVAILABLE",
            taskStartingException.getClass().getName(),
            taskStartingException.getMessage(),
            exceptionChain);
      }
      throw new ConnectException(
          "Failed to start MongoDB source task: " + taskStartingException.getMessage(),
          taskStartingException);
    }
    LOGGER.info("Started MongoDB source task");
  }

  @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
  StartedMongoSourceTask startedTask() {
    return assertNotNull(startedTask);
  }

  @Override
  public List<SourceRecord> poll() {
    return startedTask.poll();
  }

  @Override
  public void stop() {
    LOGGER.info("Stopping MongoDB source task");
    if (startedTask != null) {
      startedTask.close();
    }
    if (emailNotificationService != null) {
      emailNotificationService.close();
    }
    if (partitionManager != null) {
      partitionManager.close();
    }
  }

  @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
  static Map<String, Object> createPartitionMap(final MongoSourceConfig sourceConfig) {
    String partitionName = sourceConfig.getString(MongoSourceConfig.OFFSET_PARTITION_NAME_CONFIG);
    if (partitionName.isEmpty()) {
      partitionName = createDefaultPartitionName(sourceConfig);
    }
    return singletonMap(NS_KEY, partitionName);
  }

  @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
  static String createDefaultPartitionName(final MongoSourceConfig sourceConfig) {
    ConnectionString connectionString = sourceConfig.getConnectionString();
    StringBuilder builder = new StringBuilder();
    builder.append(connectionString.isSrvProtocol() ? "mongodb+srv://" : "mongodb://");
    builder.append(String.join(",", connectionString.getHosts()));
    builder.append("/");
    builder.append(sourceConfig.getString(DATABASE_CONFIG));
    if (!sourceConfig.getString(COLLECTION_CONFIG).isEmpty()) {
      builder.append(".");
      builder.append(sourceConfig.getString(COLLECTION_CONFIG));
    }
    return builder.toString();
  }

  /**
   * Checks to see if data should be copied.
   *
   * <p>Copying data is only required if it's been configured and it hasn't already completed.
   *
   * @return true if should copy the existing data.
   */
  private static boolean shouldCopyData(
      final SourceTaskContext context, final MongoSourceConfig sourceConfig) {
    Map<String, Object> offset = getOffset(context, sourceConfig);
    return sourceConfig.getStartupConfig().startupMode() == StartupMode.COPY_EXISTING
        && (offset == null || offset.containsKey(COPY_KEY));
  }

  @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
  static Map<String, Object> getOffset(
      final SourceTaskContext context, final MongoSourceConfig sourceConfig) {
    if (context != null) {
      return context.offsetStorageReader().offset(createPartitionMap(sourceConfig));
    }
    return null;
  }

  @Override
  public void commitRecord(final SourceRecord record, final RecordMetadata metadata) {
    startedTask.commitRecord(record, metadata);
  }

  @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
  static void mongoCommandSucceeded(
      final CommandSucceededEvent event, final SourceTaskStatistics currentStatistics) {
    String commandName = event.getCommandName();
    long elapsedTimeMs = event.getElapsedTime(TimeUnit.MILLISECONDS);
    if ("getMore".equals(commandName)) {
      currentStatistics.getGetmoreCommandsSuccessful().sample(elapsedTimeMs);
    } else if ("aggregate".equals(commandName) || "find".equals(commandName)) {
      currentStatistics.getInitialCommandsSuccessful().sample(elapsedTimeMs);
    }
    ResumeTokenUtils.getResponseOffsetSecs(event.getResponse())
        .ifPresent(offset -> currentStatistics.getLatestMongodbTimeDifferenceSecs().sample(offset));
  }

  @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
  static void mongoCommandFailed(
      final CommandFailedEvent event, final SourceTaskStatistics currentStatistics) {
    String commandName = event.getCommandName();
    long elapsedTimeMs = event.getElapsedTime(TimeUnit.MILLISECONDS);
    if ("getMore".equals(commandName)) {
      currentStatistics.getGetmoreCommandsFailed().sample(elapsedTimeMs);
    } else if ("aggregate".equals(commandName) || "find".equals(commandName)) {
      currentStatistics.getInitialCommandsFailed().sample(elapsedTimeMs);
    }
  }

  private String buildExceptionChain(final Throwable exception) {
    StringBuilder chain = new StringBuilder();
    Throwable current = exception;
    while (current != null) {
      chain
          .append(current.getClass().getName())
          .append(": ")
          .append(current.getMessage())
          .append("\n");
      if (current.getCause() != null && current.getCause() != current) {
        chain.append(" Caused by: ");
      }
      current = current.getCause();
    }
    return chain.toString();
  }

  private String maskPassword(final String connectionString) {
    if (connectionString == null) {
      return "null";
    }
    // Mask password in connection string for security
    return connectionString.replaceAll("://[^:]+:[^@]+@", "://****:****@");
  }
}
