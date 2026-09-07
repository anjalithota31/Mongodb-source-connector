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

package com.tyss.mongodb.kafka.connect.source.notification;

import static java.lang.String.format;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmailNotificationService {
  private static final Logger LOGGER = LoggerFactory.getLogger(EmailNotificationService.class);
  private static final DateTimeFormatter DATE_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  private final String fromEmail;
  private final List<String> toEmails;
  private final String connectorName;
  private final boolean enabled;
  private final String smtpHost;
  private final int smtpPort;
  private final String smtpUsername;
  private final String smtpPassword;
  private final boolean smtpSsl;
  private final boolean smtpTls;

  public EmailNotificationService(
      final String connectorName,
      final String fromEmail,
      final String toEmails,
      final String smtpHost,
      final int smtpPort,
      final String smtpUsername,
      final String smtpPassword,
      final boolean smtpSsl,
      final boolean smtpTls,
      final boolean enabled) {
    this.connectorName = connectorName;
    this.fromEmail = fromEmail;
    this.toEmails = parseEmails(toEmails);
    this.smtpHost = smtpHost;
    this.smtpPort = smtpPort;
    this.smtpUsername = smtpUsername;
    this.smtpPassword = smtpPassword;
    this.smtpSsl = smtpSsl;
    this.smtpTls = smtpTls;
    this.enabled = enabled;

    if (enabled && !this.toEmails.isEmpty()) {
      LOGGER.info("Email notification service initialized for connector: {}", connectorName);
    } else {
      LOGGER.info("Email notification service disabled for connector: {}", connectorName);
    }
  }

  public void sendFailureNotification(
      final String failureType,
      final String status,
      final String exceptionType,
      final String message,
      final String fullExceptionChain) {
    if (!enabled || toEmails.isEmpty()) {
      LOGGER.debug("Email notification disabled or not configured");
      return;
    }

    try {
      Properties props = new Properties();
      props.put("mail.smtp.host", smtpHost);
      props.put("mail.smtp.port", smtpPort);
      props.put("mail.smtp.auth", "true");

      if (smtpSsl) {
        props.put("mail.smtp.ssl.enable", "true");
        props.put("mail.smtp.ssl.checkserveridentity", "false");
        props.put("mail.smtp.ssl.trust", smtpHost);
      } else if (smtpTls) {
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.starttls.required", "true");
      }

      Session session =
          Session.getInstance(
              props,
              new javax.mail.Authenticator() {
                @Override
                protected javax.mail.PasswordAuthentication getPasswordAuthentication() {
                  return new javax.mail.PasswordAuthentication(smtpUsername, smtpPassword);
                }
              });

      Message msg = new MimeMessage(session);
      msg.setFrom(new InternetAddress(fromEmail));
      InternetAddress[] toAddresses = new InternetAddress[toEmails.size()];
      for (int i = 0; i < toEmails.size(); i++) {
        toAddresses[i] = new InternetAddress(toEmails.get(i));
      }
      msg.setRecipients(Message.RecipientType.TO, toAddresses);
      msg.setSubject(format("MongoDB Source Connector Alert: %s", connectorName));
      msg.setText(buildEmailBody(failureType, status, exceptionType, message, fullExceptionChain));

      Transport transport = session.getTransport("smtp");
      try {
        transport.connect(smtpUsername, smtpPassword);
        transport.sendMessage(msg, msg.getAllRecipients());
        LOGGER.info("Failure notification email sent successfully to: {}", toEmails);
      } finally {
        transport.close();
      }
    } catch (MessagingException e) {
      LOGGER.error("Failed to send email notification via SMTP: {}", e.getMessage(), e);
    } catch (Exception e) {
      LOGGER.error("Unexpected error sending email notification: {}", e.getMessage(), e);
    }
  }

  private String buildEmailBody(
      final String failureType,
      final String status,
      final String exceptionType,
      final String message,
      final String fullExceptionChain) {
    String timestamp = LocalDateTime.now().format(DATE_FORMATTER);

    return format(
        "MongoDB Source Connector Connectivity Alert"
            + "\n"
            + "==================================================\n"
            + "\n"
            + "Connector Name  : %s\n"
            + "Status          : %s\n"
            + "Time            : %s\n"
            + "\n"
            + "Failure Classification\n"
            + "--------------------------------------------------\n"
            + "Failure Type    : %s\n"
            + "\n"
            + "Root Cause\n"
            + "--------------------------------------------------\n"
            + "Exception Type  : %s\n"
            + "Message         : %s\n"
            + "\n"
            + "Full Exception Chain\n"
            + "--------------------------------------------------\n"
            + "%s\n"
            + "\n"
            + "Automatic Action Taken\n"
            + "--------------------------------------------------\n"
            + "â€¢ Connector task has been stopped.\n"
            + "â€¢ Check logs for detailed error information.\n"
            + "â€¢ Connector will need to be restarted manually.\n"
            + "\n"
            + "Recommended Checks\n"
            + "--------------------------------------------------\n"
            + "â€¢ Verify MongoDB cluster health and connectivity.\n"
            + "â€¢ Check if MongoDB is running and the connection.uri config is correct.\n"
            + "â€¢ Verify network connectivity and firewall rules.\n"
            + "â€¢ Check MongoDB authentication credentials.\n"
            + "â€¢ Review MongoDB change stream configuration.\n"
            + "\n"
            + "This is an automated notification from the MongoDB Source Connector.",
        connectorName, status, timestamp, failureType, exceptionType, message, fullExceptionChain);
  }

  private List<String> parseEmails(final String emailsString) {
    if (emailsString == null || emailsString.trim().isEmpty()) {
      return Collections.emptyList();
    }
    return Arrays.stream(emailsString.split(","))
        .map(String::trim)
        .filter(email -> !email.isEmpty())
        .collect(Collectors.toList());
  }

  public void close() {
    LOGGER.debug("Email notification service closed");
  }
}
