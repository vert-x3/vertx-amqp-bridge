/*
* Copyright 2016 the original author or authors.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package io.vertx.amqp.bridge;

/**
 * Helper for dealing with the various sections, and their constituent elements, that form the
 * <a href="http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#section-message-format">AMQP
 * message</a>.
 */
public class MessageHelper {

  // sections
  public static final String HEADER = "header";
  public static final String BODY = "body";
  public static final String PROPERTIES = "properties";
  public static final String APPLICATION_PROPERTIES = "application_properties";

  // header section
  public static final String HEADER_DURABLE = "durable";
  public static final String HEADER_PRIORITY = "priority";
  public static final String HEADER_TTL = "ttl";
  public static final String HEADER_FIRST_ACQUIRER = "first_acquirer";
  public static final String HEADER_DELIVERY_COUNT = "delivery_count";

  // properties section
  public static final String PROPERTIES_TO = "to";
  public static final String PROPERTIES_REPLY_TO = "reply_to";
  public static final String PROPERTIES_MESSAGE_ID = "message_id";
  public static final String PROPERTIES_CORRELATION_ID = "correlation_id";
  public static final String PROPERTIES_SUBJECT = "subject";
  public static final String PROPERTIES_GROUP_ID = "group_id";
  public static final String PROPERTIES_GROUP_SEQUENCE = "group_sequence";
  public static final String PROPERTIES_REPLY_TO_GROUP_ID = "reply_to_group_id";
  public static final String PROPERTIES_CONTENT_TYPE = "content_type";
  public static final String PROPERTIES_CONTENT_ENCODING = "content_encoding";
  public static final String PROPERTIES_CREATION_TIME = "creation_time";
  public static final String PROPERTIES_ABSOLUTE_EXPIRY_TIME = "absolute_expiry_time";
}
