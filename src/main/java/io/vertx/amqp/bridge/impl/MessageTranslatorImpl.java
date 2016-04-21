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
package io.vertx.amqp.bridge.impl;

import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;

import io.vertx.amqp.bridge.MessageHelper;
import io.vertx.core.json.JsonObject;

public class MessageTranslatorImpl {

  private static final AmqpValue EMPTY_BODY_SECTION = new AmqpValue(null);

  public JsonObject convertToJsonObject(Message protonMessage) throws IllegalArgumentException {
    JsonObject jsonObject = new JsonObject();

    Section body = protonMessage.getBody();
    // TODO: handle other body types
    if (body instanceof AmqpValue) {
      Object value = ((AmqpValue) body).getValue();
      // TODO: validate value, make any necessary conversions
      jsonObject.put(MessageHelper.BODY, value);
    }

    Properties props = protonMessage.getProperties();
    if (props != null) {
      JsonObject jsonProps = createJsonProperties(props);
      jsonObject.put(MessageHelper.PROPERTIES, jsonProps);
    }
    return jsonObject;
  }

  private JsonObject createJsonProperties(Properties protonProps) {
    JsonObject jsonProps = new JsonObject();

    if (protonProps.getTo() != null) {
      jsonProps.put(MessageHelper.PROPERTIES_TO, protonProps.getTo());
    }

    if (protonProps.getReplyTo() != null) {
      jsonProps.put(MessageHelper.PROPERTIES_REPLY_TO, protonProps.getReplyTo());
    }

    if (protonProps.getMessageId() != null) {
      // TODO: handle other types of id
      jsonProps.put(MessageHelper.PROPERTIES_MESSAGE_ID, protonProps.getMessageId().toString());
    }

    if (protonProps.getCorrelationId() != null) {
      // TODO: handle other types of id
      jsonProps.put(MessageHelper.PROPERTIES_CORRELATION_ID, protonProps.getCorrelationId().toString());
    }

    return jsonProps;
  }

  public Message convertToAmqpMessage(JsonObject jsonObject) throws IllegalArgumentException {
    Message protonMessage = Message.Factory.create();

    if (jsonObject.containsKey(MessageHelper.BODY)) {
      Object o = jsonObject.getValue(MessageHelper.BODY);
      // TODO: handle other body types
      protonMessage.setBody(new AmqpValue(o));
    } else {
      protonMessage.setBody(EMPTY_BODY_SECTION);
    }

    if (jsonObject.containsKey(MessageHelper.PROPERTIES)) {
      Properties props = createAmqpProperties(jsonObject.getJsonObject(MessageHelper.PROPERTIES));
      protonMessage.setProperties(props);
    }

    return protonMessage;
  }

  private Properties createAmqpProperties(JsonObject jsonProps) {
    Properties proptonProps = new Properties();

    if (jsonProps.containsKey(MessageHelper.PROPERTIES_TO)) {
      proptonProps.setTo(jsonProps.getString(MessageHelper.PROPERTIES_TO));
    }

    if (jsonProps.containsKey(MessageHelper.PROPERTIES_REPLY_TO)) {
      proptonProps.setReplyTo(jsonProps.getString(MessageHelper.PROPERTIES_REPLY_TO));
    }

    if (jsonProps.containsKey(MessageHelper.PROPERTIES_MESSAGE_ID)) {
      // TODO: handle other types of id
      proptonProps.setMessageId(jsonProps.getString(MessageHelper.PROPERTIES_MESSAGE_ID));
    }

    if (jsonProps.containsKey(MessageHelper.PROPERTIES_CORRELATION_ID)) {
      // TODO: handle other types of id
      proptonProps.setCorrelationId(jsonProps.getString(MessageHelper.PROPERTIES_CORRELATION_ID));
    }

    // TODO: handle other fields
    return proptonProps;
  }
}