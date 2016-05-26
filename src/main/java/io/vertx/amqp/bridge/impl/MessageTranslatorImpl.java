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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;

import io.vertx.amqp.bridge.MessageHelper;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class MessageTranslatorImpl {

  private static final AmqpValue EMPTY_BODY_SECTION = new AmqpValue(null);

  public JsonObject convertToJsonObject(Message protonMessage) throws IllegalArgumentException {
    JsonObject jsonObject = new JsonObject();

    Section body = protonMessage.getBody();
    if (body instanceof AmqpValue) {
      Object value = translateToJsonCompatible(((AmqpValue) body).getValue());
      jsonObject.put(MessageHelper.BODY, value);
      jsonObject.put(MessageHelper.BODY_TYPE, MessageHelper.BODY_TYPE_VALUE);
    } else if (body instanceof Data) {
      Binary bin = ((Data) body).getValue();
      byte[] bytes = new byte[bin.getLength()];
      System.arraycopy(bin.getArray(), bin.getArrayOffset(), bytes, 0, bin.getLength());

      jsonObject.put(MessageHelper.BODY, bytes);
      jsonObject.put(MessageHelper.BODY_TYPE, MessageHelper.BODY_TYPE_DATA);
    } else if (body instanceof AmqpSequence) {
      JsonArray jsonSequence = (JsonArray) translateToJsonCompatible(((AmqpSequence) body).getValue());

      jsonObject.put(MessageHelper.BODY, jsonSequence);
      jsonObject.put(MessageHelper.BODY_TYPE, MessageHelper.BODY_TYPE_SEQUENCE);
    }

    Properties props = protonMessage.getProperties();
    if (props != null) {
      JsonObject jsonProps = createJsonProperties(props);
      jsonObject.put(MessageHelper.PROPERTIES, jsonProps);
    }

    Header header = protonMessage.getHeader();
    if (header != null) {
      JsonObject jsonHeader = createJsonHeader(header);
      jsonObject.put(MessageHelper.HEADER, jsonHeader);
    }

    ApplicationProperties appProps = protonMessage.getApplicationProperties();
    if (appProps != null && appProps.getValue() != null) {
      @SuppressWarnings("unchecked")
      JsonObject jsonAppProps = createJsonApplicationProperties(appProps.getValue());
      jsonObject.put(MessageHelper.APPLICATION_PROPERTIES, jsonAppProps);
    }

    MessageAnnotations msgAnn = protonMessage.getMessageAnnotations();
    if (msgAnn != null && msgAnn.getValue() != null) {
      JsonObject jsonMsgAnn = createJsonMessageAnnotations(msgAnn.getValue());
      jsonObject.put(MessageHelper.MESSAGE_ANNOTATIONS, jsonMsgAnn);
    }

    return jsonObject;
  }

  private JsonObject createJsonHeader(Header protonHeader) {
    JsonObject jsonHeader = new JsonObject();

    if (protonHeader.getDurable() != null) {
      jsonHeader.put(MessageHelper.HEADER_DURABLE, protonHeader.getDurable());
    }

    if (protonHeader.getPriority() != null) {
      jsonHeader.put(MessageHelper.HEADER_PRIORITY, protonHeader.getPriority().shortValue());
    }

    if (protonHeader.getTtl() != null) {
      jsonHeader.put(MessageHelper.HEADER_TTL, protonHeader.getTtl().longValue());
    }

    if (protonHeader.getFirstAcquirer() != null) {
      jsonHeader.put(MessageHelper.HEADER_FIRST_ACQUIRER, protonHeader.getFirstAcquirer());
    }

    if (protonHeader.getDeliveryCount() != null) {
      jsonHeader.put(MessageHelper.HEADER_DELIVERY_COUNT, protonHeader.getDeliveryCount().longValue());
    }

    return jsonHeader;
  }

  private JsonObject createJsonMessageAnnotations(Map<Symbol, Object> msgAnn) {
    JsonObject jsonMsgAnn = new JsonObject();

    for (Entry<Symbol, Object> entry : msgAnn.entrySet()) {
      Symbol key = entry.getKey();
      Object value = translateToJsonCompatible(entry.getValue());

      jsonMsgAnn.put(key.toString(), value);
    }

    return jsonMsgAnn;
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

    if (protonProps.getSubject() != null) {
      jsonProps.put(MessageHelper.PROPERTIES_SUBJECT, protonProps.getSubject());
    }

    if (protonProps.getGroupId() != null) {
      jsonProps.put(MessageHelper.PROPERTIES_GROUP_ID, protonProps.getGroupId());
    }

    if (protonProps.getGroupSequence() != null) {
      jsonProps.put(MessageHelper.PROPERTIES_GROUP_SEQUENCE, protonProps.getGroupSequence().longValue());
    }

    if (protonProps.getReplyToGroupId() != null) {
      jsonProps.put(MessageHelper.PROPERTIES_REPLY_TO_GROUP_ID, protonProps.getReplyToGroupId());
    }

    if (protonProps.getContentType() != null) {
      jsonProps.put(MessageHelper.PROPERTIES_CONTENT_TYPE, protonProps.getContentType().toString());
    }

    if (protonProps.getContentEncoding() != null) {
      jsonProps.put(MessageHelper.PROPERTIES_CONTENT_ENCODING, protonProps.getContentEncoding().toString());
    }

    if (protonProps.getCreationTime() != null) {
      jsonProps.put(MessageHelper.PROPERTIES_CREATION_TIME, protonProps.getCreationTime().getTime());
    }

    if (protonProps.getAbsoluteExpiryTime() != null) {
      jsonProps.put(MessageHelper.PROPERTIES_ABSOLUTE_EXPIRY_TIME, protonProps.getAbsoluteExpiryTime().getTime());
    }

    // TODO: user-id
    return jsonProps;
  }

  private JsonObject createJsonApplicationProperties(Map<String, Object> appProps) {
    JsonObject jsonAppProps = new JsonObject();

    for (Entry<String, Object> entry : appProps.entrySet()) {
      String key = entry.getKey();
      Object value = translateToJsonCompatible(entry.getValue());

      jsonAppProps.put(key, value);
    }

    return jsonAppProps;
  }

  private Object translateToJsonCompatible(Object value) {
    if (value instanceof Map) {
      JsonObject jsonObject = new JsonObject();

      for (Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
        String key = String.valueOf(entry.getKey());
        Object val = translateToJsonCompatible(entry.getValue());

        jsonObject.put(key, val);
      }

      value = jsonObject;
    } else if (value instanceof List) {
      JsonArray jsonArray = new JsonArray();

      for (Object entry : (List<?>) value) {
        Object val = translateToJsonCompatible(entry);

        jsonArray.add(val);
      }

      value = jsonArray;
    } else if (value instanceof Binary) {
      Binary bin = (Binary) value;
      byte[] bytes = new byte[bin.getLength()];
      System.arraycopy(bin.getArray(), bin.getArrayOffset(), bytes, 0, bin.getLength());
      value = bytes;
    } else if (value instanceof Date) {
      value = ((Date) value).getTime();
    }
    return value;
  }

  public Message convertToAmqpMessage(JsonObject jsonObject) throws IllegalArgumentException {
    Message protonMessage = Message.Factory.create();

    if (jsonObject.containsKey(MessageHelper.BODY)) {
      String bodyType = jsonObject.getString(MessageHelper.BODY_TYPE);
      if (bodyType == null || MessageHelper.BODY_TYPE_VALUE.equals(bodyType)) {
        Object value = translateToAmqpCompatible(jsonObject.getValue(MessageHelper.BODY));
        protonMessage.setBody(new AmqpValue(value));
      } else if (MessageHelper.BODY_TYPE_DATA.equals(bodyType)) {
        byte[] bytes = jsonObject.getBinary(MessageHelper.BODY);
        protonMessage.setBody(new Data(new Binary(bytes)));
      } else if (MessageHelper.BODY_TYPE_SEQUENCE.equals(bodyType)) {
        JsonArray jsonSequence = jsonObject.getJsonArray(MessageHelper.BODY);
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) translateToAmqpCompatible(jsonSequence);
        protonMessage.setBody(new AmqpSequence(list));
      }
    } else {
      // messages are meant to have a body section, set an 'empty' body (an amqp-value containing null).
      protonMessage.setBody(EMPTY_BODY_SECTION);
    }

    if (jsonObject.containsKey(MessageHelper.PROPERTIES)) {
      Properties props = createAmqpProperties(jsonObject.getJsonObject(MessageHelper.PROPERTIES));
      protonMessage.setProperties(props);
    }

    if (jsonObject.containsKey(MessageHelper.HEADER)) {
      Header header = createAmqpHeader(jsonObject.getJsonObject(MessageHelper.HEADER));
      protonMessage.setHeader(header);
    }

    if (jsonObject.containsKey(MessageHelper.APPLICATION_PROPERTIES)) {
      ApplicationProperties appProps = createAmqpApplicationProperties(
          jsonObject.getJsonObject(MessageHelper.APPLICATION_PROPERTIES));
      protonMessage.setApplicationProperties(appProps);
    }

    if (jsonObject.containsKey(MessageHelper.MESSAGE_ANNOTATIONS)) {
      MessageAnnotations msgAnn = createAmqpMessageAnnotations(
          jsonObject.getJsonObject(MessageHelper.MESSAGE_ANNOTATIONS));
      protonMessage.setMessageAnnotations(msgAnn);
    }

    return protonMessage;
  }

  private Header createAmqpHeader(JsonObject jsonHeader) {
    Header protonHeader = new Header();

    if (jsonHeader.containsKey(MessageHelper.HEADER_DURABLE)) {
      protonHeader.setDurable(jsonHeader.getBoolean(MessageHelper.HEADER_DURABLE));
    }

    if (jsonHeader.containsKey(MessageHelper.HEADER_PRIORITY)) {
      int priority = jsonHeader.getInteger(MessageHelper.HEADER_PRIORITY);
      protonHeader.setPriority(UnsignedByte.valueOf((byte) priority));
    }

    if (jsonHeader.containsKey(MessageHelper.HEADER_TTL)) {
      Long ttl = jsonHeader.getLong(MessageHelper.HEADER_TTL);
      protonHeader.setTtl(UnsignedInteger.valueOf(ttl));
    }

    if (jsonHeader.containsKey(MessageHelper.HEADER_FIRST_ACQUIRER)) {
      protonHeader.setFirstAcquirer(jsonHeader.getBoolean(MessageHelper.HEADER_FIRST_ACQUIRER));
    }

    if (jsonHeader.containsKey(MessageHelper.HEADER_DELIVERY_COUNT)) {
      Long dc = jsonHeader.getLong(MessageHelper.HEADER_DELIVERY_COUNT);
      protonHeader.setDeliveryCount(UnsignedInteger.valueOf(dc));
    }

    return protonHeader;
  }

  private MessageAnnotations createAmqpMessageAnnotations(JsonObject jsonMsgAnn) {
    Map<Symbol, Object> ann = new HashMap<>();
    MessageAnnotations protonMsgAnn = new MessageAnnotations(ann);

    Map<String, Object> underlying = jsonMsgAnn.getMap();

    for (Entry<String, Object> entry : underlying.entrySet()) {
      Object value = translateToAmqpCompatible(entry.getValue());
      ann.put(Symbol.valueOf(entry.getKey()), value);
    }

    return protonMsgAnn;
  }

  private ApplicationProperties createAmqpApplicationProperties(JsonObject jsonAppProps) {
    Map<String, Object> props = new HashMap<>();
    ApplicationProperties protonAppProps = new ApplicationProperties(props);

    Map<String, Object> underlying = jsonAppProps.getMap();

    for (Entry<String, Object> entry : underlying.entrySet()) {
      Object value = translateToAmqpCompatible(entry.getValue());
      props.put(entry.getKey(), value);
    }

    return protonAppProps;
  }

  private Object translateToAmqpCompatible(Object value) {
    if (value instanceof JsonObject) {
      Map<String, Object> map = new LinkedHashMap<>();

      for (Entry<String, Object> entry : ((JsonObject) value)) {
        Object val = translateToAmqpCompatible(entry.getValue());

        map.put(entry.getKey(), val);
      }

      value = map;
    } else if (value instanceof JsonArray) {
      List<Object> list = new ArrayList<>();

      for (Object entry : ((JsonArray) value)) {
        Object val = translateToAmqpCompatible(entry);

        list.add(val);
      }

      value = list;
    }

    return value;
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

    if (jsonProps.containsKey(MessageHelper.PROPERTIES_SUBJECT)) {
      proptonProps.setSubject(jsonProps.getString(MessageHelper.PROPERTIES_SUBJECT));
    }

    if (jsonProps.containsKey(MessageHelper.PROPERTIES_GROUP_ID)) {
      proptonProps.setGroupId(jsonProps.getString(MessageHelper.PROPERTIES_GROUP_ID));
    }

    if (jsonProps.containsKey(MessageHelper.PROPERTIES_GROUP_SEQUENCE)) {
      Long seq = jsonProps.getLong(MessageHelper.PROPERTIES_GROUP_SEQUENCE);
      proptonProps.setGroupSequence(UnsignedInteger.valueOf(seq));
    }

    if (jsonProps.containsKey(MessageHelper.PROPERTIES_REPLY_TO_GROUP_ID)) {
      proptonProps.setReplyToGroupId(jsonProps.getString(MessageHelper.PROPERTIES_REPLY_TO_GROUP_ID));
    }

    if (jsonProps.containsKey(MessageHelper.PROPERTIES_CONTENT_TYPE)) {
      String contentType = jsonProps.getString(MessageHelper.PROPERTIES_CONTENT_TYPE);
      proptonProps.setContentType(Symbol.valueOf(contentType));
    }

    if (jsonProps.containsKey(MessageHelper.PROPERTIES_CONTENT_ENCODING)) {
      String contentEncoding = jsonProps.getString(MessageHelper.PROPERTIES_CONTENT_ENCODING);
      proptonProps.setContentEncoding(Symbol.valueOf(contentEncoding));
    }

    if (jsonProps.containsKey(MessageHelper.PROPERTIES_CREATION_TIME)) {
      Long creationTime = jsonProps.getLong(MessageHelper.PROPERTIES_CREATION_TIME);
      proptonProps.setCreationTime(new Date(creationTime));
    }

    if (jsonProps.containsKey(MessageHelper.PROPERTIES_ABSOLUTE_EXPIRY_TIME)) {
      Long expiryTime = jsonProps.getLong(MessageHelper.PROPERTIES_ABSOLUTE_EXPIRY_TIME);
      proptonProps.setAbsoluteExpiryTime(new Date(expiryTime));
    }

    // TODO: user-id
    return proptonProps;
  }
}