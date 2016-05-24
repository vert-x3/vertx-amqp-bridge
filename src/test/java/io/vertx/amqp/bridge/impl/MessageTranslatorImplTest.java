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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;
import org.junit.Before;
import org.junit.Test;

import io.vertx.amqp.bridge.MessageHelper;
import io.vertx.core.json.JsonObject;

public class MessageTranslatorImplTest {

  private MessageTranslatorImpl translator;

  @Before
  public void setUp() throws Exception {
    translator = new MessageTranslatorImpl();
  }

  // ============== header section ==============

  @Test
  public void testAMQP_to_JSON_WithNoHeaderSection() {
    Message protonMsg = Proton.message();

    JsonObject jsonObject = translator.convertToJsonObject(protonMsg);
    assertNotNull("expected converted msg", jsonObject);
    assertFalse("expected header element key not to be present", jsonObject.containsKey(MessageHelper.HEADER));
  }

  @Test
  public void testJSON_to_AMQP_WithNoHeaderSection() {
    JsonObject jsonObject = new JsonObject();

    Message protonMsg = translator.convertToAmqpMessage(jsonObject);
    assertNotNull("Expected converted msg", protonMsg);
    assertNull("expected converted msg to have no header section", protonMsg.getHeader());
  }

  @Test
  public void testAMQP_to_JSON_VerifyMessageHeader() {
    boolean testDurable = true;
    short testPriority = 8;
    long testTtl = 2345;
    boolean testFirstAcquirer = true;
    long testDeliveryCount = 3;

    Message protonMsg = Proton.message();
    protonMsg.setDurable(testDurable);
    protonMsg.setPriority(testPriority);
    protonMsg.setTtl(testTtl);
    protonMsg.setFirstAcquirer(testFirstAcquirer);
    protonMsg.setDeliveryCount(testDeliveryCount);

    JsonObject jsonObject = translator.convertToJsonObject(protonMsg);
    assertNotNull("expected converted msg", jsonObject);
    assertTrue("expected header element key to be present", jsonObject.containsKey(MessageHelper.HEADER));

    JsonObject jsonHeader = jsonObject.getJsonObject(MessageHelper.HEADER);
    assertNotNull("expected header element value to be non-null", jsonHeader);

    assertTrue("expected durable key to be present", jsonHeader.containsKey(MessageHelper.HEADER_DURABLE));
    assertEquals("expected durable value to be present", testDurable,
        jsonHeader.getValue(MessageHelper.HEADER_DURABLE));

    assertTrue("expected priority key to be present", jsonHeader.containsKey(MessageHelper.HEADER_PRIORITY));
    assertEquals("expected priority value to be present", testPriority,
        jsonHeader.getValue(MessageHelper.HEADER_PRIORITY));

    assertTrue("expected ttl key to be present", jsonHeader.containsKey(MessageHelper.HEADER_TTL));
    assertEquals("expected ttl value to be present", testTtl, jsonHeader.getValue(MessageHelper.HEADER_TTL));

    assertTrue("expected first acquirer key to be present",
        jsonHeader.containsKey(MessageHelper.HEADER_FIRST_ACQUIRER));
    assertEquals("expected first acquirer  value to be present", testFirstAcquirer,
        jsonHeader.getValue(MessageHelper.HEADER_FIRST_ACQUIRER));

    assertTrue("expected delivery count key to be present",
        jsonHeader.containsKey(MessageHelper.HEADER_DELIVERY_COUNT));
    assertEquals("expected delivery count value to be present", testDeliveryCount,
        jsonHeader.getValue(MessageHelper.HEADER_DELIVERY_COUNT));
  }

  @Test
  public void testJSON_to_AMQP_VerifyMessageHeader() {
    boolean testDurable = true;
    byte testPriority = 8;
    long testTtl = 2345;
    boolean testFirstAcquirer = true;
    long testDeliveryCount = 3;

    JsonObject jsonHeader = new JsonObject();
    jsonHeader.put(MessageHelper.HEADER_DURABLE, testDurable);
    jsonHeader.put(MessageHelper.HEADER_PRIORITY, testPriority);
    jsonHeader.put(MessageHelper.HEADER_TTL, testTtl);
    jsonHeader.put(MessageHelper.HEADER_FIRST_ACQUIRER, testFirstAcquirer);
    jsonHeader.put(MessageHelper.HEADER_DELIVERY_COUNT, testDeliveryCount);

    JsonObject jsonObject = new JsonObject();
    jsonObject.put(MessageHelper.HEADER, jsonHeader);

    Message protonMsg = translator.convertToAmqpMessage(jsonObject);
    assertNotNull("Expected converted msg", protonMsg);

    Header header = protonMsg.getHeader();
    assertNotNull("Header section not present", header);

    assertEquals("expected durable value to be present", testDurable, header.getDurable());
    assertEquals("expected priority value to be present", UnsignedByte.valueOf(testPriority), header.getPriority());
    assertEquals("expected ttl value to be present", UnsignedInteger.valueOf(testTtl), header.getTtl());
    assertEquals("expected first acquirer value to be present", testFirstAcquirer, header.getFirstAcquirer());
    assertEquals("expected delivery count value to be present", UnsignedInteger.valueOf(testDeliveryCount),
        header.getDeliveryCount());
  }

  // ============== application-properties section ==============

  @Test
  public void testAMQP_to_JSON_WithNoApplicationPropertiesSection() {
    Message protonMsg = Proton.message();

    JsonObject jsonObject = translator.convertToJsonObject(protonMsg);
    assertNotNull("expected converted msg", jsonObject);
    assertFalse("expected appliation properties element key not to be present",
        jsonObject.containsKey(MessageHelper.APPLICATION_PROPERTIES));
  }

  @Test
  public void testJSON_to_AMQP_WithNoApplicationPropertiesSection() {
    JsonObject jsonObject = new JsonObject();

    Message protonMsg = translator.convertToAmqpMessage(jsonObject);
    assertNotNull("Expected converted msg", protonMsg);
    assertNull("expected converted msg to have no application properties section",
        protonMsg.getApplicationProperties());
  }

  @Test
  public void testAMQP_to_JSON_VerifyMessageApplicationProperties() {

    Map<String, Object> props = new HashMap<>();
    ApplicationProperties appProps = new ApplicationProperties(props);

    String testPropKeyA = "testPropKeyA";
    String testPropValueA = "testPropValueA";
    String testPropKeyB = "testPropKeyB";
    String testPropValueB = "testPropValueB";

    props.put(testPropKeyA, testPropValueA);
    props.put(testPropKeyB, testPropValueB);

    Message protonMsg = Proton.message();
    protonMsg.setApplicationProperties(appProps);

    JsonObject jsonObject = translator.convertToJsonObject(protonMsg);
    assertNotNull("expected converted msg", jsonObject);
    assertTrue("expected application properties element key to be present",
        jsonObject.containsKey(MessageHelper.APPLICATION_PROPERTIES));

    JsonObject jsonAppProps = jsonObject.getJsonObject(MessageHelper.APPLICATION_PROPERTIES);
    assertNotNull("expected application properties element value to be non-null", jsonAppProps);

    assertTrue("expected key to be present", jsonAppProps.containsKey(testPropKeyB));
    assertEquals("expected value to be equal", testPropValueB, jsonAppProps.getValue(testPropKeyB));

    assertTrue("expected key to be present", jsonAppProps.containsKey(testPropKeyA));
    assertEquals("expected value to be equal", testPropValueA, jsonAppProps.getValue(testPropKeyA));
  }

  /**
   * Verifies that a Symbol application property is converted to a String [by the JsonObject]
   */
  @Test
  public void testAMQP_to_JSON_VerifyApplicationPropertySymbol() {
    Map<String, Object> props = new HashMap<>();
    ApplicationProperties appProps = new ApplicationProperties(props);

    String symbolPropKey = "symbolPropKey";
    Symbol symbolPropValue = Symbol.valueOf("symbolPropValue");

    props.put(symbolPropKey, symbolPropValue);

    Message protonMsg = Proton.message();
    protonMsg.setApplicationProperties(appProps);

    JsonObject jsonObject = translator.convertToJsonObject(protonMsg);
    assertNotNull("expected converted msg", jsonObject);
    assertTrue("expected application properties element key to be present",
        jsonObject.containsKey(MessageHelper.APPLICATION_PROPERTIES));

    JsonObject jsonAppProps = jsonObject.getJsonObject(MessageHelper.APPLICATION_PROPERTIES);
    assertNotNull("expected application properties element value to be non-null", jsonAppProps);

    assertTrue("expected key to be present", jsonAppProps.containsKey(symbolPropKey));
    assertEquals("expected value to be equal, as a string", symbolPropValue.toString(), jsonAppProps.getValue(symbolPropKey));
  }

  /**
   * Verifies that an incoming Binary AMQP application property is converted into an encoded string [by combination of
   * the translator and JsonObject itself]
   */
  @Test
  public void testAMQP_to_JSON_VerifyApplicationPropertyBinary() {
    Map<String, Object> props = new HashMap<>();
    ApplicationProperties appProps = new ApplicationProperties(props);

    String binaryPropKey = "binaryPropKey";
    String binaryPropValueSource = "binaryPropValueSource";
    Binary bin = new Binary(binaryPropValueSource.getBytes(StandardCharsets.UTF_8));

    props.put(binaryPropKey, bin);

    Message protonMsg = Proton.message();
    protonMsg.setApplicationProperties(appProps);

    JsonObject jsonObject = translator.convertToJsonObject(protonMsg);
    assertNotNull("expected converted msg", jsonObject);
    assertTrue("expected application properties element key to be present",
        jsonObject.containsKey(MessageHelper.APPLICATION_PROPERTIES));

    JsonObject jsonAppProps = jsonObject.getJsonObject(MessageHelper.APPLICATION_PROPERTIES);
    assertNotNull("expected application properties element value to be non-null", jsonAppProps);

    assertTrue("expected key to be present", jsonAppProps.containsKey(binaryPropKey));

    Map<String, Object> propsMap = jsonAppProps.getMap();
    assertTrue("expected value to be present, as encoded string", propsMap.containsKey(binaryPropKey));
    assertTrue("expected key to be present", jsonAppProps.getValue(binaryPropKey) instanceof String);
    assertArrayEquals("unepected decoded bytes", binaryPropValueSource.getBytes(StandardCharsets.UTF_8),
        jsonAppProps.getBinary(binaryPropKey));
  }

  @Test
  public void testJSON_to_AMQP_VerifyMessageApplicationProperties() {

    String testPropKeyA = "testPropKeyA";
    String testPropValueA = "testPropValueA";
    String testPropKeyB = "testPropKeyB";
    String testPropValueB = "testPropValueB";

    JsonObject jsonAppProps = new JsonObject();

    jsonAppProps.put(testPropKeyA, testPropValueA);
    jsonAppProps.put(testPropKeyB, testPropValueB);

    JsonObject jsonObject = new JsonObject();
    jsonObject.put(MessageHelper.APPLICATION_PROPERTIES, jsonAppProps);

    Message protonMsg = translator.convertToAmqpMessage(jsonObject);
    assertNotNull("Expected converted msg", protonMsg);

    ApplicationProperties appProps = protonMsg.getApplicationProperties();
    assertNotNull("Application properties section not present", appProps);

    @SuppressWarnings("unchecked")
    Map<String, Object> props = appProps.getValue();
    assertNotNull("Application properties map not present", appProps);

    assertTrue("expected key to be present", props.containsKey(testPropKeyA));
    assertEquals("expected value to be equal", testPropValueA, props.get(testPropKeyA));

    assertTrue("expected key to be present", props.containsKey(testPropKeyB));
    assertEquals("expected value to be equal", testPropValueB, props.get(testPropKeyB));

    assertEquals("unexpected number of props", 2, props.size());
  }

  // ============== body section ==============

  @Test
  public void testAMQP_to_JSON_VerifyBodyWithAmqpValueString() {
    String testContent = "myTestContent";
    Message protonMsg = Proton.message();
    protonMsg.setBody(new AmqpValue(testContent));

    JsonObject jsonObject = translator.convertToJsonObject(protonMsg);
    assertNotNull("expected converted msg", jsonObject);
    assertTrue("expected body element key to be present", jsonObject.containsKey(MessageHelper.BODY));
    assertNotNull("expected body element value to be non-null", jsonObject.getValue(MessageHelper.BODY));
    assertEquals("body value not as expected", testContent, jsonObject.getValue(MessageHelper.BODY));
  }

  @Test
  public void testJSON_to_AMQP_VerifyStringBody() {
    String testContent = "myTestContent";

    JsonObject jsonObject = new JsonObject();
    jsonObject.put(MessageHelper.BODY, testContent);

    Message protonMsg = translator.convertToAmqpMessage(jsonObject);

    assertNotNull("Expected converted msg", protonMsg);
    Section body = protonMsg.getBody();
    assertTrue("Unexpected body type", body instanceof AmqpValue);
    assertEquals("Unexpected message body value", testContent, ((AmqpValue) body).getValue());
  }

  // ============== properties section ==============

  @Test
  public void testAMQP_to_JSON_WithNoPropertiesSection() {
    Message protonMsg = Proton.message();

    JsonObject jsonObject = translator.convertToJsonObject(protonMsg);
    assertNotNull("expected converted msg", jsonObject);
    assertFalse("expected properties element key not to be present", jsonObject.containsKey(MessageHelper.PROPERTIES));
  }

  @Test
  public void testJSON_to_AMQP_WithNoPropertiesSection() {
    JsonObject jsonObject = new JsonObject();

    Message protonMsg = translator.convertToAmqpMessage(jsonObject);
    assertNotNull("Expected converted msg", protonMsg);
    assertNull("expected converted msg to have no properties section", protonMsg.getProperties());
  }

  @Test
  public void testAMQP_to_JSON_VerifyMessageProperties() {
    String testToAddress = "myToAddress";
    String testReplyToAddress = "myReplyToAddress";
    String testMessageId = "myTestMessageId";
    String testCorrelationId = "myTestCorrelationId";
    String testSubject = "myTestSubject";
    String testGroupId = "myTestGroupId";
    long testGroupSeq = 4567;
    String testReplyToGroupId = "myReplyToGroupId";
    String testContentType = "myContentType";
    String testContentEncoding = "myContentEncoding";
    long testCreationTime = System.currentTimeMillis();
    long testAbsExpiryTime = testCreationTime + 3456;

    Message protonMsg = Proton.message();
    protonMsg.setAddress(testToAddress);
    protonMsg.setReplyTo(testReplyToAddress);
    protonMsg.setMessageId(testMessageId);
    protonMsg.setCorrelationId(testCorrelationId);
    protonMsg.setSubject(testSubject);
    protonMsg.setGroupId(testGroupId);
    protonMsg.setGroupSequence(testGroupSeq);
    protonMsg.setReplyToGroupId(testReplyToGroupId);
    protonMsg.setContentType(testContentType);
    protonMsg.setContentEncoding(testContentEncoding);
    protonMsg.setCreationTime(testCreationTime);
    protonMsg.setExpiryTime(testAbsExpiryTime);

    JsonObject jsonObject = translator.convertToJsonObject(protonMsg);
    assertNotNull("expected converted msg", jsonObject);
    assertTrue("expected properties element key to be present", jsonObject.containsKey(MessageHelper.PROPERTIES));

    JsonObject properties = jsonObject.getJsonObject(MessageHelper.PROPERTIES);
    assertNotNull("expected properties element value to be non-null", properties);

    assertTrue("expected to key to be present", properties.containsKey(MessageHelper.PROPERTIES_TO));
    assertEquals("expected to value to be present", testToAddress, properties.getValue(MessageHelper.PROPERTIES_TO));

    assertTrue("expected reply to key to be present", properties.containsKey(MessageHelper.PROPERTIES_REPLY_TO));
    assertEquals("expected reply to value to be present", testReplyToAddress,
        properties.getValue(MessageHelper.PROPERTIES_REPLY_TO));

    assertTrue("expected message id key to be present", properties.containsKey(MessageHelper.PROPERTIES_MESSAGE_ID));
    assertEquals("expected message id value to be present", testMessageId,
        properties.getValue(MessageHelper.PROPERTIES_MESSAGE_ID));

    assertTrue("expected correlation id key to be present",
        properties.containsKey(MessageHelper.PROPERTIES_CORRELATION_ID));
    assertEquals("expected correlation id value to be present", testCorrelationId,
        properties.getValue(MessageHelper.PROPERTIES_CORRELATION_ID));

    assertTrue("expected subject key to be present", properties.containsKey(MessageHelper.PROPERTIES_SUBJECT));
    assertEquals("expected subject value to be present", testSubject,
        properties.getValue(MessageHelper.PROPERTIES_SUBJECT));

    assertTrue("expected group id key to be present", properties.containsKey(MessageHelper.PROPERTIES_GROUP_ID));
    assertEquals("expected group id value to be present", testGroupId,
        properties.getValue(MessageHelper.PROPERTIES_GROUP_ID));

    assertTrue("expected group sequence key to be present",
        properties.containsKey(MessageHelper.PROPERTIES_GROUP_SEQUENCE));
    assertEquals("expected group sequence value to be present", testGroupSeq,
        properties.getValue(MessageHelper.PROPERTIES_GROUP_SEQUENCE));

    assertTrue("expected reply to group id key to be present",
        properties.containsKey(MessageHelper.PROPERTIES_REPLY_TO_GROUP_ID));
    assertEquals("expected reply to group id value to be present", testReplyToGroupId,
        properties.getValue(MessageHelper.PROPERTIES_REPLY_TO_GROUP_ID));

    assertTrue("expected content type key to be present",
        properties.containsKey(MessageHelper.PROPERTIES_CONTENT_TYPE));
    assertEquals("expected content type  value to be present", testContentType,
        properties.getValue(MessageHelper.PROPERTIES_CONTENT_TYPE));

    assertTrue("expected content encoding key to be present",
        properties.containsKey(MessageHelper.PROPERTIES_CONTENT_ENCODING));
    assertEquals("expected content encoding  value to be present", testContentEncoding,
        properties.getValue(MessageHelper.PROPERTIES_CONTENT_ENCODING));

    assertTrue("expected creation time key to be present",
        properties.containsKey(MessageHelper.PROPERTIES_CREATION_TIME));
    assertEquals("expected creation time value to be present", testCreationTime,
        properties.getValue(MessageHelper.PROPERTIES_CREATION_TIME));

    assertTrue("expected absolute expiry time key to be present",
        properties.containsKey(MessageHelper.PROPERTIES_ABSOLUTE_EXPIRY_TIME));
    assertEquals("expected absolute expiry time value to be present", testAbsExpiryTime,
        properties.getValue(MessageHelper.PROPERTIES_ABSOLUTE_EXPIRY_TIME));
  }

  @Test
  public void testJSON_to_AMQP_VerifyMessageProperties() {
    String testToAddress = "myToAddress";
    String testReplyToAddress = "myReplyToAddress";
    String testMessageId = "myTestMessageId";
    String testCorrelationId = "myTestCorrelationId";
    String testSubject = "myTestSubject";
    String testGroupId = "myTestGroupId";
    long testGroupSeq = 4567;
    String testReplyToGroupId = "myReplyToGroupId";
    String testContentType = "myContentType";
    String testContentEncoding = "myContentEncoding";
    long testCreationTime = System.currentTimeMillis();
    long testAbsExpiryTime = testCreationTime + 3456;

    JsonObject jsonProps = new JsonObject();
    jsonProps.put(MessageHelper.PROPERTIES_TO, testToAddress);
    jsonProps.put(MessageHelper.PROPERTIES_REPLY_TO, testReplyToAddress);
    jsonProps.put(MessageHelper.PROPERTIES_MESSAGE_ID, testMessageId);
    jsonProps.put(MessageHelper.PROPERTIES_CORRELATION_ID, testCorrelationId);
    jsonProps.put(MessageHelper.PROPERTIES_SUBJECT, testSubject);
    jsonProps.put(MessageHelper.PROPERTIES_GROUP_ID, testGroupId);
    jsonProps.put(MessageHelper.PROPERTIES_GROUP_SEQUENCE, testGroupSeq);
    jsonProps.put(MessageHelper.PROPERTIES_REPLY_TO_GROUP_ID, testReplyToGroupId);
    jsonProps.put(MessageHelper.PROPERTIES_CONTENT_TYPE, testContentType);
    jsonProps.put(MessageHelper.PROPERTIES_CONTENT_ENCODING, testContentEncoding);
    jsonProps.put(MessageHelper.PROPERTIES_CREATION_TIME, testCreationTime);
    jsonProps.put(MessageHelper.PROPERTIES_ABSOLUTE_EXPIRY_TIME, testAbsExpiryTime);

    JsonObject jsonObject = new JsonObject();
    jsonObject.put(MessageHelper.PROPERTIES, jsonProps);

    Message protonMsg = translator.convertToAmqpMessage(jsonObject);
    assertNotNull("Expected converted msg", protonMsg);

    Properties properties = protonMsg.getProperties();
    assertNotNull("Properties section not present", properties);

    assertEquals("expected to value to be present", testToAddress, properties.getTo());
    assertEquals("expected reply to value to be present", testReplyToAddress, properties.getReplyTo());
    assertEquals("expected message id value to be present", testMessageId, properties.getMessageId());
    assertEquals("expected correlation id value to be present", testCorrelationId, properties.getCorrelationId());
    assertEquals("expected subject value to be present", testSubject, properties.getSubject());
    assertEquals("expected group id value to be present", testGroupId, properties.getGroupId());
    assertEquals("expected group sequence value to be present", UnsignedInteger.valueOf(testGroupSeq),
        properties.getGroupSequence());
    assertEquals("expected reply to group id value to be present", testReplyToGroupId, properties.getReplyToGroupId());
    assertEquals("expected content type value to be present", Symbol.valueOf(testContentType),
        properties.getContentType());
    assertEquals("expected content encoding value to be present", Symbol.valueOf(testContentEncoding),
        properties.getContentEncoding());
    assertEquals("expected creation time value to be present", testCreationTime,
        properties.getCreationTime().getTime());
    assertEquals("expected absolute expiry time value to be present", testAbsExpiryTime,
        properties.getAbsoluteExpiryTime().getTime());
  }
}
