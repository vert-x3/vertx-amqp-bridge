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
package io.vertx.amqpbridge.impl;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.vertx.amqpbridge.AmqpConstants;
import org.apache.qpid.proton.Proton;
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
import org.junit.Before;
import org.junit.Test;

import io.vertx.core.json.JsonArray;
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
    assertFalse("expected header element key not to be present", jsonObject.containsKey(AmqpConstants.HEADER));
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
    assertTrue("expected header element key to be present", jsonObject.containsKey(AmqpConstants.HEADER));

    JsonObject jsonHeader = jsonObject.getJsonObject(AmqpConstants.HEADER);
    assertNotNull("expected header element value to be non-null", jsonHeader);

    assertTrue("expected durable key to be present", jsonHeader.containsKey(AmqpConstants.HEADER_DURABLE));
    assertEquals("expected durable value to be present", testDurable,
        jsonHeader.getValue(AmqpConstants.HEADER_DURABLE));

    assertTrue("expected priority key to be present", jsonHeader.containsKey(AmqpConstants.HEADER_PRIORITY));
    assertEquals("expected priority value to be present", testPriority,
        jsonHeader.getValue(AmqpConstants.HEADER_PRIORITY));

    assertTrue("expected ttl key to be present", jsonHeader.containsKey(AmqpConstants.HEADER_TTL));
    assertEquals("expected ttl value to be present", testTtl, jsonHeader.getValue(AmqpConstants.HEADER_TTL));

    assertTrue("expected first acquirer key to be present",
        jsonHeader.containsKey(AmqpConstants.HEADER_FIRST_ACQUIRER));
    assertEquals("expected first acquirer  value to be present", testFirstAcquirer,
        jsonHeader.getValue(AmqpConstants.HEADER_FIRST_ACQUIRER));

    assertTrue("expected delivery count key to be present",
        jsonHeader.containsKey(AmqpConstants.HEADER_DELIVERY_COUNT));
    assertEquals("expected delivery count value to be present", testDeliveryCount,
        jsonHeader.getValue(AmqpConstants.HEADER_DELIVERY_COUNT));
  }

  @Test
  public void testJSON_to_AMQP_VerifyMessageHeader() {
    boolean testDurable = true;
    byte testPriority = 8;
    long testTtl = 2345;
    boolean testFirstAcquirer = true;
    long testDeliveryCount = 3;

    JsonObject jsonHeader = new JsonObject();
    jsonHeader.put(AmqpConstants.HEADER_DURABLE, testDurable);
    jsonHeader.put(AmqpConstants.HEADER_PRIORITY, testPriority);
    jsonHeader.put(AmqpConstants.HEADER_TTL, testTtl);
    jsonHeader.put(AmqpConstants.HEADER_FIRST_ACQUIRER, testFirstAcquirer);
    jsonHeader.put(AmqpConstants.HEADER_DELIVERY_COUNT, testDeliveryCount);

    JsonObject jsonObject = new JsonObject();
    jsonObject.put(AmqpConstants.HEADER, jsonHeader);

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
        jsonObject.containsKey(AmqpConstants.APPLICATION_PROPERTIES));
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
        jsonObject.containsKey(AmqpConstants.APPLICATION_PROPERTIES));

    JsonObject jsonAppProps = jsonObject.getJsonObject(AmqpConstants.APPLICATION_PROPERTIES);
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
        jsonObject.containsKey(AmqpConstants.APPLICATION_PROPERTIES));

    JsonObject jsonAppProps = jsonObject.getJsonObject(AmqpConstants.APPLICATION_PROPERTIES);
    assertNotNull("expected application properties element value to be non-null", jsonAppProps);

    assertTrue("expected key to be present", jsonAppProps.containsKey(symbolPropKey));
    assertEquals("expected value to be equal, as a string", symbolPropValue.toString(),
        jsonAppProps.getValue(symbolPropKey));
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
        jsonObject.containsKey(AmqpConstants.APPLICATION_PROPERTIES));

    JsonObject jsonAppProps = jsonObject.getJsonObject(AmqpConstants.APPLICATION_PROPERTIES);
    assertNotNull("expected application properties element value to be non-null", jsonAppProps);

    assertTrue("expected key to be present", jsonAppProps.containsKey(binaryPropKey));

    Map<String, Object> propsMap = jsonAppProps.getMap();
    assertTrue("expected key to be present", propsMap.containsKey(binaryPropKey));
    assertTrue("expected value to be present, as encoded string",
        jsonAppProps.getValue(binaryPropKey) instanceof String);
    assertArrayEquals("unepected decoded bytes", binaryPropValueSource.getBytes(StandardCharsets.UTF_8),
        jsonAppProps.getBinary(binaryPropKey));
  }

  /**
   * Verifies that an incoming timestamp AMQP application property is converted to a long [by the translator]
   */
  @Test
  public void testAMQP_to_JSON_VerifyApplicationPropertyTimestamp() {
    Map<String, Object> props = new HashMap<>();
    ApplicationProperties appProps = new ApplicationProperties(props);

    String timestampPropKey = "timestampPropKey";
    long now = System.currentTimeMillis();

    props.put(timestampPropKey, new Date(now));

    Message protonMsg = Proton.message();
    protonMsg.setApplicationProperties(appProps);

    JsonObject jsonObject = translator.convertToJsonObject(protonMsg);
    assertNotNull("expected converted msg", jsonObject);
    assertTrue("expected application properties element key to be present",
        jsonObject.containsKey(AmqpConstants.APPLICATION_PROPERTIES));

    JsonObject jsonAppProps = jsonObject.getJsonObject(AmqpConstants.APPLICATION_PROPERTIES);
    assertNotNull("expected application properties element value to be non-null", jsonAppProps);

    assertTrue("expected key to be present", jsonAppProps.containsKey(timestampPropKey));
    Map<String, Object> propsMap = jsonAppProps.getMap();
    assertTrue("expected key to be present", propsMap.containsKey(timestampPropKey));
    assertTrue("expected value to be present, as encoded long",
        jsonAppProps.getValue(timestampPropKey) instanceof Long);
    assertEquals("expected value to be equal", now, jsonAppProps.getValue(timestampPropKey));
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
    jsonObject.put(AmqpConstants.APPLICATION_PROPERTIES, jsonAppProps);

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

  // ============== message-annotations section ==============

  @Test
  public void testAMQP_to_JSON_WithNoMessageAnnotations() {
    Message protonMsg = Proton.message();

    JsonObject jsonObject = translator.convertToJsonObject(protonMsg);
    assertNotNull("expected converted msg", jsonObject);
    assertFalse("expected message annotations element key not to be present",
        jsonObject.containsKey(AmqpConstants.MESSAGE_ANNOTATIONS));
  }

  @Test
  public void testJSON_to_AMQP_WithNoMessageAnnotations() {
    JsonObject jsonObject = new JsonObject();

    Message protonMsg = translator.convertToAmqpMessage(jsonObject);
    assertNotNull("Expected converted msg", protonMsg);
    assertNull("expected converted msg to have no message annotations section", protonMsg.getMessageAnnotations());
  }

  @Test
  public void testAMQP_to_JSON_VerifyMessageAnnotations() {

    Map<Symbol, Object> annotations = new HashMap<>();
    MessageAnnotations ma = new MessageAnnotations(annotations);

    String testAnnKeyNameA = "testAnnKeyA";
    String testAnnKeyNameB = "testAnnKeyB";
    Symbol testAnnKeyA = Symbol.valueOf(testAnnKeyNameA);
    String testAnnValueA = "testAnnValueA";
    Symbol testAnnKeyB = Symbol.valueOf(testAnnKeyNameB);
    String testAnnValueB = "testAnnValueB";

    annotations.put(testAnnKeyA, testAnnValueA);
    annotations.put(testAnnKeyB, testAnnValueB);

    Message protonMsg = Proton.message();
    protonMsg.setMessageAnnotations(ma);

    JsonObject jsonObject = translator.convertToJsonObject(protonMsg);
    assertNotNull("expected converted msg", jsonObject);
    assertTrue("expected message annotations element key to be present",
        jsonObject.containsKey(AmqpConstants.MESSAGE_ANNOTATIONS));

    JsonObject jsonMsgAnn = jsonObject.getJsonObject(AmqpConstants.MESSAGE_ANNOTATIONS);
    assertNotNull("expected message annotations element value to be non-null", jsonMsgAnn);

    assertTrue("expected key to be present", jsonMsgAnn.containsKey(testAnnKeyNameA));
    assertEquals("expected value to be equal", testAnnValueA, jsonMsgAnn.getValue(testAnnKeyNameA));

    assertTrue("expected key to be present", jsonMsgAnn.containsKey(testAnnKeyNameB));
    assertEquals("expected value to be equal", testAnnValueB, jsonMsgAnn.getValue(testAnnKeyNameB));
  }

  @Test
  public void testAMQP_to_JSON_VerifyMessageAnnotationsNestedListAndMap() {
    String nestedBinaryKey = "binaryPropKey";
    String binaryValueSource = "binaryValueSource";

    Map<String, Object> nestedMap = new HashMap<>();
    nestedMap.put(nestedBinaryKey, new Binary(binaryValueSource.getBytes(StandardCharsets.UTF_8)));

    List<Object> nestedList = new ArrayList<>();
    nestedList.add(new Binary(binaryValueSource.getBytes(StandardCharsets.UTF_8)));

    String testAnnMapKeyName = "testAnnMapKeyName";
    Symbol testAnnMapKey = Symbol.valueOf(testAnnMapKeyName);
    String testAnnListKeyName = "testAnnListKeyName";
    Symbol testAnnListKey = Symbol.valueOf(testAnnListKeyName);

    Map<Symbol, Object> annotations = new HashMap<>();
    annotations.put(testAnnMapKey, nestedMap);
    annotations.put(testAnnListKey, nestedList);
    MessageAnnotations ma = new MessageAnnotations(annotations);

    Message protonMsg = Proton.message();
    protonMsg.setMessageAnnotations(ma);

    JsonObject jsonObject = translator.convertToJsonObject(protonMsg);
    assertNotNull("expected converted msg", jsonObject);
    assertTrue("expected message annotations element key to be present",
        jsonObject.containsKey(AmqpConstants.MESSAGE_ANNOTATIONS));

    JsonObject jsonMsgAnn = jsonObject.getJsonObject(AmqpConstants.MESSAGE_ANNOTATIONS);
    assertNotNull("expected message annotations element value to be non-null", jsonMsgAnn);

    assertTrue("expected map annotation key to be present", jsonMsgAnn.containsKey(testAnnMapKeyName));
    assertTrue("expected value to be JsonObject", jsonMsgAnn.getValue(testAnnMapKeyName) instanceof JsonObject);
    JsonObject nestedMapJson = jsonMsgAnn.getJsonObject(testAnnMapKeyName);
    assertTrue("expected nested map key to be present", nestedMapJson.containsKey(nestedBinaryKey));
    assertArrayEquals("expected nested value to be equal", binaryValueSource.getBytes(StandardCharsets.UTF_8),
        nestedMapJson.getBinary(nestedBinaryKey));

    assertTrue("expected list annotation key to be present", jsonMsgAnn.containsKey(testAnnListKeyName));
    assertTrue("expected value to be JsonArray", jsonMsgAnn.getValue(testAnnListKeyName) instanceof JsonArray);
    JsonArray nestedListJson = jsonMsgAnn.getJsonArray(testAnnListKeyName);
    assertArrayEquals("expected nested value to be equal", binaryValueSource.getBytes(StandardCharsets.UTF_8),
        nestedListJson.getBinary(0));
  }

  @Test
  public void testJSON_to_AMQP_VerifyMessageAnnotations() {
    String testAnnKeyNameA = "testAnnKeyA";
    String testAnnKeyNameB = "testAnnKeyB";
    Symbol testAnnKeyA = Symbol.valueOf(testAnnKeyNameA);
    String testAnnValueA = "testAnnValueA";
    Symbol testAnnKeyB = Symbol.valueOf(testAnnKeyNameB);
    String testAnnValueB = "testAnnValueB";

    JsonObject jsonAppProps = new JsonObject();

    jsonAppProps.put(testAnnKeyNameA, testAnnValueA);
    jsonAppProps.put(testAnnKeyNameB, testAnnValueB);

    JsonObject jsonObject = new JsonObject();
    jsonObject.put(AmqpConstants.MESSAGE_ANNOTATIONS, jsonAppProps);

    Message protonMsg = translator.convertToAmqpMessage(jsonObject);
    assertNotNull("Expected converted msg", protonMsg);

    MessageAnnotations ma = protonMsg.getMessageAnnotations();
    assertNotNull("message annotations  section not present", ma);

    Map<Symbol, Object> annotations = ma.getValue();
    assertNotNull("message annotations  map not present", ma);

    assertTrue("expected key to be present", annotations.containsKey(testAnnKeyA));
    assertEquals("expected value to be equal", testAnnValueA, annotations.get(testAnnKeyA));

    assertTrue("expected key to be present", annotations.containsKey(testAnnKeyB));
    assertEquals("expected value to be equal", testAnnValueB, annotations.get(testAnnKeyB));

    assertEquals("unexpected number of props", 2, annotations.size());
  }

  @Test
  public void testJSON_to_AMQP_VerifyMessageAnnotationsNestedListMap() {
    String nestedListAnnKeyName = "nestedListAnnKeyName";
    String nestedMapAnnKeyName = "nestedMapAnnKeyName";
    Symbol nestedListAnnKey = Symbol.valueOf(nestedListAnnKeyName);
    Symbol nestedMapAnnKey = Symbol.valueOf(nestedMapAnnKeyName);

    String nestedListEntry = "nestedListEntry";
    JsonArray nestedJsonList = new JsonArray();
    nestedJsonList.add(nestedListEntry);

    String nestedMapEntryKey = "nestedMapEntryKey";
    String nestedMapEntryValue = "nestedMapEntryValue";

    JsonObject nestedJsonMap = new JsonObject();
    nestedJsonMap.put(nestedMapEntryKey, nestedMapEntryValue);

    JsonObject jsonMsgAnn = new JsonObject();
    jsonMsgAnn.put(nestedListAnnKeyName, nestedJsonList);
    jsonMsgAnn.put(nestedMapAnnKeyName, nestedJsonMap);

    JsonObject jsonObject = new JsonObject();
    jsonObject.put(AmqpConstants.MESSAGE_ANNOTATIONS, jsonMsgAnn);

    Message protonMsg = translator.convertToAmqpMessage(jsonObject);
    assertNotNull("Expected converted msg", protonMsg);

    MessageAnnotations ma = protonMsg.getMessageAnnotations();
    assertNotNull("message annotations section not present", ma);

    Map<Symbol, Object> annotations = ma.getValue();
    assertNotNull("message annotations  map not present", ma);

    List<Object> expectedList = new ArrayList<>();
    expectedList.add(nestedListEntry);

    assertTrue("expected key to be present", annotations.containsKey(nestedListAnnKey));
    assertTrue("expected value to be list", annotations.get(nestedListAnnKey) instanceof List);
    assertEquals("expected value to be equal", expectedList, annotations.get(nestedListAnnKey));

    Map<String, Object> expectedMap = new LinkedHashMap<>();
    expectedMap.put(nestedMapEntryKey, nestedMapEntryValue);

    assertTrue("expected key to be present", annotations.containsKey(nestedMapAnnKey));
    assertTrue("expected value to be map", annotations.get(nestedMapAnnKey) instanceof Map);
    assertEquals("expected value to be equal", expectedMap, annotations.get(nestedMapAnnKey));
  }

  // ============== body section ==============

  // ------ amqp-value body section ------

  @Test
  public void testAMQP_to_JSON_VerifyBodyWithAmqpValueString() {
    String testContent = "myTestContent";
    Message protonMsg = Proton.message();
    protonMsg.setBody(new AmqpValue(testContent));

    JsonObject jsonObject = translator.convertToJsonObject(protonMsg);
    assertNotNull("expected converted msg", jsonObject);
    assertTrue("expected body element key to be present", jsonObject.containsKey(AmqpConstants.BODY));
    assertNotNull("expected body element value to be non-null", jsonObject.getValue(AmqpConstants.BODY));
    assertEquals("body value not as expected", testContent, jsonObject.getValue(AmqpConstants.BODY));
    assertTrue("expected body_type element key to be present", jsonObject.containsKey(AmqpConstants.BODY_TYPE));
    assertEquals("unexpected body_type value", AmqpConstants.BODY_TYPE_VALUE,
        jsonObject.getValue(AmqpConstants.BODY_TYPE));
  }

  @Test
  public void testJSON_to_AMQP_VerifyStringBody() {
    doJSON_to_AMQP_VerifyStringBodyTestImpl(true);
  }

  @Test
  public void testJSON_to_AMQP_VerifyStringBodyWithoutBodyTypeSet() {
    doJSON_to_AMQP_VerifyStringBodyTestImpl(false);
  }

  private void doJSON_to_AMQP_VerifyStringBodyTestImpl(boolean setBodyType) {
    String testContent = "myTestContent";

    JsonObject jsonObject = new JsonObject();
    jsonObject.put(AmqpConstants.BODY, testContent);
    if(setBodyType){
      jsonObject.put(AmqpConstants.BODY_TYPE, AmqpConstants.BODY_TYPE_VALUE);
    }

    Message protonMsg = translator.convertToAmqpMessage(jsonObject);

    assertNotNull("Expected converted msg", protonMsg);
    Section body = protonMsg.getBody();
    assertTrue("Unexpected body type", body instanceof AmqpValue);
    assertEquals("Unexpected message body value", testContent, ((AmqpValue) body).getValue());
  }

  @Test
  public void testAMQP_to_JSON_VerifyBodyWithAmqpValueMapNestedList() {
    String testValue = "testValue";
    List<String> list = new ArrayList<>();
    list.add(testValue);

    Map<String, Object> map = new HashMap<>();
    String testKey = "testKey";
    map.put(testKey, list);

    Message protonMsg = Proton.message();
    protonMsg.setBody(new AmqpValue(map));

    JsonObject jsonObject = translator.convertToJsonObject(protonMsg);
    assertNotNull("expected converted msg", jsonObject);
    assertTrue("expected body element key to be present", jsonObject.containsKey(AmqpConstants.BODY));
    assertNotNull("expected body element value to be non-null", jsonObject.getValue(AmqpConstants.BODY));
    assertTrue("expected body_type element key to be present", jsonObject.containsKey(AmqpConstants.BODY_TYPE));
    assertEquals("unexpected body_type value", AmqpConstants.BODY_TYPE_VALUE,
        jsonObject.getValue(AmqpConstants.BODY_TYPE));
    JsonObject jsonMap = jsonObject.getJsonObject(AmqpConstants.BODY);

    assertTrue("expected list element key to be present", jsonMap.containsKey(testKey));
    assertNotNull("expected list element value to be non-null", jsonMap.getValue(testKey));
    JsonArray jsonList = jsonMap.getJsonArray(testKey);

    assertEquals("list entry not as expected", testValue, jsonList.getValue(0));
  }

  @Test
  public void testJSON_to_AMQP_VerifyListBodyWithNestedMap() {
    String testKey = "testKey";
    String testValue = "testValue";
    JsonObject nestedJsonMap = new JsonObject();
    nestedJsonMap.put(testKey, testValue);

    JsonArray jsonList = new JsonArray();
    jsonList.add(nestedJsonMap);

    JsonObject jsonObject = new JsonObject();
    jsonObject.put(AmqpConstants.BODY, jsonList);
    jsonObject.put(AmqpConstants.BODY_TYPE, AmqpConstants.BODY_TYPE_VALUE);

    Message protonMsg = translator.convertToAmqpMessage(jsonObject);

    assertNotNull("Expected converted msg", protonMsg);
    Section body = protonMsg.getBody();
    assertTrue("Unexpected body type", body instanceof AmqpValue);
    @SuppressWarnings("unchecked")
    List<Object> list = (List<Object>) ((AmqpValue) body).getValue();
    assertNotNull("Unexpected list", list);
    assertEquals("Unexpected size", 1, list.size());
    Object map = list.get(0);
    assertTrue("Unexpected nested type", map instanceof Map);
    assertTrue("Key not present in nested map", ((Map<?, ?>) map).containsKey(testKey));
    assertEquals("Value not as expected in nested map", testValue, ((Map<?, ?>) map).get(testKey));
  }

  // ------ data body section ------

  @Test
  public void testAMQP_to_JSON_VerifyBodyWithDataSection() {
    String testContent = "myTestContent";
    Data data = new Data(new Binary(testContent.getBytes(StandardCharsets.UTF_8)));
    Message protonMsg = Proton.message();
    protonMsg.setBody(data);

    JsonObject jsonObject = translator.convertToJsonObject(protonMsg);
    assertNotNull("expected converted msg", jsonObject);
    assertTrue("expected body element key to be present", jsonObject.containsKey(AmqpConstants.BODY));
    assertNotNull("expected body element value to be non-null", jsonObject.getValue(AmqpConstants.BODY));
    assertTrue("expected body_type element key to be present", jsonObject.containsKey(AmqpConstants.BODY_TYPE));
    assertEquals("unexpected body_type value", AmqpConstants.BODY_TYPE_DATA,
        jsonObject.getValue(AmqpConstants.BODY_TYPE));

    jsonObject.put(AmqpConstants.BODY_TYPE, AmqpConstants.BODY_TYPE_VALUE);
    assertArrayEquals("body content not as expected", testContent.getBytes(StandardCharsets.UTF_8),
        jsonObject.getBinary(AmqpConstants.BODY));
  }

  @Test
  public void testJSON_to_AMQP_VerifyDataBody() {
    String testContent = "myTestContent";

    JsonObject jsonObject = new JsonObject();
    jsonObject.put(AmqpConstants.BODY, testContent.getBytes(StandardCharsets.UTF_8));
    jsonObject.put(AmqpConstants.BODY_TYPE, AmqpConstants.BODY_TYPE_DATA);

    Message protonMsg = translator.convertToAmqpMessage(jsonObject);

    assertNotNull("Expected converted msg", protonMsg);
    Section body = protonMsg.getBody();
    assertTrue("Unexpected body type", body instanceof Data);
    assertNotNull("Unexpected body content", body);
    assertEquals("Unexpected message body value", new Binary(testContent.getBytes(StandardCharsets.UTF_8)),
        ((Data) body).getValue());
  }

  // ------ amqp-sequence body section ------

  @Test
  public void testAMQP_to_JSON_VerifyBodyWithAmqpSequence() {
    String testContent = "myTestContent";
    List<Object> elements = new ArrayList<>();
    elements.add(new Binary(testContent.getBytes(StandardCharsets.UTF_8)));

    Message protonMsg = Proton.message();
    protonMsg.setBody(new AmqpSequence(elements));

    JsonObject jsonObject = translator.convertToJsonObject(protonMsg);

    assertNotNull("expected converted msg", jsonObject);
    assertTrue("expected body element key to be present", jsonObject.containsKey(AmqpConstants.BODY));
    assertNotNull("expected body element value to be non-null", jsonObject.getValue(AmqpConstants.BODY));
    assertTrue("expected body_type element key to be present", jsonObject.containsKey(AmqpConstants.BODY_TYPE));
    assertEquals("unexpected body_type value", AmqpConstants.BODY_TYPE_SEQUENCE,
        jsonObject.getValue(AmqpConstants.BODY_TYPE));
    JsonArray jsonSequence = jsonObject.getJsonArray(AmqpConstants.BODY);
    assertArrayEquals("sequence element value not as expected", testContent.getBytes(StandardCharsets.UTF_8),
        jsonSequence.getBinary(0));
    assertTrue("expected body_type element key to be present", jsonObject.containsKey(AmqpConstants.BODY_TYPE));
  }

  @Test
  public void testJSON_to_AMQP_VerifySequenceBody() {
    String testKey = "testKey";
    String testValue = "testValue";
    JsonObject nestedJsonMap = new JsonObject();
    nestedJsonMap.put(testKey, testValue);

    JsonArray jsonSequence = new JsonArray();
    jsonSequence.add(nestedJsonMap);

    JsonObject jsonObject = new JsonObject();
    jsonObject.put(AmqpConstants.BODY, jsonSequence);
    jsonObject.put(AmqpConstants.BODY_TYPE, AmqpConstants.BODY_TYPE_SEQUENCE);

    Message protonMsg = translator.convertToAmqpMessage(jsonObject);

    assertNotNull("Expected converted msg", protonMsg);
    Section body = protonMsg.getBody();
    assertTrue("Unexpected body type", body instanceof AmqpSequence);
    @SuppressWarnings("unchecked")
    List<Object> sequenceElements = ((AmqpSequence) body).getValue();
    assertNotNull("Unexpected sequence element list", sequenceElements);
    assertEquals("Unexpected sequence size", 1, sequenceElements.size());
    Object sequenceElement = sequenceElements.get(0);
    assertTrue("Unexpected sequence element type", sequenceElement instanceof Map);
    assertTrue("Key not present in nested map", ((Map<?, ?>) sequenceElement).containsKey(testKey));
    assertEquals("Value not as expected in nested map", testValue, ((Map<?, ?>) sequenceElement).get(testKey));
  }

  // ============== properties section ==============

  @Test
  public void testAMQP_to_JSON_WithNoPropertiesSection() {
    Message protonMsg = Proton.message();

    JsonObject jsonObject = translator.convertToJsonObject(protonMsg);
    assertNotNull("expected converted msg", jsonObject);
    assertFalse("expected properties element key not to be present", jsonObject.containsKey(AmqpConstants.PROPERTIES));
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
    String testUserId = "myUserId";

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
    protonMsg.setUserId(testUserId.getBytes(StandardCharsets.UTF_8));

    JsonObject jsonObject = translator.convertToJsonObject(protonMsg);
    assertNotNull("expected converted msg", jsonObject);
    assertTrue("expected properties element key to be present", jsonObject.containsKey(AmqpConstants.PROPERTIES));

    JsonObject properties = jsonObject.getJsonObject(AmqpConstants.PROPERTIES);
    assertNotNull("expected properties element value to be non-null", properties);

    assertTrue("expected to key to be present", properties.containsKey(AmqpConstants.PROPERTIES_TO));
    assertEquals("expected to value to be present", testToAddress, properties.getValue(AmqpConstants.PROPERTIES_TO));

    assertTrue("expected reply to key to be present", properties.containsKey(AmqpConstants.PROPERTIES_REPLY_TO));
    assertEquals("expected reply to value to be present", testReplyToAddress,
        properties.getValue(AmqpConstants.PROPERTIES_REPLY_TO));

    assertTrue("expected message id key to be present", properties.containsKey(AmqpConstants.PROPERTIES_MESSAGE_ID));
    assertEquals("expected message id value to be present", testMessageId,
        properties.getValue(AmqpConstants.PROPERTIES_MESSAGE_ID));

    assertTrue("expected correlation id key to be present",
        properties.containsKey(AmqpConstants.PROPERTIES_CORRELATION_ID));
    assertEquals("expected correlation id value to be present", testCorrelationId,
        properties.getValue(AmqpConstants.PROPERTIES_CORRELATION_ID));

    assertTrue("expected subject key to be present", properties.containsKey(AmqpConstants.PROPERTIES_SUBJECT));
    assertEquals("expected subject value to be present", testSubject,
        properties.getValue(AmqpConstants.PROPERTIES_SUBJECT));

    assertTrue("expected group id key to be present", properties.containsKey(AmqpConstants.PROPERTIES_GROUP_ID));
    assertEquals("expected group id value to be present", testGroupId,
        properties.getValue(AmqpConstants.PROPERTIES_GROUP_ID));

    assertTrue("expected group sequence key to be present",
        properties.containsKey(AmqpConstants.PROPERTIES_GROUP_SEQUENCE));
    assertEquals("expected group sequence value to be present", testGroupSeq,
        properties.getValue(AmqpConstants.PROPERTIES_GROUP_SEQUENCE));

    assertTrue("expected reply to group id key to be present",
        properties.containsKey(AmqpConstants.PROPERTIES_REPLY_TO_GROUP_ID));
    assertEquals("expected reply to group id value to be present", testReplyToGroupId,
        properties.getValue(AmqpConstants.PROPERTIES_REPLY_TO_GROUP_ID));

    assertTrue("expected content type key to be present",
        properties.containsKey(AmqpConstants.PROPERTIES_CONTENT_TYPE));
    assertEquals("expected content type  value to be present", testContentType,
        properties.getValue(AmqpConstants.PROPERTIES_CONTENT_TYPE));

    assertTrue("expected content encoding key to be present",
        properties.containsKey(AmqpConstants.PROPERTIES_CONTENT_ENCODING));
    assertEquals("expected content encoding  value to be present", testContentEncoding,
        properties.getValue(AmqpConstants.PROPERTIES_CONTENT_ENCODING));

    assertTrue("expected creation time key to be present",
        properties.containsKey(AmqpConstants.PROPERTIES_CREATION_TIME));
    assertEquals("expected creation time value to be present", testCreationTime,
        properties.getValue(AmqpConstants.PROPERTIES_CREATION_TIME));

    assertTrue("expected absolute expiry time key to be present",
        properties.containsKey(AmqpConstants.PROPERTIES_ABSOLUTE_EXPIRY_TIME));
    assertEquals("expected absolute expiry time value to be present", testAbsExpiryTime,
        properties.getValue(AmqpConstants.PROPERTIES_ABSOLUTE_EXPIRY_TIME));

    assertTrue("expected user id key to be present",
        properties.containsKey(AmqpConstants.PROPERTIES_USER_ID));
    assertEquals("expected user id value to be present", testUserId,
        properties.getValue(AmqpConstants.PROPERTIES_USER_ID));
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
    String testUserId = "myUserId";

    JsonObject jsonProps = new JsonObject();
    jsonProps.put(AmqpConstants.PROPERTIES_TO, testToAddress);
    jsonProps.put(AmqpConstants.PROPERTIES_REPLY_TO, testReplyToAddress);
    jsonProps.put(AmqpConstants.PROPERTIES_MESSAGE_ID, testMessageId);
    jsonProps.put(AmqpConstants.PROPERTIES_CORRELATION_ID, testCorrelationId);
    jsonProps.put(AmqpConstants.PROPERTIES_SUBJECT, testSubject);
    jsonProps.put(AmqpConstants.PROPERTIES_GROUP_ID, testGroupId);
    jsonProps.put(AmqpConstants.PROPERTIES_GROUP_SEQUENCE, testGroupSeq);
    jsonProps.put(AmqpConstants.PROPERTIES_REPLY_TO_GROUP_ID, testReplyToGroupId);
    jsonProps.put(AmqpConstants.PROPERTIES_CONTENT_TYPE, testContentType);
    jsonProps.put(AmqpConstants.PROPERTIES_CONTENT_ENCODING, testContentEncoding);
    jsonProps.put(AmqpConstants.PROPERTIES_CREATION_TIME, testCreationTime);
    jsonProps.put(AmqpConstants.PROPERTIES_ABSOLUTE_EXPIRY_TIME, testAbsExpiryTime);
    jsonProps.put(AmqpConstants.PROPERTIES_USER_ID, testUserId);

    JsonObject jsonObject = new JsonObject();
    jsonObject.put(AmqpConstants.PROPERTIES, jsonProps);

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
    assertEquals("expected user id value to be present",
        new Binary(testUserId.getBytes(StandardCharsets.UTF_8)), properties.getUserId());
  }
}
