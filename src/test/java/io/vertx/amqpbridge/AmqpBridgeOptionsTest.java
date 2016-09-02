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
package io.vertx.amqpbridge;

import static org.junit.Assert.*;

import org.junit.Test;

public class AmqpBridgeOptionsTest {

  @Test
  public void testEqualsItself() {
    AmqpBridgeOptions options = new AmqpBridgeOptions();

    assertEquals("Options should be equal to itself", options, options);
  }

  @Test
  public void testDifferentObjectsEqual() {
    AmqpBridgeOptions options1 = new AmqpBridgeOptions();
    options1.setReplyHandlingSupport(true);
    AmqpBridgeOptions options2 = new AmqpBridgeOptions();
    options2.setReplyHandlingSupport(true);

    assertNotSame("Options should be different objects", options1, options2);
    assertEquals("Options should be equal", options1, options2);
  }

  @Test
  public void testDifferentObjectsNotEqual() {
    AmqpBridgeOptions options1 = new AmqpBridgeOptions();
    options1.setReplyHandlingSupport(true);
    AmqpBridgeOptions options2 = new AmqpBridgeOptions();
    options2.setReplyHandlingSupport(false);

    assertNotSame("Options should be different objects", options1, options2);
    assertNotEquals("Options should not be equal", options1, options2);
  }

  @Test
  public void testEqualsNull() {
    AmqpBridgeOptions options = new AmqpBridgeOptions();

    assertFalse("Options should not equal null", options.equals(null));
  }

  @Test
  public void testEqualObjectsReturnSameHashCode() {
    AmqpBridgeOptions options1 = new AmqpBridgeOptions();
    options1.setReplyHandlingSupport(true);
    AmqpBridgeOptions options2 = new AmqpBridgeOptions();
    options2.setReplyHandlingSupport(true);

    assertNotSame("Options should be different objects", options1, options2);
    assertEquals("Options should be equal", options1, options2);
    assertEquals("Options should have same hash code", options1.hashCode(), options2.hashCode());
  }

  @Test
  public void testHashCodeReturnsSameValueOnRepeatedCall() {
    AmqpBridgeOptions options = new AmqpBridgeOptions();
    options.addEnabledSaslMechanism("PLAIN");

    assertEquals("Options should have same hash code for both calls", options.hashCode(), options.hashCode());
  }

  @Test
  public void testReplyHandlingSupportEnabled() {
    AmqpBridgeOptions options = new AmqpBridgeOptions();

    assertTrue("Reply Handler Support should be enabled", options.isReplyHandlingSupport());

    options.setReplyHandlingSupport(false);
    assertFalse("Reply Handler Support should not be enabled", options.isReplyHandlingSupport());
  }
}
