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
    boolean replyHandlingSupport = true;
    String vhost = "vhost";
    String containerId = "container";

    AmqpBridgeOptions options1 = new AmqpBridgeOptions();
    options1.setReplyHandlingSupport(replyHandlingSupport);
    options1.setVhost(vhost);
    options1.setContainerId(containerId);
    AmqpBridgeOptions options2 = new AmqpBridgeOptions();
    options2.setReplyHandlingSupport(replyHandlingSupport);
    options2.setVhost(vhost);
    options2.setContainerId(containerId);

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

    options1 = new AmqpBridgeOptions();
    options1.setVhost("vhost1");
    options2 = new AmqpBridgeOptions();
    options2.setVhost("vhost2");

    assertNotSame("Options should be different objects", options1, options2);
    assertNotEquals("Options should not be equal", options1, options2);

    options1 = new AmqpBridgeOptions();
    options1.setContainerId("containerId1");
    options2 = new AmqpBridgeOptions();
    options2.setContainerId("containerId2");

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
    boolean replyHandlingSupport = true;
    options1.setReplyHandlingSupport(replyHandlingSupport);
    AmqpBridgeOptions options2 = new AmqpBridgeOptions();
    options2.setReplyHandlingSupport(replyHandlingSupport);

    assertNotSame("Options should be different objects", options1, options2);
    assertEquals("Options should be equal", options1, options2);
    assertEquals("Options should have same hash code", options1.hashCode(), options2.hashCode());

    String vhost = "vhost";
    options1 = new AmqpBridgeOptions();
    options1.setVhost(vhost);
    options2 = new AmqpBridgeOptions();
    options2.setVhost(vhost);

    assertNotSame("Options should be different objects", options1, options2);
    assertEquals("Options should be equal", options1, options2);
    assertEquals("Options should have same hash code", options1.hashCode(), options2.hashCode());

    String containerId = "containerId";
    options1 = new AmqpBridgeOptions();
    options1.setContainerId(containerId);
    options2 = new AmqpBridgeOptions();
    options2.setContainerId(containerId);

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

  @Test
  public void testContainerId() {
    AmqpBridgeOptions options = new AmqpBridgeOptions();

    assertNull("No default should be present", options.getContainerId());

    String containerId = "container";
    options.setContainerId(containerId);
    assertEquals("ContainerId option was not as expected", containerId, options.getContainerId());
  }

  @Test
  public void testVhost() {
    AmqpBridgeOptions options = new AmqpBridgeOptions();

    assertNull("No default should be present", options.getVhost());

    String vhost = "vhost";
    options.setVhost(vhost);
    assertEquals("Vhost option was not as expected", vhost, options.getVhost());
  }
}
