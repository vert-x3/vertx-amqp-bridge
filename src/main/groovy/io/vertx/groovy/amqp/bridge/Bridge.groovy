/*
 * Copyright 2014 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.vertx.groovy.amqp.bridge;
import groovy.transform.CompileStatic
import io.vertx.lang.groovy.InternalHelper
import io.vertx.core.json.JsonObject
import io.vertx.groovy.core.Vertx
import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.groovy.core.eventbus.MessageConsumer
import io.vertx.amqp.bridge.BridgeOptions
import io.vertx.groovy.core.eventbus.MessageProducer
/**
 * Vert.x AMQP Bridge. Facilitates sending and receiving AMQP messages.
*/
@CompileStatic
public class Bridge {
  private final def io.vertx.amqp.bridge.Bridge delegate;
  public Bridge(Object delegate) {
    this.delegate = (io.vertx.amqp.bridge.Bridge) delegate;
  }
  public Object getDelegate() {
    return delegate;
  }
  /**
   * Creates a Bridge.
   * @param vertx the vertx instance to use
   * @return the (not-yet-started) bridge.
   */
  public static Bridge bridge(Vertx vertx) {
    def ret = InternalHelper.safeCreate(io.vertx.amqp.bridge.Bridge.bridge(vertx != null ? (io.vertx.core.Vertx)vertx.getDelegate() : null), io.vertx.groovy.amqp.bridge.Bridge.class);
    return ret;
  }
  /**
   * Creates a Bridge with the given options.
   * @param vertx the vertx instance to use
   * @param options the options (see <a href="../../../../../../../cheatsheet/BridgeOptions.html">BridgeOptions</a>)
   * @return the (not-yet-started) bridge.
   */
  public static Bridge bridge(Vertx vertx, Map<String, Object> options) {
    def ret = InternalHelper.safeCreate(io.vertx.amqp.bridge.Bridge.bridge(vertx != null ? (io.vertx.core.Vertx)vertx.getDelegate() : null, options != null ? new io.vertx.amqp.bridge.BridgeOptions(new io.vertx.core.json.JsonObject(options)) : null), io.vertx.groovy.amqp.bridge.Bridge.class);
    return ret;
  }
  /**
   * Starts the bridge, establishing the underlying connection.
   * @param hostname the host name to connect to
   * @param port the port to connect to
   * @param username the username
   * @param password the password
   * @param resultHandler the result handler
   * @return the bridge
   */
  public Bridge start(String hostname, int port, String username, String password, Handler<AsyncResult<Void>> resultHandler) {
    delegate.start(hostname, port, username, password, resultHandler);
    return this;
  }
  /**
   * Starts the bridge, establishing the underlying connection.
   * @param hostname the host name to connect to
   * @param port the port to connect to
   * @param resultHandler the result handler
   * @return the bridge
   */
  public Bridge start(String hostname, int port, Handler<AsyncResult<Void>> resultHandler) {
    delegate.start(hostname, port, resultHandler);
    return this;
  }
  /**
   * Creates a consumer on the given AMQP address.
   * @param amqpAddress the address to consume from
   * @return the consumer
   */
  public <T> MessageConsumer<T> createConsumer(String amqpAddress) {
    def ret = InternalHelper.safeCreate(delegate.createConsumer(amqpAddress), io.vertx.groovy.core.eventbus.MessageConsumer.class);
    return ret;
  }
  /**
   * Creates a producer to the given AMQP address.
   * @param amqpAddress the address to produce to
   * @return the producer
   */
  public <T> MessageProducer<T> createProducer(String amqpAddress) {
    def ret = InternalHelper.safeCreate(delegate.createProducer(amqpAddress), io.vertx.groovy.core.eventbus.MessageProducer.class);
    return ret;
  }
  /**
   * Shuts the bridge down, closing the underlying connection.
   * @param resultHandler the result handler
   * @return the bridge
   */
  public Bridge shutdown(Handler<AsyncResult<Void>> resultHandler) {
    delegate.shutdown(resultHandler);
    return this;
  }
}
