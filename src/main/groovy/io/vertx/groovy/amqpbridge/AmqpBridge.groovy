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

package io.vertx.groovy.amqpbridge;
import groovy.transform.CompileStatic
import io.vertx.lang.groovy.InternalHelper
import io.vertx.core.json.JsonObject
import io.vertx.groovy.core.Vertx
import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.groovy.core.eventbus.MessageConsumer
import io.vertx.groovy.core.eventbus.MessageProducer
import io.vertx.amqpbridge.AmqpBridgeOptions
/**
 * Vert.x AMQP Bridge. Facilitates sending and receiving AMQP 1.0 messages.
*/
@CompileStatic
public class AmqpBridge {
  private final def io.vertx.amqpbridge.AmqpBridge delegate;
  public AmqpBridge(Object delegate) {
    this.delegate = (io.vertx.amqpbridge.AmqpBridge) delegate;
  }
  public Object getDelegate() {
    return delegate;
  }
  /**
   * Creates a Bridge.
   * @param vertx the vertx instance to use
   * @return the (not-yet-started) bridge.
   */
  public static AmqpBridge create(Vertx vertx) {
    def ret = InternalHelper.safeCreate(io.vertx.amqpbridge.AmqpBridge.create(vertx != null ? (io.vertx.core.Vertx)vertx.getDelegate() : null), io.vertx.groovy.amqpbridge.AmqpBridge.class);
    return ret;
  }
  /**
   * Creates a Bridge with the given options.
   * @param vertx the vertx instance to use
   * @param options the options (see <a href="../../../../../../cheatsheet/AmqpBridgeOptions.html">AmqpBridgeOptions</a>)
   * @return the (not-yet-started) bridge.
   */
  public static AmqpBridge create(Vertx vertx, Map<String, Object> options) {
    def ret = InternalHelper.safeCreate(io.vertx.amqpbridge.AmqpBridge.create(vertx != null ? (io.vertx.core.Vertx)vertx.getDelegate() : null, options != null ? new io.vertx.amqpbridge.AmqpBridgeOptions(io.vertx.lang.groovy.InternalHelper.toJsonObject(options)) : null), io.vertx.groovy.amqpbridge.AmqpBridge.class);
    return ret;
  }
  /**
   * Starts the bridge, establishing the underlying connection.
   * @param hostname the host name to connect to
   * @param port the port to connect to
   * @param username the username
   * @param password the password
   * @param resultHandler the result handler
   */
  public void start(String hostname, int port, String username, String password, Handler<AsyncResult<AmqpBridge>> resultHandler) {
    delegate.start(hostname, port, username, password, resultHandler != null ? new Handler<AsyncResult<io.vertx.amqpbridge.AmqpBridge>>() {
      public void handle(AsyncResult<io.vertx.amqpbridge.AmqpBridge> ar) {
        if (ar.succeeded()) {
          resultHandler.handle(io.vertx.core.Future.succeededFuture(InternalHelper.safeCreate(ar.result(), io.vertx.groovy.amqpbridge.AmqpBridge.class)));
        } else {
          resultHandler.handle(io.vertx.core.Future.failedFuture(ar.cause()));
        }
      }
    } : null);
  }
  /**
   * Starts the bridge, establishing the underlying connection.
   * @param hostname the host name to connect to
   * @param port the port to connect to
   * @param resultHandler the result handler
   */
  public void start(String hostname, int port, Handler<AsyncResult<AmqpBridge>> resultHandler) {
    delegate.start(hostname, port, resultHandler != null ? new Handler<AsyncResult<io.vertx.amqpbridge.AmqpBridge>>() {
      public void handle(AsyncResult<io.vertx.amqpbridge.AmqpBridge> ar) {
        if (ar.succeeded()) {
          resultHandler.handle(io.vertx.core.Future.succeededFuture(InternalHelper.safeCreate(ar.result(), io.vertx.groovy.amqpbridge.AmqpBridge.class)));
        } else {
          resultHandler.handle(io.vertx.core.Future.failedFuture(ar.cause()));
        }
      }
    } : null);
  }
  /**
   * Creates a consumer on the given AMQP address.
   *
   * This method MUST be called from the bridge Context thread, as used in the result handler callback from the start
   * methods. The bridge MUST be successfully started before the method is called.
   * @param amqpAddress the address to consume from
   * @return the consumer
   */
  public <T> MessageConsumer<T> createConsumer(String amqpAddress) {
    def ret = InternalHelper.safeCreate(delegate.createConsumer(amqpAddress), io.vertx.groovy.core.eventbus.MessageConsumer.class);
    return ret;
  }
  /**
   * Creates a producer to the given AMQP address.
   *
   * This method MUST be called from the bridge Context thread, as used in the result handler callback from the start
   * methods. The bridge MUST be successfully started before the method is called.
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
   */
  public void close(Handler<AsyncResult<Void>> resultHandler) {
    delegate.close(resultHandler);
  }
}
