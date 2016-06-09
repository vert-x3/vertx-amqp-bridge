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

import io.vertx.amqp.bridge.impl.AmqpBridgeImpl;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;

/**
 * Vert.x AMQP Bridge. Facilitates sending and receiving AMQP 1.0 messages.
 */
@VertxGen
public interface AmqpBridge {

  /**
   * Creates a Bridge.
   *
   * @param vertx
   *          the vertx instance to use
   * @return the (not-yet-started) bridge.
   */
  static AmqpBridge create(Vertx vertx) {
    return create(vertx, new AmqpBridgeOptions());
  }

  /**
   * Creates a Bridge with the given options.
   *
   * @param vertx
   *          the vertx instance to use
   * @param options
   *          the options
   * @return the (not-yet-started) bridge.
   */
  static AmqpBridge create(Vertx vertx, AmqpBridgeOptions options) {
    return new AmqpBridgeImpl(vertx, options);
  }

  /**
   * Starts the bridge, establishing the underlying connection.
   *
   * @param hostname
   *          the host name to connect to
   * @param port
   *          the port to connect to
   * @param username
   *          the username
   * @param password
   *          the password
   * @param resultHandler
   *          the result handler
   */
  void start(String hostname, int port, String username, String password, Handler<AsyncResult<AmqpBridge>> resultHandler) ;

  /**
   * Starts the bridge, establishing the underlying connection.
   *
   * @param hostname
   *          the host name to connect to
   * @param port
   *          the port to connect to
   * @param resultHandler
   *          the result handler
   */
  void start(String hostname, int port, Handler<AsyncResult<AmqpBridge>> resultHandler);

  /**
   * Creates a consumer on the given AMQP address.
   *
   * This method MUST be called from the bridge Context thread, as used in the result handler callback from the start
   * methods. The bridge MUST be successfully started before the method is called.
   *
   * @param amqpAddress
   *          the address to consume from
   * @return the consumer
   * @throws IllegalStateException
   *           if the bridge was not started or the method is invoked on a thread other than the bridge Context thread,
   *           as used in the result handler callback from the start methods.
   */
  <T> MessageConsumer<T> createConsumer(String amqpAddress) throws IllegalStateException;

  /**
   * Creates a producer to the given AMQP address.
   *
   * This method MUST be called from the bridge Context thread, as used in the result handler callback from the start
   * methods. The bridge MUST be successfully started before the method is called.
   *
   * @param amqpAddress
   *          the address to produce to
   * @return the producer
   * @throws IllegalStateException
   *           if the bridge was not started or the method is invoked on a thread other than the bridge Context thread,
   *           as used in the result handler callback from the start methods.
   */
  <T> MessageProducer<T> createProducer(String amqpAddress) throws IllegalStateException;

  /**
   * Shuts the bridge down, closing the underlying connection.
   *
   * @param resultHandler
   *          the result handler
   */
  void close(Handler<AsyncResult<Void>> resultHandler);
}
