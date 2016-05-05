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

import io.vertx.amqp.bridge.impl.BridgeImpl;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;

/**
 * Vert.x AMQP Bridge. Facilitates sending and receiving AMQP messages.
 */
public interface Bridge {

  /**
   * Creates a Bridge.
   *
   * @param vertx
   *          the vertx instance to use
   * @return the (not-yet-started) bridge.
   */
  static Bridge bridge(Vertx vertx) {
    return bridge(vertx, new BridgeOptions());
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
  static Bridge bridge(Vertx vertx, BridgeOptions options) {
    return new BridgeImpl(vertx, options);
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
   * @return the bridge
   */
  Bridge start(String hostname, int port, String username, String password, Handler<AsyncResult<Void>> resultHandler);

  /**
   * Starts the bridge, establishing the underlying connection.
   *
   * @param hostname
   *          the host name to connect to
   * @param port
   *          the port to connect to
   * @param resultHandler
   *          the result handler
   * @return the bridge
   */
  Bridge start(String hostname, int port, Handler<AsyncResult<Void>> resultHandler);

  /**
   * Creates a consumer on the given AMQP address.
   *
   * @param amqpAddress
   *          the address to consume from
   * @return the consumer
   */
  MessageConsumer<JsonObject> createConsumer(String amqpAddress);

  /**
   * Creates a producer to the given AMQP address.
   *
   * @param amqpAddress
   *          the address to produce to
   * @return the producer
   */
  MessageProducer<JsonObject> createProducer(String amqpAddress);

  /**
   * Shuts the bridge down, closing the underlying connection.
   *
   * @param resultHandler
   *          the result handler
   * @return the bridge
   */
  Bridge shutdown(Handler<AsyncResult<Void>> resultHandler);
}
