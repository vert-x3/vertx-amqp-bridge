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

import java.util.LinkedHashMap;

import org.apache.qpid.proton.amqp.Symbol;

import io.vertx.amqp.bridge.Bridge;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;

public class BridgeImpl implements Bridge {

  private ProtonClient client;
  private ProtonConnection connection;
  private int port;

  public BridgeImpl(Vertx vertx, int port) {
    client = ProtonClient.create(vertx);
    this.port = port;
  }

  private static final Logger LOG = LoggerFactory.getLogger(BridgeImpl.class);

  @Override
  public Bridge start(Handler<AsyncResult<Void>> resultHandler) {

    client.connect("localhost", port, connectResult -> {
      if (connectResult.succeeded()) {
        connection = connectResult.result();

        LinkedHashMap<Symbol, Object> props = new LinkedHashMap<Symbol, Object>();
        props.put(BridgeMetaDataSupportImpl.PRODUCT_KEY, BridgeMetaDataSupportImpl.PRODUCT);
        props.put(BridgeMetaDataSupportImpl.VERSION_KEY, BridgeMetaDataSupportImpl.VERSION);
        connection.setProperties(props);

        connection.openHandler(openResult -> {
          LOG.trace("Bridge connection open complete");
          if (openResult.succeeded()) {
            resultHandler.handle(Future.succeededFuture());
          } else {
            resultHandler.handle(Future.failedFuture(openResult.cause()));
          }
        }).open();
        connection.open();

        // TODO: create dynamic address with receiver to use with reply handling
        // TODO: return the succeededFuture only after the receiver opens remotely?
        // Assume that it will and test later if it is ever used to verify that it did?
      } else {
        resultHandler.handle(Future.failedFuture(connectResult.cause()));
      }
    });

    return this;
  }

  @Override
  public MessageConsumer<JsonObject> createConsumer(String amqpAddress) {
    return new AmqpConsumerImpl(connection, amqpAddress);
  }

  // TODO: add result handler to tell that it opened? Producer creation has no way to plug this in, unlike consumer
  // 'completion handler' which could be used for the purpose. Might be simpler to have callback on create for both.
  @Override
  public MessageProducer<JsonObject> createProducer(String amqpAddress) {
    return new AmqpProducerImpl(connection, amqpAddress);
  }

  @Override
  public Bridge shutdown(Handler<AsyncResult<Void>> resultHandler) {
    if (connection != null) {
      connection.closeHandler(res -> {
        try {
          if (res.succeeded()) {
            resultHandler.handle(Future.succeededFuture());
          } else {
            resultHandler.handle(Future.failedFuture(res.cause()));
          }
        } finally {
          connection.disconnect();
          connection = null;
        }
      }).close();
    }
    return this;
  }
}
