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

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.VertxException;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonSender;

public class AmqpProducerImpl implements MessageProducer<JsonObject> {

  private final ProtonSender sender;
  private final MessageTranslatorImpl translator = new MessageTranslatorImpl();
  private final BridgeImpl bridge;
  private final String amqpAddress;
  private boolean closed;
  private Handler<Throwable> exceptionHandler;
  private Handler<Void> drainHandler;

  public AmqpProducerImpl(BridgeImpl bridge, ProtonConnection connection, String amqpAddress) {
    sender = connection.createSender(amqpAddress);
    sender.closeHandler(res -> {
      if (!closed && exceptionHandler != null) {
        if (res.succeeded()) {
          exceptionHandler.handle(new VertxException("Producer closed remotely"));
        } else {
          exceptionHandler.handle(new VertxException("Producer closed remotely with error", res.cause()));
        }
      }

      if(!closed) {
        closed = true;
        sender.close();
      }
    });
    sender.sendQueueDrainHandler(s -> {
      if(drainHandler != null) {
        drainHandler.handle(null);
      }
    });
    sender.open();
    this.bridge = bridge;
    this.amqpAddress= amqpAddress;
  }

  @Override
  public boolean writeQueueFull() {
    return sender.sendQueueFull();
  }

  @Override
  public MessageProducer<JsonObject> send(JsonObject messageBody) {
    return send(messageBody, null);
  }

  @Override
  public <R> MessageProducer<JsonObject> send(JsonObject messageBody, Handler<AsyncResult<Message<R>>> replyHandler) {
    return doSend(messageBody, replyHandler, null);
  }

  protected <R> MessageProducer<JsonObject> doSend(JsonObject messageBody,
                                                   Handler<AsyncResult<Message<R>>> replyHandler, String toAddress) {
    org.apache.qpid.proton.message.Message msg = translator.convertToAmqpMessage(messageBody);

    if (toAddress != null) {
      msg.setAddress(toAddress);
    }

    if (replyHandler != null) {
      bridge.registerReplyToHandler(msg, replyHandler);
    }

    sender.send(msg);

    return this;
  }

  @Override
  public MessageProducer<JsonObject> exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;

    return this;
  }

  @Override
  public MessageProducer<JsonObject> write(JsonObject data) {
    return send(data, null);
  }

  @Override
  public MessageProducer<JsonObject> setWriteQueueMaxSize(int maxSize) {
    // No-op, available sending credit is controlled by recipient peer in AMQP 1.0.
    return this;
  }

  @Override
  public MessageProducer<JsonObject> drainHandler(Handler<Void> handler) {
    drainHandler = handler;

    return this;
  }

  @Override
  public MessageProducer<JsonObject> deliveryOptions(DeliveryOptions options) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  @Override
  public String address() {
    return amqpAddress;
  }

  @Override
  public void end() {
    close();
  }

  @Override
  public void close() {
    closed = true;
    sender.close();
  }
}
