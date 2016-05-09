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

import java.util.ArrayDeque;
import java.util.Queue;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.ReadStream;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonReceiver;

public class AmqpConsumerImpl implements MessageConsumer<JsonObject> {

  private final Vertx vertx;
  private final BridgeImpl bridge;
  private final ProtonReceiver receiver;
  private final MessageTranslatorImpl translator;
  private Handler<Message<JsonObject>> handler;
  private final Queue<Message<JsonObject>> buffered = new ArrayDeque<>();
  private boolean paused = false;

  public AmqpConsumerImpl(Vertx vertx, BridgeImpl bridge, ProtonConnection connection, String amqpAddress) {
    this.vertx = vertx;
    this.bridge = bridge;
    translator = new MessageTranslatorImpl();

    receiver = connection.createReceiver(amqpAddress);
    receiver.handler((delivery, protonMessage) -> {
      JsonObject body = translator.convertToJsonObject(protonMessage);
      Message<JsonObject> vertxMessage = new AmqpMessageImpl(body, this.bridge, protonMessage);

      handleMessage(vertxMessage);
    });
    receiver.open();
  }

  private void handleMessage(Message<JsonObject> vertxMessage) {
    Handler<Message<JsonObject>> h = null;
    if (handler != null && !paused && buffered.isEmpty()) {
      h = handler;
    } else if (handler != null && !paused) {
      // Buffered messages present, deliver the oldest of those instead
      buffered.add(vertxMessage);
      vertxMessage = buffered.poll();
      h = handler;

      // Schedule a delivery for the next buffered message
      scheduleBufferedMessageDelivery();
    } else {
      // Buffer message until we have a handler or aren't paused
      buffered.add(vertxMessage);
    }

    if (h != null) {
      deliverMessageToHandler(vertxMessage, h);
    }
  }

  private void deliverMessageToHandler(Message<JsonObject> vertxMessage, Handler<Message<JsonObject>> h) {
    h.handle(vertxMessage);
    // TODO: manual accept and credit replenishment?
  }

  private void scheduleBufferedMessageDelivery() {
    if (!buffered.isEmpty()) {
      vertx.runOnContext(v -> {
        if (handler != null) {
          Message<JsonObject> message = buffered.poll();
          if (message != null) {
            deliverMessageToHandler(message, handler);

            // Schedule a delivery for a further buffered message if any
            scheduleBufferedMessageDelivery();
          }
        }
      });
    }
  }

  @Override
  public MessageConsumer<JsonObject> exceptionHandler(Handler<Throwable> handler) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  @Override
  public MessageConsumer<JsonObject> handler(final Handler<Message<JsonObject>> handler) {
    this.handler = handler;

    if (handler != null) {
      scheduleBufferedMessageDelivery();
    }

    return this;
  }

  @Override
  public MessageConsumer<JsonObject> pause() {
    paused = true;
    return this;
  }

  @Override
  public MessageConsumer<JsonObject> resume() {
    paused = false;
    scheduleBufferedMessageDelivery();
    return this;
  }

  @Override
  public MessageConsumer<JsonObject> endHandler(Handler<Void> endHandler) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  @Override
  public ReadStream<JsonObject> bodyStream() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isRegistered() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  @Override
  public String address() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  @Override
  public MessageConsumer<JsonObject> setMaxBufferedMessages(int maxBufferedMessages) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  @Override
  public int getMaxBufferedMessages() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  @Override
  public void completionHandler(Handler<AsyncResult<Void>> completionHandler) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  @Override
  public void unregister() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  @Override
  public void unregister(Handler<AsyncResult<Void>> completionHandler) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }
}
