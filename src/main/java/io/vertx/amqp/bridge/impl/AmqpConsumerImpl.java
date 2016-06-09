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
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.VertxException;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.impl.BodyReadStream;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.ReadStream;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonReceiver;

public class AmqpConsumerImpl implements MessageConsumer<JsonObject> {

  private static final Logger LOG = LoggerFactory.getLogger(AmqpConsumerImpl.class);

  private final AmqpBridgeImpl bridge;
  private final ProtonReceiver receiver;
  private final String amqpAddress;
  private final MessageTranslatorImpl translator = new MessageTranslatorImpl();
  private final Queue<AmqpMessageImpl> buffered = new ArrayDeque<>();
  private Handler<Message<JsonObject>> handler;
  private boolean paused;
  private boolean closed;
  private Handler<Throwable> exceptionHandler;
  private Handler<Void> endHandler;
  private boolean initialCreditGiven;
  private int initialCredit = 1000;

  public AmqpConsumerImpl(AmqpBridgeImpl bridge, ProtonConnection connection, String amqpAddress) {
    if(!bridge.onContextEventLoop()) {
      throw new IllegalStateException("Consumer creation was not executed on the bridge context thread");
    }

    this.bridge = bridge;
    this.amqpAddress = amqpAddress;
    receiver = connection.createReceiver(amqpAddress);
    receiver.closeHandler(res -> {
      Handler<Void> endh = null;
      Handler<Throwable> exh = null;
      boolean closeReceiver = false;

      synchronized (AmqpConsumerImpl.this) {
        if (!closed && endHandler != null) {
          endh = endHandler;
        } else if (!closed && exceptionHandler != null) {
          exh = exceptionHandler;
        }

        if(!closed) {
          closed = true;
          closeReceiver = true;
        }
      }

      if (endh != null) {
        endh.handle(null);
      } else if (exh != null) {
        if (res.succeeded()) {
          exh.handle(new VertxException("Consumer closed remotely"));
        } else {
          exh.handle(new VertxException("Consumer closed remotely with error", res.cause()));
        }
      } else {
        if (res.succeeded()) {
          LOG.warn("Consumer for address " + amqpAddress + " unexpectedly closed remotely");
        } else {
          LOG.warn("Consumer for address " + amqpAddress + " unexpectedly closed remotely with error", res.cause());
        }
      }

      if(closeReceiver) {
        receiver.close();
      }
    });
    receiver.handler((delivery, protonMessage) -> {
      JsonObject body = translator.convertToJsonObject(protonMessage);
      AmqpMessageImpl vertxMessage = new AmqpMessageImpl(body, this.bridge, protonMessage, delivery, amqpAddress,
          protonMessage.getReplyTo());

      handleMessage(vertxMessage);
    });
    // Disable auto-accept and automated prefetch, we will manage disposition and credit
    // manually to allow for delayed handler registration and pause/resume functionality.
    receiver.setAutoAccept(false);
    receiver.setPrefetch(0);

    receiver.open();
  }

  private void handleMessage(AmqpMessageImpl vertxMessage) {
    Handler<Message<JsonObject>> h = null;
    boolean schedule = false;

    synchronized (AmqpConsumerImpl.this) {
      if (handler != null && !paused && buffered.isEmpty()) {
        h = handler;
      } else if (handler != null && !paused) {
        // Buffered messages present, deliver the oldest of those instead
        buffered.add(vertxMessage);
        vertxMessage = buffered.poll();
        h = handler;

        // Schedule a delivery for the next buffered message
        schedule = true;
      } else {
        // Buffer message until we have a handler or aren't paused
        buffered.add(vertxMessage);
      }
    }

    // Delivering outside the synchronized block
    if (h != null) {
      deliverMessageToHandler(vertxMessage, h);
    }

    // schedule next delivery if appropriate, after earlier delivery to allow chance to pause etc.
    if(schedule) {
      scheduleBufferedMessageDelivery();
    }
  }

  private void deliverMessageToHandler(AmqpMessageImpl vertxMessage, Handler<Message<JsonObject>> h) {
    h.handle(vertxMessage);
    vertxMessage.accept();
    receiver.flow(1);
  }

  private void scheduleBufferedMessageDelivery() {
    boolean schedule = false;

    synchronized (AmqpConsumerImpl.this) {
      schedule = !buffered.isEmpty() && !paused;
    }

    if (schedule) {
      bridge.runOnContext(false, v -> {
        Handler<Message<JsonObject>> h = null;
        AmqpMessageImpl message = null;

        synchronized (AmqpConsumerImpl.this) {
          h = handler;
          if (h != null && !paused) {
            message = buffered.poll();
          }
        }

        if (message != null) {
          // Delivering outside the synchronized block
          deliverMessageToHandler(message, h);

          // Schedule a delivery for a further buffered message if any
          scheduleBufferedMessageDelivery();
        }
      });
    }
  }

  @Override
  public synchronized MessageConsumer<JsonObject> exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  @Override
  public MessageConsumer<JsonObject> handler(final Handler<Message<JsonObject>> handler) {
    int creditToFlow = 0;
    boolean schedule = false;

    synchronized (AmqpConsumerImpl.this) {
      this.handler = handler;
      if (handler != null) {
        schedule = true;

        // Flow initial credit if needed
        if (!initialCreditGiven) {
          initialCreditGiven = true;
          creditToFlow = initialCredit;
        }
      }
    }

    if(creditToFlow > 0) {
      final int c = creditToFlow;
      bridge.runOnContext(true, v -> {
        receiver.flow(c);
      });
    }

    if(schedule) {
      scheduleBufferedMessageDelivery();
    }

    return this;
  }

  @Override
  public synchronized MessageConsumer<JsonObject> pause() {
    paused = true;
    return this;
  }

  @Override
  public synchronized MessageConsumer<JsonObject> resume() {
    paused = false;
    scheduleBufferedMessageDelivery();
    return this;
  }

  @Override
  public synchronized MessageConsumer<JsonObject> endHandler(Handler<Void> endHandler) {
    this.endHandler = endHandler;
    return this;
  }

  @Override
  public ReadStream<JsonObject> bodyStream() {
    return new BodyReadStream<>(this);
  }

  @Override
  public synchronized boolean isRegistered() {
    return handler != null;
  }

  @Override
  public String address() {
    return amqpAddress;
  }

  @Override
  public synchronized MessageConsumer<JsonObject> setMaxBufferedMessages(int maxBufferedMessages) {
    if(!initialCreditGiven) {
      initialCredit = maxBufferedMessages;
    }

    return this;
  }

  @Override
  public synchronized int getMaxBufferedMessages() {
    return initialCredit;
  }

  @Override
  public void completionHandler(Handler<AsyncResult<Void>> completionHandler) {
    throw new UnsupportedOperationException("Registration completion handler is not supported by this consumer");
  }

  @Override
  public synchronized void unregister() {
    unregister(null);
  }

  @Override
  public synchronized void unregister(Handler<AsyncResult<Void>> completionHandler) {
    handler = null;
    closed = true;

    bridge.runOnContext(true, v -> {
      if (completionHandler != null) {
        receiver.closeHandler((result) -> {
          if (result.succeeded()) {
            completionHandler.handle(Future.succeededFuture());
          } else {
            completionHandler.handle(Future.failedFuture(result.cause()));
          }
        });
      } else {
        receiver.closeHandler(null);
      }
      receiver.close();
    });
  }
}
