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
import io.vertx.proton.impl.ProtonSenderImpl;

public class AmqpProducerImpl implements MessageProducer<JsonObject> {

  private final ProtonSender sender;
  private final MessageTranslatorImpl translator = new MessageTranslatorImpl();
  private final AmqpBridgeImpl bridge;
  private final String amqpAddress;
  private boolean closed;
  private Handler<Throwable> exceptionHandler;
  private Handler<Void> drainHandler;
  private long remoteCredit = 0;

  public AmqpProducerImpl(AmqpBridgeImpl bridge, ProtonConnection connection, String amqpAddress) {
    if(!bridge.onContextEventLoop()) {
      throw new IllegalStateException("Should be executing on the bridge context thread");
    }

    this.bridge = bridge;
    this.amqpAddress= amqpAddress;

    sender = connection.createSender(amqpAddress);
    sender.closeHandler(res -> {
      Handler<Throwable> eh = null;
      boolean closeSender = false;

      synchronized (AmqpProducerImpl.this) {
        if (!closed && exceptionHandler != null) {
          eh = exceptionHandler;
        }

        if(!closed) {
          closed = true;
          closeSender = true;
        }
      }

      if (eh != null) {
        if (res.succeeded()) {
          eh.handle(new VertxException("Producer closed remotely"));
        } else {
          eh.handle(new VertxException("Producer closed remotely with error", res.cause()));
        }
      }

      if(closeSender) {
        sender.close();
      }
    });
    sender.sendQueueDrainHandler(s -> {
      Handler<Void> dh = null;
      synchronized (AmqpProducerImpl.this) {
        // Update current state of remote credit
        remoteCredit = ((ProtonSenderImpl) sender).getRemoteCredit();

        // check the user drain handler, fire it outside synchronized block if not null
        if (drainHandler != null) {
          dh = drainHandler;
        }
      }

      if(dh != null) {
        dh.handle(null);
      }
    });

    sender.open();
  }

  @Override
  public synchronized boolean writeQueueFull() {
    return remoteCredit <= 0;
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

    synchronized (AmqpProducerImpl.this) {
      // Update the credit tracking. We only need to adjust this here because the sends etc may not be on the context
      // thread and if that is the case we can't use the ProtonSender sendQueueFull method to check that credit has been
      // exhausted following this doSend call since we will have only scheduled the actual send for later.
      remoteCredit--;
    }

    bridge.runOnContext(true, v -> {
      if (replyHandler != null) {
        bridge.registerReplyToHandler(msg, replyHandler);
      }

      sender.send(msg);

      synchronized (AmqpProducerImpl.this) {
        // Update the credit tracking *again*. We need to reinitialise it here in case the doSend call was performed on
        // a thread other than the bridge context, to ensure we didn't fall foul of a race between the above pre-send
        // update on that thread, the above send on the context thread, and the sendQueueDrainHandler based updates on
        // the context thread.
        remoteCredit = ((ProtonSenderImpl) sender).getRemoteCredit();
      }
    });

    return this;
  }

  @Override
  public synchronized MessageProducer<JsonObject> exceptionHandler(Handler<Throwable> handler) {
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
  public synchronized MessageProducer<JsonObject> drainHandler(Handler<Void> handler) {
    drainHandler = handler;
    return this;
  }

  @Override
  public MessageProducer<JsonObject> deliveryOptions(DeliveryOptions options) {
    throw new UnsupportedOperationException("DeliveryOptions are not supported by this producer");
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
    synchronized (this) {
      closed = true;
    }

    bridge.runOnContext(true, v -> {
      sender.close();
    });
  }
}
