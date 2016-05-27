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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Source;

import io.vertx.amqp.bridge.Bridge;
import io.vertx.amqp.bridge.BridgeOptions;
import io.vertx.amqp.bridge.MessageHelper;
import io.vertx.amqp.bridge.impl.AmqpProducerImpl;
import io.vertx.amqp.bridge.impl.AmqpMessageImpl;
import io.vertx.amqp.bridge.impl.BridgeImpl;
import io.vertx.amqp.bridge.impl.MessageTranslatorImpl;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonDelivery;
import io.vertx.proton.ProtonReceiver;

public class BridgeImpl implements Bridge {

  private ProtonClient client;
  private ProtonConnection connection;
  private int replyToMsgIdIndex = 1;
  private ProtonReceiver replyToConsumer;
  private String replyToConsumerAddress;
  private AmqpProducerImpl replySender;
  private Map<String, Handler<?>> replyToMapping = new HashMap<>();
  private MessageTranslatorImpl translator = new MessageTranslatorImpl();
  private boolean disableReplyHandlerSupport = false;
  private BridgeOptions options;
  private Vertx vertx;

  public BridgeImpl(Vertx vertx, BridgeOptions options) {
    this.vertx = vertx;
    client = ProtonClient.create(vertx);
    this.options = options;
  }

  private static final Logger LOG = LoggerFactory.getLogger(BridgeImpl.class);

  @Override
  public Bridge start(String hostname, int port, Handler<AsyncResult<Void>> resultHandler) {
    return start(hostname, port, null, null, resultHandler);
  }

  @Override
  public Bridge start(String hostname, int port, String username, String password, Handler<AsyncResult<Void>> resultHandler) {
    client.connect(options, hostname, port, username, password, connectResult -> {
      if (connectResult.succeeded()) {
        connection = connectResult.result();

        LinkedHashMap<Symbol, Object> props = new LinkedHashMap<Symbol, Object>();
        props.put(BridgeMetaDataSupportImpl.PRODUCT_KEY, BridgeMetaDataSupportImpl.PRODUCT);
        props.put(BridgeMetaDataSupportImpl.VERSION_KEY, BridgeMetaDataSupportImpl.VERSION);
        connection.setProperties(props);

        connection.openHandler(openResult -> {
          LOG.trace("Bridge connection open complete");
          if (openResult.succeeded()) {
            if (disableReplyHandlerSupport) {
              resultHandler.handle(Future.succeededFuture());
              return;
            }

            // Create a reply sender
            replySender = new AmqpProducerImpl(this, connection, null);

            // Create a receiver, requesting a dynamic address, which we will inspect once attached and use as the
            // replyTo value on outgoing messages sent with replyHandler specified.
            replyToConsumer = connection.createReceiver(null);
            Source source = (Source) replyToConsumer.getSource();
            source.setDynamic(true);

            replyToConsumer.handler(this::handleIncomingMessageReply);
            replyToConsumer.openHandler(replyToConsumerResult -> {
              if (replyToConsumerResult.succeeded()) {
                Source remoteSource = (Source) replyToConsumer.getRemoteSource();
                if (remoteSource != null) {
                  replyToConsumerAddress = remoteSource.getAddress();
                }

                resultHandler.handle(Future.succeededFuture());
              } else {
                resultHandler.handle(Future.failedFuture(replyToConsumerResult.cause()));
              }
            }).open();
          } else {
            resultHandler.handle(Future.failedFuture(openResult.cause()));
          }
        }).open();
        connection.open();
      } else {
        resultHandler.handle(Future.failedFuture(connectResult.cause()));
      }
    });

    return this;
  }

  @Override
  public MessageConsumer<JsonObject> createConsumer(String amqpAddress) {
    return new AmqpConsumerImpl(vertx, this, connection, amqpAddress);
  }

  @Override
  public MessageProducer<JsonObject> createProducer(String amqpAddress) {
    return new AmqpProducerImpl(this, connection, amqpAddress);
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

  <R> void registerReplyToHandler(org.apache.qpid.proton.message.Message msg,
                                  Handler<AsyncResult<Message<R>>> replyHandler) {
    // TODO: complete, possibly do something nicer with the message generics in the producer

    if (replyToConsumerAddress == null) {
      throw new IllegalStateException("No reply-to address available, unable register reply handler");
    }

    msg.setReplyTo(replyToConsumerAddress);

    if (msg.getMessageId() == null) {
      String generatedMessageId = generateMessageId();
      msg.setMessageId(generatedMessageId);

      replyToMapping.put(generatedMessageId, replyHandler);
    } else {
      // TODO: use as-is? convert? fail? enforce it is always going to be string we/producer set?
      throw new IllegalStateException("Not yet implemented");
    }
  }

  private String generateMessageId() {
    // TODO generate a proper ID. Pure UUID string? Use some kind of connection prefix for trace?
    return "myMessageId-" + (replyToMsgIdIndex++);
  }

  private void handleIncomingMessageReply(ProtonDelivery delivery,
                                          org.apache.qpid.proton.message.Message protonMessage) {
    Object correlationId = protonMessage.getCorrelationId();
    if (correlationId != null) {
      // Remove the associated handler from the map (only 1 reply permitted).
      Handler<?> handler = replyToMapping.remove(correlationId);

      if (handler != null) {
        @SuppressWarnings("unchecked")
        Handler<AsyncResult<Message<JsonObject>>> h = (Handler<AsyncResult<Message<JsonObject>>>) handler;

        JsonObject body = translator.convertToJsonObject(protonMessage);
        Message<JsonObject> msg = new AmqpMessageImpl(body, BridgeImpl.this, protonMessage, delivery,
            replyToConsumerAddress, protonMessage.getReplyTo());

        AsyncResult<Message<JsonObject>> result = Future.succeededFuture(msg);
        h.handle(result);
        return;
      }
    }

    // TODO: either had no id, or no matching handler...handle the message in some way?
    LOG.error("Received message on replyTo consumer, could not match to a replyHandler: " + protonMessage);
  }

  <R> void sendReply(org.apache.qpid.proton.message.Message origIncomingMessage, Object replyMessageBody,
                     Handler<AsyncResult<Message<R>>> replyHandler) {
    // TODO enforce body type better
    JsonObject replyBody = (JsonObject) replyMessageBody;

    // TODO verify not null
    String replyAddress = origIncomingMessage.getReplyTo();

    // Set the correlationId to the messageId value if there was one, so the recipient reply handler can be found if it
    // is also a vertx amqp bridge
    Object origMessageId = origIncomingMessage.getMessageId();
    if (origMessageId != null) {
      JsonObject replyBodyProps = replyBody.getJsonObject(MessageHelper.PROPERTIES);
      if (replyBodyProps == null) {
        replyBodyProps = new JsonObject();
        replyBody.put(MessageHelper.PROPERTIES, replyBodyProps);
      }

      // TODO: preserve existing correlation-id if there was one?
      replyBodyProps.put(MessageHelper.PROPERTIES_CORRELATION_ID, origMessageId);
    } else {
      // TODO: Anything? Could just be a non-bridge recipient who didn't set one on their request
    }

    replySender.doSend(replyBody, replyHandler, replyAddress);
  }

  public Bridge setDisableReplyHandlerSupport(boolean disableReplyHandlerSupport) {
    this.disableReplyHandlerSupport = disableReplyHandlerSupport;
    return this;
  }
}
