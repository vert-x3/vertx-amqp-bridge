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
package io.vertx.amqpbridge.impl;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import io.vertx.amqpbridge.AmqpBridge;
import io.vertx.amqpbridge.AmqpBridgeOptions;
import io.vertx.amqpbridge.AmqpConstants;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.engine.EndpointState;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonDelivery;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.impl.ProtonConnectionImpl;

public class AmqpBridgeImpl implements AmqpBridge {

  private final Vertx vertx;
  private final Context bridgeContext;
  private final AmqpBridgeOptions options;
  private ProtonClient client;
  private ProtonConnection connection;
  private ProtonReceiver replyToConsumer;
  private String replyToConsumerAddress;
  private AmqpProducerImpl replySender;
  private Map<String, Handler<?>> replyToMapping = new ConcurrentHashMap<>();
  private MessageTranslatorImpl translator = new MessageTranslatorImpl();
  private AtomicBoolean started = new AtomicBoolean();
  private AtomicBoolean closed = new AtomicBoolean();
  private volatile Handler<Void> endHandler;

  public AmqpBridgeImpl(Vertx vertx, AmqpBridgeOptions options) {
    this.vertx = vertx;
    this.options = options;
    bridgeContext = vertx.getOrCreateContext();
  }

  private static final Logger LOG = LoggerFactory.getLogger(AmqpBridgeImpl.class);

  @Override
  public void start(String hostname, int port, Handler<AsyncResult<AmqpBridge>> resultHandler) {
    start(hostname, port, null, null, resultHandler);
  }

  @Override
  public void start(String hostname, int port, String username, String password,
                      Handler<AsyncResult<AmqpBridge>> resultHandler) {
    runOnContext(true, v -> {
      startImpl(hostname, port, username, password, resultHandler);
    });
  }

  private void startImpl(String hostname, int port, String username, String password,
                         Handler<AsyncResult<AmqpBridge>> resultHandler) {
    client = ProtonClient.create(vertx);
    client.connect(options, hostname, port, username, password, connectResult -> {
      if (connectResult.succeeded()) {
        connection = connectResult.result();

        LinkedHashMap<Symbol, Object> props = new LinkedHashMap<Symbol, Object>();
        props.put(BridgeMetaDataSupportImpl.PRODUCT_KEY, BridgeMetaDataSupportImpl.PRODUCT);
        props.put(BridgeMetaDataSupportImpl.VERSION_KEY, BridgeMetaDataSupportImpl.VERSION);
        connection.setProperties(props);

        if(options.getVhost() != null) {
          connection.setHostname(options.getVhost());
        }

        if(options.getContainerId() != null) {
          connection.setContainer(options.getContainerId());
        }

        connection.disconnectHandler(closeResult -> {
          endHandlerImpl();
        });

        connection.closeHandler(closeResult -> {
          try {
            disconnectImpl();
          } finally {
            endHandlerImpl();
          }
        });

        connection.openHandler(openResult -> {
          LOG.trace("Bridge connection open complete");
          if (openResult.succeeded()) {
            if (!options.isReplyHandlingSupport() || !connection.isAnonymousRelaySupported()) {
              started.set(true);
              resultHandler.handle(Future.succeededFuture(AmqpBridgeImpl.this));
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

                started.set(true);
                resultHandler.handle(Future.succeededFuture(AmqpBridgeImpl.this));
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
  }

  private void endHandlerImpl() {
    Handler<Void> handler = endHandler;
    endHandler = null;
    if (handler != null && !closed.get()) {
      handler.handle(null);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public MessageConsumer<JsonObject> createConsumer(String amqpAddress) {
    if (!started.get()) {
      throw new IllegalStateException("Bridge was not successfully started");
    }

    return new AmqpConsumerImpl(this, connection, amqpAddress);
  }

  @SuppressWarnings("unchecked")
  @Override
  public MessageProducer<JsonObject> createProducer(String amqpAddress) {
    if (!started.get()) {
      throw new IllegalStateException("Bridge was not successfully started");
    }

    return new AmqpProducerImpl(this, connection, amqpAddress);
  }

  @Override
  public void close(Handler<AsyncResult<Void>> resultHandler) {
    closed.set(true);
    runOnContext(true, v -> {
      shutdownImpl(resultHandler);
    });
  }

  private void shutdownImpl(Handler<AsyncResult<Void>> resultHandler) {
    if (connection != null) {
      if (isLocalOpen(connection) && isRemoteOpen(connection)) {
        connection.closeHandler(res -> {
          try {
            disconnectImpl();
          } finally {
            if (res.succeeded()) {
              resultHandler.handle(Future.succeededFuture());
            } else {
              resultHandler.handle(Future.failedFuture(res.cause()));
            }
          }
        }).close();
      } else {
        try {
          disconnectImpl();
        } finally {
          resultHandler.handle(Future.succeededFuture());
        }
      }
    } else {
      resultHandler.handle(Future.succeededFuture());
    }
  }

  private void disconnectImpl() {
    ProtonConnection conn = connection;
    connection = null;
    if (conn != null) {
      try {
        // Does nothing if already closed
        conn.close();
      } finally {
        conn.disconnect();
      }
    }
  }

  private boolean isLocalOpen(ProtonConnection connection) {
    return ((ProtonConnectionImpl) connection).getLocalState() == EndpointState.ACTIVE;
  }

  private boolean isRemoteOpen(ProtonConnection connection) {
    return ((ProtonConnectionImpl) connection).getRemoteState() == EndpointState.ACTIVE;
  }

  @Override
  public void endHandler(Handler<Void> endHandler) {
    this.endHandler = endHandler;
  }

  <R> void registerReplyToHandler(org.apache.qpid.proton.message.Message msg,
                                  Handler<AsyncResult<Message<R>>> replyHandler) {
    verifyReplyToAddressAvailable();
    msg.setReplyTo(replyToConsumerAddress);

    String generatedMessageId = UUID.randomUUID().toString();
    msg.setMessageId(generatedMessageId);

    replyToMapping.put(generatedMessageId, replyHandler);
  }

  void verifyReplyToAddressAvailable() throws IllegalStateException {
    if (replyToConsumerAddress == null) {
      throw new IllegalStateException(
          "No reply-to address available, unable to send with a reply handler. Try an explicit consumer for replies.");
    }
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
        Message<JsonObject> msg = new AmqpMessageImpl(body, AmqpBridgeImpl.this, protonMessage, delivery,
            replyToConsumerAddress, protonMessage.getReplyTo());

        AsyncResult<Message<JsonObject>> result = Future.succeededFuture(msg);
        h.handle(result);
        return;
      }
    }

    LOG.error("Received message on replyTo consumer, could not match to a replyHandler: " + protonMessage);
  }

  <R> void sendReply(org.apache.qpid.proton.message.Message origIncomingMessage, JsonObject replyBody,
                     Handler<AsyncResult<Message<R>>> replyHandler) {
    if(replySender == null) {
      throw new IllegalStateException(
          "No reply sender available, unable to send implicit replies. Try an explicit producer for replies.");
    }

    String replyAddress = origIncomingMessage.getReplyTo();
    if (replyAddress == null) {
      throw new IllegalStateException("Original message has no reply-to address, unable to send implicit reply");
    }

    // Set the correlationId to the messageId value if there was one, so that if the reply recipient is also a
    // vertx amqp bridge it can match the response to a reply handler if set when sending.
    Object origMessageId = origIncomingMessage.getMessageId();
    if (origMessageId != null) {
      JsonObject replyBodyProps = replyBody.getJsonObject(AmqpConstants.PROPERTIES);
      if (replyBodyProps == null) {
        replyBodyProps = new JsonObject();
        replyBody.put(AmqpConstants.PROPERTIES, replyBodyProps);
      }

      replyBodyProps.put(AmqpConstants.PROPERTIES_CORRELATION_ID, origMessageId);
    }

    replySender.doSend(replyBody, replyHandler, replyAddress);
  }

  boolean onContextEventLoop() {
    return ((ContextInternal) bridgeContext).nettyEventLoop().inEventLoop();
  }

  void runOnContext(boolean immediateIfOnContext, Handler<Void> action) {
    if (immediateIfOnContext && onContextEventLoop()) {
      action.handle(null);
    } else {
      bridgeContext.runOnContext(action);
    }
  }
}
