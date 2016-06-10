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
package io.vertx.amqpbridge;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.amqpbridge.impl.AmqpBridgeImpl;
import io.vertx.amqpbridge.impl.BridgeMetaDataSupportImpl;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.ReadStream;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonHelper;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;
import io.vertx.proton.impl.ProtonSenderImpl;

@RunWith(VertxUnitRunner.class)
public class AmqpBridgeTest extends ActiveMQTestBase {

  private static Logger LOG = LoggerFactory.getLogger(AmqpBridgeTest.class);

  private Vertx vertx;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    vertx = Vertx.vertx();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    try {
      super.tearDown();
    } finally {
      if (vertx != null) {
        vertx.close();
      }
    }
  }

  @Test(timeout = 20000)
  public void testBasicStartup(TestContext context) throws Exception {

    context.assertEquals(0L, getBrokerAdminView(context).getTotalConnectionsCount(),
        "unexpected total connection count before");
    context.assertEquals(0, getBrokerAdminView(context).getCurrentConnectionsCount(),
        "unexpected current connection count before");

    Async async = context.async();

    AmqpBridge bridge = AmqpBridge.create(vertx);
    bridge.start("localhost", getBrokerAmqpConnectorPort(), res -> {
      LOG.trace("Startup complete");
      context.assertTrue(res.succeeded());

      context.assertEquals(1L, getBrokerAdminView(context).getTotalConnectionsCount(),
          "unexpected total connection count during");
      context.assertEquals(1, getBrokerAdminView(context).getCurrentConnectionsCount(),
          "unexpected current connection count during");

      bridge.close(shutdownRes -> {
        LOG.trace("Shutdown complete");
        context.assertTrue(shutdownRes.succeeded());

        context.assertEquals(1L, getBrokerAdminView(context).getTotalConnectionsCount(),
            "unexpected total connection count after");
        context.assertEquals(0, getBrokerAdminView(context).getCurrentConnectionsCount(),
            "unexpected current connection count after");

        async.complete();
      });
    });

    async.awaitSuccess();
  }

  private BrokerView getBrokerAdminView(TestContext context) {
    try {
      return getBrokerService().getAdminView();
    } catch (Exception e) {
      context.fail(e);
      // Above line throws, but satisfy the compiler.
      return null;
    }
  }

  @Test(timeout = 20000)
  public void testConnectionMetaData(TestContext context) throws Exception {
    stopBroker();

    Async asyncMetaData = context.async();
    Async asyncShutdown = context.async();
    MockServer server = new MockServer(vertx, serverConnection -> {
      serverConnection.closeHandler(x -> {
        serverConnection.close();
      });

      serverConnection.openHandler(x -> {
        // Open the connection.
        serverConnection.open();

        // Validate the properties separately.
        Map<Symbol, Object> properties = serverConnection.getRemoteProperties();

        context.assertNotNull(properties, "connection properties not present");

        context.assertTrue(properties.containsKey(BridgeMetaDataSupportImpl.PRODUCT_KEY),
            "product property key not present");
        context.assertEquals(BridgeMetaDataSupportImpl.PRODUCT, properties.get(BridgeMetaDataSupportImpl.PRODUCT_KEY),
            "unexpected product property value");

        context.assertTrue(properties.containsKey(BridgeMetaDataSupportImpl.VERSION_KEY),
            "version property key not present");
        context.assertEquals(BridgeMetaDataSupportImpl.VERSION, properties.get(BridgeMetaDataSupportImpl.VERSION_KEY),
            "unexpected version property value");

        asyncMetaData.complete();
      });
    });

    AmqpBridge bridge = AmqpBridge.create(vertx);
    ((AmqpBridgeImpl) bridge).setReplyHandlerSupported(false);
    bridge.start("localhost", server.actualPort(), res -> {
      LOG.trace("Startup complete");
      asyncMetaData.awaitSuccess();

      LOG.trace("Shutting down");
      bridge.close(shutdownRes -> {
        LOG.trace("Shutdown complete");
        context.assertTrue(shutdownRes.succeeded());
        asyncShutdown.complete();
      });
    });

    try {
      asyncShutdown.awaitSuccess();
    } finally {
      server.close();
    }
  }

  @Test(timeout = 20000)
  public void testReceiveBasicMessage(TestContext context) throws Exception {
    String testName = getTestName();
    String sentContent = "myMessageContent-" + testName;

    Async asyncShutdown = context.async();
    Async asyncSendMsg = context.async();

    int port = getBrokerAmqpConnectorPort();

    AmqpBridge bridge = AmqpBridge.create(vertx);
    bridge.start("localhost", port, res -> {
      LOG.trace("Startup complete");

      // Set up a consumer using the bridge
      MessageConsumer<JsonObject> consumer = bridge.<JsonObject>createConsumer(testName).handler(msg -> {
        JsonObject jsonObject = msg.body();
        context.assertNotNull(jsonObject, "message jsonObject body was null");

        Object amqpBodyContent = jsonObject.getValue(AmqpConstants.BODY);
        context.assertNotNull(amqpBodyContent, "amqp message body content was null");

        context.assertEquals(sentContent, amqpBodyContent, "amqp message body was not as expected");

        LOG.trace("Shutting down");
        bridge.close(shutdownRes -> {
          LOG.trace("Shutdown complete");
          context.assertTrue(shutdownRes.succeeded());
          asyncShutdown.complete();
        });
      });

      context.assertEquals(testName, consumer.address(), "address was not as expected");
    });

    // Send it a message from a regular AMQP client
    ProtonClient client = ProtonClient.create(vertx);
    client.connect("localhost", port, res -> {
      context.assertTrue(res.succeeded());

      org.apache.qpid.proton.message.Message protonMsg = Proton.message();
      protonMsg.setBody(new AmqpValue(sentContent));

      ProtonConnection conn = res.result().open();

      ProtonSender sender = conn.createSender(testName).open();
      sender.send(protonMsg, delivery -> {
        context.assertNotNull(delivery.getRemoteState(), "message had no remote state");
        context.assertTrue(delivery.getRemoteState() instanceof Accepted, "message was not accepted");
        context.assertTrue(delivery.remotelySettled(), "message was not settled");

        conn.closeHandler(closeResult -> {
          conn.disconnect();
        }).close();

        asyncSendMsg.complete();
      });
    });

    asyncSendMsg.awaitSuccess();
    asyncShutdown.awaitSuccess();
  }

  @Test(timeout = 20000)
  public void testReceiveBasicMessageAsBodyStream(TestContext context) throws Exception {
    String testName = getTestName();
    String sentContent = "myMessageContent-" + testName;

    Async asyncShutdown = context.async();
    Async asyncSendMsg = context.async();

    int port = getBrokerAmqpConnectorPort();

    AmqpBridge bridge = AmqpBridge.create(vertx);
    bridge.start("localhost", port, res -> {
      LOG.trace("Startup complete");

      // Set up a read stream using the bridge
      ReadStream<JsonObject> stream = bridge.<JsonObject>createConsumer(testName).bodyStream();
      stream.handler(jsonObject -> {
        context.assertNotNull(jsonObject, "jsonObject was null");

        Object amqpBodyContent = jsonObject.getValue(AmqpConstants.BODY);
        context.assertNotNull(amqpBodyContent, "amqp message body content was null");

        context.assertEquals(sentContent, amqpBodyContent, "amqp message body was not as expected");

        LOG.trace("Shutting down");
        bridge.close(shutdownRes -> {
          LOG.trace("Shutdown complete");
          context.assertTrue(shutdownRes.succeeded());
          asyncShutdown.complete();
        });
      });
    });

    // Send it a message from a regular AMQP client
    ProtonClient client = ProtonClient.create(vertx);
    client.connect("localhost", port, res -> {
      context.assertTrue(res.succeeded());

      org.apache.qpid.proton.message.Message protonMsg = Proton.message();
      protonMsg.setBody(new AmqpValue(sentContent));

      ProtonConnection conn = res.result().open();

      ProtonSender sender = conn.createSender(testName).open();
      sender.send(protonMsg, delivery -> {
        context.assertNotNull(delivery.getRemoteState(), "message had no remote state");
        context.assertTrue(delivery.getRemoteState() instanceof Accepted, "message was not accepted");
        context.assertTrue(delivery.remotelySettled(), "message was not settled");

        conn.closeHandler(closeResult -> {
          conn.disconnect();
        }).close();

        asyncSendMsg.complete();
      });
    });

    asyncSendMsg.awaitSuccess();
    asyncShutdown.awaitSuccess();
  }

  @Test(timeout = 20000)
  public void testSendBasicMessage(TestContext context) throws Exception {
    String testName = getTestName();
    String sentContent = "myMessageContent-" + testName;

    Async asyncRecvMsg = context.async();

    int port = getBrokerAmqpConnectorPort();

    AmqpBridge bridge = AmqpBridge.create(vertx);
    bridge.start("localhost", port, res -> {
      // Set up a sender using the bridge
      context.assertTrue(res.succeeded());

      MessageProducer<JsonObject> producer = bridge.createProducer(testName);

      JsonObject body = new JsonObject();
      body.put("body", sentContent);

      producer.send(body);

      context.assertEquals(testName, producer.address(), "address was not as expected");
    });

    // Receive it with a regular AMQP client
    ProtonClient client = ProtonClient.create(vertx);
    client.connect("localhost", port, res -> {
      context.assertTrue(res.succeeded());

      org.apache.qpid.proton.message.Message protonMsg = Proton.message();
      protonMsg.setBody(new AmqpValue(sentContent));

      ProtonConnection conn = res.result().open();

      ProtonReceiver receiver = conn.createReceiver(testName);
      receiver.handler((d, m) -> {
        Section body = m.getBody();
        context.assertNotNull(body);
        context.assertTrue(body instanceof AmqpValue);
        Object actual = ((AmqpValue) body).getValue();

        context.assertEquals(sentContent, actual, "Unexpected message body");
        asyncRecvMsg.complete();

        conn.closeHandler(closeResult -> {
          conn.disconnect();
        }).close();
      }).open();
    });

    asyncRecvMsg.awaitSuccess();
  }

  @Test(timeout = 20000)
  public void testBasicRequestReply(TestContext context) {
    Async asyncRequest = context.async();
    Async asyncShutdown = context.async();

    String destinationName = getTestName();
    String content = "myStringContent";
    String replyContent = "myStringReply";

    AmqpBridge bridge = AmqpBridge.create(vertx);
    bridge.start("localhost", getBrokerAmqpConnectorPort(), startResult -> {
      context.assertTrue(startResult.succeeded());

      MessageProducer<JsonObject> producer = bridge.createProducer(destinationName);

      JsonObject body = new JsonObject();
      body.put(AmqpConstants.BODY, content);

      producer.<JsonObject> send(body, reply -> {
        LOG.trace("Sender got reply");
        context.assertEquals(replyContent, reply.result().body().getValue(AmqpConstants.BODY),
            "unexpected reply msg content");
        context.assertNotNull(reply.result().address(), "address was not set on reply");
        context.assertNull(reply.result().replyAddress(), "reply address was unexpectedly set on the reply");

        LOG.trace("Shutting down");
        bridge.close(shutdownRes -> {
          LOG.trace("Shutdown complete");
          context.assertTrue(shutdownRes.succeeded());
          asyncShutdown.complete();
        });
      });
      LOG.trace("Client sent msg");

      MessageConsumer<JsonObject> consumer = bridge.createConsumer(destinationName);
      consumer.handler(msg -> {
        JsonObject receivedMsgBody = msg.body();
        LOG.trace("Consumer got request msg: " + receivedMsgBody);

        context.assertNotNull(receivedMsgBody, "expected msg body but none found");
        context.assertEquals(content, receivedMsgBody.getValue(AmqpConstants.BODY), "unexpected msg content");
        context.assertNotNull(msg.replyAddress(), "reply address was not set on the request");

        JsonObject replyBody = new JsonObject();
        replyBody.put(AmqpConstants.BODY, replyContent);

        msg.reply(replyBody);

        asyncRequest.complete();
      });
    });

    asyncRequest.awaitSuccess();
    asyncShutdown.awaitSuccess();
  }

  @Test(timeout = 20000)
  public void testReplyToOriginalReply(TestContext context) {
    Async requestReceivedAsync = context.async();
    Async replyRecievedAsync = context.async();
    Async shutdownAsync = context.async();

    String destinationName = getTestName();
    String content = "myStringContent";
    String replyContent = "myStringReply";
    String replyToReplyContent = "myStringReplyToReply";

    AmqpBridge bridge = AmqpBridge.create(vertx);
    bridge.start("localhost", getBrokerAmqpConnectorPort(), startResult -> {
      context.assertTrue(startResult.succeeded());

      MessageProducer<JsonObject> producer = bridge.createProducer(destinationName);

      JsonObject body = new JsonObject();
      body.put(AmqpConstants.BODY, content);

      producer.<JsonObject> send(body, reply -> {
        LOG.trace("Sender got first reply");
        Message<JsonObject> replyMessage = reply.result();
        context.assertEquals(replyContent, replyMessage.body().getValue(AmqpConstants.BODY),
            "unexpected reply msg content");
        context.assertNotNull(replyMessage.address(), "address was not set on the reply");
        context.assertNotNull(replyMessage.replyAddress(), "reply address was not set on the reply");

        replyRecievedAsync.complete();

        JsonObject replyToReplyBody = new JsonObject();
        replyToReplyBody.put(AmqpConstants.BODY, replyToReplyContent);

        replyMessage.reply(replyToReplyBody);
      });
      LOG.trace("Client sent msg");

      MessageConsumer<JsonObject> consumer = bridge.createConsumer(destinationName);
      consumer.handler(msg -> {
        JsonObject receivedMsgBody = msg.body();
        LOG.trace("Receiver got request: " + receivedMsgBody);

        context.assertNotNull(receivedMsgBody, "expected msg body but none found");
        context.assertEquals(content, receivedMsgBody.getValue(AmqpConstants.BODY), "unexpected msg content");
        context.assertNotNull(msg.replyAddress(), "reply address was not set on the request");

        JsonObject replyBody = new JsonObject();
        replyBody.put(AmqpConstants.BODY, replyContent);

        msg.<JsonObject> reply(replyBody, replyToReply -> {
          LOG.trace("Receiver got reply to reply");
          Message<JsonObject> replyToReplyMessage = replyToReply.result();
          context.assertEquals(replyToReplyContent, replyToReplyMessage.body().getValue(AmqpConstants.BODY),
              "unexpected 2nd reply msg content");
          context.assertNull(replyToReplyMessage.replyAddress(), "reply address was unexpectedly set on 2nd reply");

          LOG.trace("Shutting down");
          bridge.close(shutdownRes -> {
            LOG.trace("Shutdown complete");
            context.assertTrue(shutdownRes.succeeded());
            shutdownAsync.complete();
          });
        });

        requestReceivedAsync.complete();
      });
    });

    requestReceivedAsync.awaitSuccess();
    replyRecievedAsync.awaitSuccess();
    shutdownAsync.awaitSuccess();
  }

  @Test(timeout = 20000)
  public void testReceiveMultipleMessageAfterDelayedHandlerAddition(TestContext context) throws Exception {
    String testName = getTestName();
    String sentContent = "myMessageContent-" + testName;

    Async asyncShutdown = context.async();
    Async asyncSendMsg = context.async();

    int port = getBrokerAmqpConnectorPort();
    int msgCount = 5;

    // Send some message from a regular AMQP client
    ProtonClient client = ProtonClient.create(vertx);
    client.connect("localhost", port, res -> {
      context.assertTrue(res.succeeded());

      org.apache.qpid.proton.message.Message protonMsg = Proton.message();
      protonMsg.setBody(new AmqpValue(sentContent));

      ProtonConnection conn = res.result().open();

      ProtonSender sender = conn.createSender(testName).open();
      for (int i = 1; i <= msgCount; i++) {
        final int msgNum = i;
        sender.send(protonMsg, delivery -> {
          LOG.trace("Running onUpdated for sent message " + msgNum);
          context.assertNotNull(delivery.getRemoteState(), "message " + msgNum + " had no remote state");
          context.assertTrue(delivery.getRemoteState() instanceof Accepted, "message " + msgNum + " was not accepted");
          context.assertTrue(delivery.remotelySettled(), "message " + msgNum + " was not settled");

          if (msgNum == msgCount) {
            conn.closeHandler(closeResult -> {
              conn.disconnect();
            }).close();
            asyncSendMsg.complete();
          }
        });
      }
    });

    AmqpBridge bridge = AmqpBridge.create(vertx);
    bridge.start("localhost", port, res -> {
      LOG.trace("Startup complete");

      // Set up a consumer using the bridge but DONT register the handler
      MessageConsumer<JsonObject> consumer = bridge.createConsumer(testName);

      // Add the handler after a delay
      vertx.setTimer(500, x -> {
        AtomicInteger received = new AtomicInteger();
        consumer.handler(msg -> {
          int msgNum = received.incrementAndGet();
          LOG.trace("Received message " + msgNum);

          JsonObject jsonObject = msg.body();
          context.assertNotNull(jsonObject, "message " + msgNum + " jsonObject body was null");

          Object amqpBodyContent = jsonObject.getValue(AmqpConstants.BODY);
          context.assertNotNull(amqpBodyContent, "amqp message " + msgNum + " body content was null");

          context.assertEquals(sentContent, amqpBodyContent, "amqp message " + msgNum + " body not as expected");

          if (msgNum == msgCount) {
            LOG.trace("Shutting down");
            bridge.close(shutdownRes -> {
              LOG.trace("Shutdown complete");
              context.assertTrue(shutdownRes.succeeded());
              asyncShutdown.complete();
            });
          }
        });
      });
    });

    asyncSendMsg.awaitSuccess();
    asyncShutdown.awaitSuccess();
  }

  @Test(timeout = 20000)
  public void testReceiveMultipleMessageAfterPause(TestContext context) throws Exception {
    String testName = getTestName();
    String sentContent = "myMessageContent-" + testName;

    Async asyncShutdown = context.async();
    Async asyncSendMsg = context.async();

    final int port = getBrokerAmqpConnectorPort();
    final int pauseCount = 2;
    final int totalMsgCount = 5;
    final int delay = 500;

    // Send some message from a regular AMQP client
    ProtonClient client = ProtonClient.create(vertx);
    client.connect("localhost", port, res -> {
      context.assertTrue(res.succeeded());

      org.apache.qpid.proton.message.Message protonMsg = Proton.message();
      protonMsg.setBody(new AmqpValue(sentContent));

      ProtonConnection conn = res.result().open();

      ProtonSender sender = conn.createSender(testName).open();
      for (int i = 1; i <= totalMsgCount; i++) {
        final int msgNum = i;
        sender.send(protonMsg, delivery -> {
          LOG.trace("Running onUpdated for sent message " + msgNum);
          context.assertNotNull(delivery.getRemoteState(), "message " + msgNum + " had no remote state");
          context.assertTrue(delivery.getRemoteState() instanceof Accepted, "message " + msgNum + " was not accepted");
          context.assertTrue(delivery.remotelySettled(), "message " + msgNum + " was not settled");

          if (msgNum == totalMsgCount) {
            conn.closeHandler(closeResult -> {
              conn.disconnect();
            }).close();
            asyncSendMsg.complete();
          }
        });
      }
    });

    AmqpBridge bridge = AmqpBridge.create(vertx);
    bridge.start("localhost", port, res -> {
      LOG.trace("Startup complete");

      final AtomicInteger received = new AtomicInteger();
      final AtomicLong pauseStartTime = new AtomicLong();

      // Set up a consumer using the bridge
      MessageConsumer<JsonObject> consumer = bridge.createConsumer(testName);
      consumer.handler(msg -> {
        int msgNum = received.incrementAndGet();
        LOG.trace("Received message " + msgNum);

        JsonObject jsonObject = msg.body();
        context.assertNotNull(jsonObject, "message " + msgNum + " jsonObject body was null");

        Object amqpBodyContent = jsonObject.getValue(AmqpConstants.BODY);
        context.assertNotNull(amqpBodyContent, "amqp message " + msgNum + " body content was null");

        context.assertEquals(sentContent, amqpBodyContent, "amqp message " + msgNum + " body not as expected");

        // Pause once we get initial messages
        if (msgNum == pauseCount) {
          LOG.trace("Pausing");
          consumer.pause();

          // Resume after a delay
          pauseStartTime.set(System.currentTimeMillis());
          vertx.setTimer(delay, x -> {
            LOG.trace("Resuming");
            consumer.resume();
          });
        }

        // Verify subsequent deliveries occur after the expected delay
        if (msgNum > pauseCount) {
          context.assertTrue(pauseStartTime.get() > 0, "pause start not initialised before receiving msg" + msgNum);
          context.assertTrue(System.currentTimeMillis() + delay > pauseStartTime.get(),
              "delivery occurred before expected");
        }

        if (msgNum == totalMsgCount) {
          LOG.trace("Shutting down");
          bridge.close(shutdownRes -> {
            LOG.trace("Shutdown complete");
            context.assertTrue(shutdownRes.succeeded());
            asyncShutdown.complete();
          });
        }
      });
    });

    asyncSendMsg.awaitSuccess();
    asyncShutdown.awaitSuccess();
  }

  @Test(timeout = 20000)
  public void testProducerClose(TestContext context) throws Exception {
    doProducerCloseTestImpl(context, false);
  }

  @Test(timeout = 20000)
  public void testProducerEnd(TestContext context) throws Exception {
    doProducerCloseTestImpl(context, true);
  }

  private void doProducerCloseTestImpl(TestContext context, boolean callEnd) throws Exception {
    stopBroker();

    final String testName = getTestName();
    final String sentContent = "myMessageContent-" + testName;

    final Async asyncRecieveMsg = context.async();
    final Async asyncClose = context.async();
    final Async asyncShutdown = context.async();

    final AtomicBoolean exceptionHandlerCalled = new AtomicBoolean();

    // === Server handling ====

    MockServer server = new MockServer(vertx, serverConnection -> {
      // Expect a connection
      serverConnection.openHandler(serverSender -> {
        LOG.trace("Server connection open");
        // Add a close handler
        serverConnection.closeHandler(x -> {
          serverConnection.close();
        });

        serverConnection.open();
      });

      // Expect a session to open, when the producer is created
      serverConnection.sessionOpenHandler(serverSession -> {
        LOG.trace("Server session open");
        serverSession.open();
      });

      // Expect a receiver link open for the producer
      serverConnection.receiverOpenHandler(serverReceiver -> {
        LOG.trace("Server receiver open");

        Target remoteTarget = (Target) serverReceiver.getRemoteTarget();
        context.assertNotNull(remoteTarget, "target should not be null");
        context.assertEquals(testName, remoteTarget.getAddress(), "expected given address");
        // Naive test-only handling
        serverReceiver.setTarget(remoteTarget.copy());

        // Add the message handler
        serverReceiver.handler((delivery, message) -> {
          Section body = message.getBody();
          context.assertNotNull(body, "received body was null");
          context.assertTrue(body instanceof AmqpValue, "unexpected body section type: " + body.getClass());
          context.assertEquals(sentContent, ((AmqpValue) body).getValue(), "Unexpected message body content");

          asyncRecieveMsg.complete();
        });

        // Add a close handler
        serverReceiver.closeHandler(x -> {
          serverReceiver.close();
          asyncClose.complete();
        });

        serverReceiver.open();
      });
    });

    // === Bridge producer handling ====

    AmqpBridge bridge = AmqpBridge.create(vertx);
    ((AmqpBridgeImpl) bridge).setReplyHandlerSupported(false);
    bridge.start("localhost", server.actualPort(), res -> {
      // Set up a producer using the bridge, use it, close it.
      context.assertTrue(res.succeeded());

      MessageProducer<JsonObject> producer = bridge.createProducer(testName);
      producer.exceptionHandler(x -> {
        exceptionHandlerCalled.set(true);
      });

      JsonObject body = new JsonObject();
      body.put("body", sentContent);

      producer.send(body);

      if (callEnd) {
        producer.end();
      } else {
        producer.close();
      }

      bridge.close(shutdownRes -> {
        LOG.trace("Shutdown complete");
        context.assertTrue(shutdownRes.succeeded());
        asyncShutdown.complete();
      });
    });

    try {
      asyncRecieveMsg.awaitSuccess();
      asyncClose.awaitSuccess();
      asyncShutdown.awaitSuccess();
      context.assertFalse(exceptionHandlerCalled.get(), "exception handler unexpectedly called");
    } finally {
      server.close();
    }
  }

  @Test(timeout = 20000)
  public void testConsumerUnregisterCompletionNotification(TestContext context) throws Exception {
    stopBroker();

    final String testName = getTestName();
    final String sentContent = "myMessageContent-" + testName;

    final Async asyncUnregister = context.async();
    final Async asyncShutdown = context.async();

    final AtomicBoolean exceptionHandlerCalled = new AtomicBoolean();

    MockServer server = new MockServer(vertx,
        serverConnection -> handleReceiverOpenSendMessageThenClose(serverConnection, testName, sentContent, context));

    AmqpBridge bridge = AmqpBridge.create(vertx);
    ((AmqpBridgeImpl) bridge).setReplyHandlerSupported(false);
    bridge.start("localhost", server.actualPort(), res -> {
      LOG.trace("Startup complete");

      MessageConsumer<JsonObject> consumer = bridge.createConsumer(testName);
      context.assertFalse(consumer.isRegistered(), "expected registered to be false");
      consumer.exceptionHandler(x -> {
        exceptionHandlerCalled.set(true);
      });
      consumer.handler(msg -> {
        // Received message, verify it
        JsonObject jsonObject = msg.body();
        context.assertNotNull(jsonObject, "message jsonObject body was null");
        Object amqpBodyContent = jsonObject.getValue(AmqpConstants.BODY);
        context.assertNotNull(amqpBodyContent, "amqp message body content was null");
        context.assertEquals(sentContent, amqpBodyContent, "amqp message body not as expected");

        // Unregister
        consumer.unregister(unregisterResult -> {
          LOG.trace("Unregister completed");
          context.assertTrue(unregisterResult.succeeded(), "Expected unregistration to succeed");
          asyncUnregister.complete();

          // Shut down
          LOG.trace("Shutting down");
          bridge.close(shutdownRes -> {
            LOG.trace("Shutdown complete");
            context.assertTrue(shutdownRes.succeeded());
            asyncShutdown.complete();
          });
        });
      });
      context.assertTrue(consumer.isRegistered(), "expected registered to be true");
    });

    try {
      context.assertFalse(exceptionHandlerCalled.get(), "exception handler unexpectedly called");
      asyncUnregister.awaitSuccess();
      asyncShutdown.awaitSuccess();
    } finally {
      server.close();
    }
  }

  private void handleReceiverOpenSendMessageThenClose(ProtonConnection serverConnection, String testAddress,
                                                      String testContent, TestContext context) {
    // Expect a connection
    serverConnection.openHandler(serverSender -> {
      LOG.trace("Server connection open");
      // Add a close handler
      serverConnection.closeHandler(x -> {
        serverConnection.close();
      });

      serverConnection.open();
    });

    // Expect a session to open, when the receiver is created
    serverConnection.sessionOpenHandler(serverSession -> {
      LOG.trace("Server session open");
      serverSession.open();
    });

    // Expect a sender link open for the receiver
    serverConnection.senderOpenHandler(serverSender -> {
      LOG.trace("Server sender open");
      Source remoteSource = (Source) serverSender.getRemoteSource();
      context.assertNotNull(remoteSource, "source should not be null");
      context.assertEquals(testAddress, remoteSource.getAddress(), "expected given address");
      // Naive test-only handling
      serverSender.setSource(remoteSource.copy());

      // Assume we will get credit, buffer the send immediately
      org.apache.qpid.proton.message.Message protonMsg = Proton.message();
      protonMsg.setBody(new AmqpValue(testContent));

      serverSender.send(protonMsg);

      // Add a close handler
      serverSender.closeHandler(x -> {
        serverSender.close();
      });

      serverSender.open();
    });
  }

  @Test(timeout = 20000)
  public void testSenderFlowControlMechanisms(TestContext context) throws Exception {
    stopBroker();

    final long delay = 250;
    final String testName = getTestName();
    final String sentContent = "myMessageContent-" + testName;

    final Async asyncSendInitialCredit = context.async();
    final Async asyncSendSubsequentCredit = context.async();
    final Async asyncShutdown = context.async();

    // === Server handling ====

    MockServer server = new MockServer(vertx, serverConnection -> {
      // Expect a connection
      serverConnection.openHandler(serverSender -> {
        LOG.trace("Server connection open");
        // Add a close handler
        serverConnection.closeHandler(x -> {
          serverConnection.close();
        });

        serverConnection.open();
      });

      // Expect a session to open, when the sender is created
      serverConnection.sessionOpenHandler(serverSession -> {
        LOG.trace("Server session open");
        serverSession.open();
      });

      // Expect a receiver link open for the sender
      serverConnection.receiverOpenHandler(serverReceiver -> {
        LOG.trace("Server receiver open");

        Target remoteTarget = (Target) serverReceiver.getRemoteTarget();
        context.assertNotNull(remoteTarget, "target should not be null");
        context.assertEquals(testName, remoteTarget.getAddress(), "expected given address");
        // Naive test-only handling
        serverReceiver.setTarget(remoteTarget.copy());

        // Disable auto accept and credit prefetch handling, do it (or not) ourselves
        serverReceiver.setAutoAccept(false);
        serverReceiver.setPrefetch(0);

        // Add the message handler
        serverReceiver.handler((delivery, message) -> {
          Section body = message.getBody();
          context.assertNotNull(body, "received body was null");
          context.assertTrue(body instanceof AmqpValue, "unexpected body section type: " + body.getClass());
          context.assertEquals(sentContent, ((AmqpValue) body).getValue(), "Unexpected message body content");

          // Only flow subsequent credit after a delay and related checks complete
          vertx.setTimer(delay, x -> {
            asyncSendSubsequentCredit.awaitSuccess();
            LOG.trace("Sending subsequent credit after delay");
            serverReceiver.flow(1);
          });
        });

        // Add a close handler
        serverReceiver.closeHandler(x -> {
          serverReceiver.close();
        });

        serverReceiver.open();

        // Only flow initial credit after a delay and initial checks complete
        vertx.setTimer(delay, x -> {
          asyncSendInitialCredit.awaitSuccess();
          LOG.trace("Sending credit after delay");
          serverReceiver.flow(1);
        });
      });
    });

    // === Bridge producer handling ====

    AmqpBridge bridge = AmqpBridge.create(vertx);
    ((AmqpBridgeImpl) bridge).setReplyHandlerSupported(false);

    bridge.start("localhost", server.actualPort(), res -> {
      context.assertTrue(res.succeeded());

      MessageProducer<JsonObject> producer = bridge.createProducer(testName);

      context.assertTrue(producer.writeQueueFull(), "expected write queue to be full, we have not yet granted credit");
      producer.drainHandler(x -> {
        context.assertTrue(asyncSendInitialCredit.isSucceeded(), "should have been called after initial credit delay");
        context.assertFalse(producer.writeQueueFull(), "expected write queue not to be full, we just granted credit");

        // Send message using the credit
        JsonObject body = new JsonObject();
        body.put("body", sentContent);

        producer.send(body);

        context.assertTrue(producer.writeQueueFull(), "expected write queue to be full, we just used all the credit");

        // Now replace the drain handler, have it act on subsequent credit arriving
        producer.drainHandler(y -> {
          context.assertTrue(asyncSendSubsequentCredit.isSucceeded(), "should have been called after 2nd credit delay");
          context.assertFalse(producer.writeQueueFull(), "expected write queue not to be full, we just granted credit");

          LOG.trace("Shutting down");
          bridge.close(shutdownRes -> {
            LOG.trace("Shutdown complete");
            context.assertTrue(shutdownRes.succeeded());
            asyncShutdown.complete();
          });
        });

        // Now allow server to send the subsequent credit
        asyncSendSubsequentCredit.complete();
      });

      // Now allow to send initial credit. Things will kick off again in the drain handler.
      asyncSendInitialCredit.complete();
    });

    try {
      asyncShutdown.awaitSuccess();
    } finally {
      server.close();
    }
  }

  @Test(timeout = 20000)
  public void testSenderClosedRemotelyCallsExceptionHandler(TestContext context) throws Exception {
    doSenderClosedRemotelyCallsExceptionHandlerTestImpl(context, false);
  }

  @Test(timeout = 20000)
  public void testSenderClosedRemotelyWithErrorCallsExceptionHandler(TestContext context) throws Exception {
    doSenderClosedRemotelyCallsExceptionHandlerTestImpl(context, true);
  }

  private void doSenderClosedRemotelyCallsExceptionHandlerTestImpl(TestContext context,
                                                                   boolean closeWithError) throws Exception {
    stopBroker();

    final String testName = getTestName();
    final String sentContent = "myMessageContent-" + testName;

    final Async asyncShutdown = context.async();
    final Async asyncExceptionHandlerCalled = context.async();

    // === Server handling ====

    MockServer server = new MockServer(vertx, serverConnection -> {
      // Expect a connection
      serverConnection.openHandler(serverSender -> {
        LOG.trace("Server connection open");
        // Add a close handler
        serverConnection.closeHandler(x -> {
          serverConnection.close();
        });

        serverConnection.open();
      });

      // Expect a session to open, when the sender is created
      serverConnection.sessionOpenHandler(serverSession -> {
        LOG.trace("Server session open");
        serverSession.open();
      });

      // Expect a receiver link open for the sender
      serverConnection.receiverOpenHandler(serverReceiver -> {
        LOG.trace("Server receiver open");

        Target remoteTarget = (Target) serverReceiver.getRemoteTarget();
        context.assertNotNull(remoteTarget, "target should not be null");
        context.assertEquals(testName, remoteTarget.getAddress(), "expected given address");
        // Naive test-only handling
        serverReceiver.setTarget(remoteTarget.copy());

        // Add the message handler
        serverReceiver.handler((delivery, message) -> {
          Section body = message.getBody();
          context.assertNotNull(body, "received body was null");
          context.assertTrue(body instanceof AmqpValue, "unexpected body section type: " + body.getClass());
          context.assertEquals(sentContent, ((AmqpValue) body).getValue(), "Unexpected message body content");

          if (closeWithError) {
            serverReceiver.setCondition(ProtonHelper.condition(AmqpError.INTERNAL_ERROR, "testing-error"));
          }

          // Now close the link server side
          serverReceiver.close();
        });

        // Add a close handler
        serverReceiver.closeHandler(x -> {
          serverReceiver.close();
        });

        serverReceiver.open();
      });
    });

    // === Bridge producer handling ====

    AmqpBridge bridge = AmqpBridge.create(vertx);
    ((AmqpBridgeImpl) bridge).setReplyHandlerSupported(false);

    bridge.start("localhost", server.actualPort(), res -> {
      context.assertTrue(res.succeeded());

      MessageProducer<JsonObject> producer = bridge.createProducer(testName);

      producer.exceptionHandler(ex -> {
        context.assertNotNull(ex, "expected exception");
        context.assertTrue(ex instanceof VertxException, "expected vertx exception");
        if (closeWithError) {
          context.assertNotNull(ex.getCause(), "expected cause");
        } else {
          context.assertNull(ex.getCause(), "expected no cause");
        }
        LOG.trace("Producer exception handler called:", ex);

        asyncExceptionHandlerCalled.complete();

        LOG.trace("Shutting down");
        bridge.close(shutdownRes -> {
          LOG.trace("Shutdown complete");
          context.assertTrue(shutdownRes.succeeded());
          asyncShutdown.complete();
        });
      });

      JsonObject body = new JsonObject();
      body.put("body", sentContent);

      producer.send(body);
    });

    try {
      asyncExceptionHandlerCalled.awaitSuccess();
      asyncShutdown.awaitSuccess();
    } finally {
      server.close();
    }
  }

  @Test(timeout = 20000)
  public void testConsumerClosedRemotelyCallsExceptionHandler(TestContext context) throws Exception {
    doConsumerClosedRemotelyCallsExceptionHandlerTestImpl(context, false);
  }

  @Test(timeout = 20000)
  public void testConsumerClosedRemotelyWithErrorCallsExceptionHandler(TestContext context) throws Exception {
    doConsumerClosedRemotelyCallsExceptionHandlerTestImpl(context, true);
  }

  private void doConsumerClosedRemotelyCallsExceptionHandlerTestImpl(TestContext context,
                                                                     boolean closeWithError) throws Exception {
    stopBroker();

    final String testName = getTestName();
    final String sentContent = "myMessageContent-" + testName;

    final Async asyncShutdown = context.async();
    final Async asyncExceptionHandlerCalled = context.async();

    final AtomicBoolean msgReceived = new AtomicBoolean();

    // === Server handling ====

    MockServer server = new MockServer(vertx, serverConnection -> {
      // Expect a connection
      serverConnection.openHandler(serverSender -> {
        LOG.trace("Server connection open");
        // Add a close handler
        serverConnection.closeHandler(x -> {
          serverConnection.close();
        });

        serverConnection.open();
      });

      // Expect a session to open, when the receiver is created
      serverConnection.sessionOpenHandler(serverSession -> {
        LOG.trace("Server session open");
        serverSession.open();
      });

      // Expect a sender link open for the receiver
      serverConnection.senderOpenHandler(serverSender -> {
        LOG.trace("Server sender open");
        Source remoteSource = (Source) serverSender.getRemoteSource();
        context.assertNotNull(remoteSource, "source should not be null");
        context.assertEquals(testName, remoteSource.getAddress(), "expected given address");
        // Naive test-only handling
        serverSender.setSource(remoteSource.copy());

        serverSender.open();

        // Assume we will get credit, buffer the send immediately
        org.apache.qpid.proton.message.Message protonMsg = Proton.message();
        protonMsg.setBody(new AmqpValue(sentContent));

        serverSender.send(protonMsg);

        // Mark it closed server side
        if (closeWithError) {
          serverSender.setCondition(ProtonHelper.condition(AmqpError.INTERNAL_ERROR, "testing-error"));
        }
        serverSender.close();

      });
    });

    // === Bridge consumer handling ====

    AmqpBridge bridge = AmqpBridge.create(vertx);
    ((AmqpBridgeImpl) bridge).setReplyHandlerSupported(false);
    bridge.start("localhost", server.actualPort(), res -> {
      LOG.trace("Startup complete");

      MessageConsumer<JsonObject> consumer = bridge.createConsumer(testName);
      consumer.exceptionHandler(ex -> {
        context.assertNotNull(ex, "expected exception");
        context.assertTrue(ex instanceof VertxException, "expected vertx exception");
        if (closeWithError) {
          context.assertNotNull(ex.getCause(), "expected cause");
        } else {
          context.assertNull(ex.getCause(), "expected no cause");
        }
        LOG.trace("Producer exception handler called:", ex);

        context.assertTrue(msgReceived.get(), "expected msg to be received first");
        asyncExceptionHandlerCalled.complete();

        LOG.trace("Shutting down");
        bridge.close(shutdownRes -> {
          LOG.trace("Shutdown complete");
          context.assertTrue(shutdownRes.succeeded());
          asyncShutdown.complete();
        });
      });

      consumer.handler(msg -> {
        // Received message, verify it
        JsonObject jsonObject = msg.body();
        context.assertNotNull(jsonObject, "message jsonObject body was null");
        Object amqpBodyContent = jsonObject.getValue(AmqpConstants.BODY);
        context.assertNotNull(amqpBodyContent, "amqp message body content was null");
        context.assertEquals(sentContent, amqpBodyContent, "amqp message body not as expected");

        msgReceived.set(true);
      });
    });

    try {
      asyncExceptionHandlerCalled.awaitSuccess();
      asyncShutdown.awaitSuccess();
    } finally {
      server.close();
    }
  }

  @Test(timeout = 20000)
  public void testConsumerClosedRemotelyCallsEndHandler(TestContext context) throws Exception {
    stopBroker();

    final String testName = getTestName();
    final String sentContent = "myMessageContent-" + testName;

    final Async asyncShutdown = context.async();
    final Async asyncEndHandlerCalled = context.async();

    final AtomicBoolean msgReceived = new AtomicBoolean();

    // === Server handling ====

    MockServer server = new MockServer(vertx, serverConnection -> {
      // Expect a connection
      serverConnection.openHandler(serverSender -> {
        LOG.trace("Server connection open");
        // Add a close handler
        serverConnection.closeHandler(x -> {
          serverConnection.close();
        });

        serverConnection.open();
      });

      // Expect a session to open, when the receiver is created
      serverConnection.sessionOpenHandler(serverSession -> {
        LOG.trace("Server session open");
        serverSession.open();
      });

      // Expect a sender link open for the receiver
      serverConnection.senderOpenHandler(serverSender -> {
        LOG.trace("Server sender open");
        Source remoteSource = (Source) serverSender.getRemoteSource();
        context.assertNotNull(remoteSource, "source should not be null");
        context.assertEquals(testName, remoteSource.getAddress(), "expected given address");
        // Naive test-only handling
        serverSender.setSource(remoteSource.copy());

        serverSender.open();

        // Assume we will get credit, buffer the send immediately
        org.apache.qpid.proton.message.Message protonMsg = Proton.message();
        protonMsg.setBody(new AmqpValue(sentContent));

        serverSender.send(protonMsg);

        // Mark it closed server side
        serverSender.setCondition(ProtonHelper.condition(AmqpError.INTERNAL_ERROR, "testing-error"));

        serverSender.close();
      });
    });

    // === Bridge consumer handling ====

    AmqpBridge bridge = AmqpBridge.create(vertx);
    ((AmqpBridgeImpl) bridge).setReplyHandlerSupported(false);
    bridge.start("localhost", server.actualPort(), res -> {
      LOG.trace("Startup complete");

      MessageConsumer<JsonObject> consumer = bridge.createConsumer(testName);
      consumer.endHandler(x -> {
        context.assertTrue(msgReceived.get(), "expected msg to be received first");
        asyncEndHandlerCalled.complete();

        LOG.trace("Shutting down");
        bridge.close(shutdownRes -> {
          LOG.trace("Shutdown complete");
          context.assertTrue(shutdownRes.succeeded());
          asyncShutdown.complete();
        });
      });

      consumer.handler(msg -> {
        // Received message, verify it
        JsonObject jsonObject = msg.body();
        context.assertNotNull(jsonObject, "message jsonObject body was null");
        Object amqpBodyContent = jsonObject.getValue(AmqpConstants.BODY);
        context.assertNotNull(amqpBodyContent, "amqp message body content was null");
        context.assertEquals(sentContent, amqpBodyContent, "amqp message body not as expected");

        msgReceived.set(true);
      });
    });

    try {
      asyncEndHandlerCalled.awaitSuccess();
      asyncShutdown.awaitSuccess();
    } finally {
      server.close();
    }
  }

  @Test(timeout = 20000)
  public void testConsumerUnregisteredLocallyDoesNotCallEndHandler(TestContext context) throws Exception {
    stopBroker();

    final String testName = getTestName();
    final String sentContent = "myMessageContent-" + testName;

    final Async asyncShutdown = context.async();

    // === Server handling ====

    MockServer server = new MockServer(vertx, serverConnection -> {
      // Expect a connection
      serverConnection.openHandler(serverSender -> {
        LOG.trace("Server connection open");
        // Add a close handler
        serverConnection.closeHandler(x -> {
          serverConnection.close();
        });

        serverConnection.open();
      });

      // Expect a session to open, when the receiver is created
      serverConnection.sessionOpenHandler(serverSession -> {
        LOG.trace("Server session open");
        serverSession.open();
      });

      // Expect a sender link open for the receiver
      serverConnection.senderOpenHandler(serverSender -> {
        LOG.trace("Server sender open");
        Source remoteSource = (Source) serverSender.getRemoteSource();
        context.assertNotNull(remoteSource, "source should not be null");
        context.assertEquals(testName, remoteSource.getAddress(), "expected given address");
        // Naive test-only handling
        serverSender.setSource(remoteSource.copy());

        // Assume we will get credit, buffer the send immediately
        org.apache.qpid.proton.message.Message protonMsg = Proton.message();
        protonMsg.setBody(new AmqpValue(sentContent));

        serverSender.send(protonMsg);

        // Add a close handler
        serverSender.closeHandler(x -> {
          serverSender.close();
        });

        serverSender.open();
      });
    });

    // === Bridge consumer handling ====

    AmqpBridge bridge = AmqpBridge.create(vertx);
    ((AmqpBridgeImpl) bridge).setReplyHandlerSupported(false);
    bridge.start("localhost", server.actualPort(), res -> {
      LOG.trace("Startup complete");

      MessageConsumer<JsonObject> consumer = bridge.createConsumer(testName);
      consumer.endHandler(x -> {
        context.fail("should not call end handler");
      });
      consumer.handler(msg -> {
        // Received message, verify it
        JsonObject jsonObject = msg.body();
        context.assertNotNull(jsonObject, "message jsonObject body was null");
        Object amqpBodyContent = jsonObject.getValue(AmqpConstants.BODY);
        context.assertNotNull(amqpBodyContent, "amqp message body content was null");
        context.assertEquals(sentContent, amqpBodyContent, "amqp message body not as expected");

        consumer.unregister(x -> {
          // Unregister complete, schedule shutdown, give chance for end handler to run, so we can verify it didn't.
          vertx.setTimer(50, y -> {
            LOG.trace("Shutting down");
            bridge.close(shutdownRes -> {
              LOG.trace("Shutdown complete");
              context.assertTrue(shutdownRes.succeeded());
              asyncShutdown.complete();
            });
          });
        });
      });
    });

    try {
      asyncShutdown.awaitSuccess();
    } finally {
      server.close();
    }
  }

  @Test(timeout = 20000)
  public void testInitialCredit(TestContext context) throws Exception {
    doConsumerInitialCreditTestImpl(context, false, 1000);
  }

  @Test(timeout = 20000)
  public void testInitialCreditInfluencedByConsumerBufferSize(TestContext context) throws Exception {
    doConsumerInitialCreditTestImpl(context, true, 42);
  }

  private void doConsumerInitialCreditTestImpl(TestContext context, boolean setMaxBuffered,
                                               int initialCredit) throws Exception {
    stopBroker();

    final String testName = getTestName();
    final String sentContent = "myMessageContent-" + testName;

    final AtomicBoolean firstSendQDrainHandlerCall = new AtomicBoolean();
    final Async asyncInitialCredit = context.async();
    final Async asyncShutdown = context.async();

    // === Server handling ====

    MockServer server = new MockServer(vertx, serverConnection -> {
      // Expect a connection
      serverConnection.openHandler(serverSender -> {
        LOG.trace("Server connection open");
        // Add a close handler
        serverConnection.closeHandler(x -> {
          serverConnection.close();
        });

        serverConnection.open();
      });

      // Expect a session to open, when the receiver is created
      serverConnection.sessionOpenHandler(serverSession -> {
        LOG.trace("Server session open");
        serverSession.open();
      });

      // Expect a sender link open for the receiver
      serverConnection.senderOpenHandler(serverSender -> {
        LOG.trace("Server sender open");
        Source remoteSource = (Source) serverSender.getRemoteSource();
        context.assertNotNull(remoteSource, "source should not be null");
        context.assertEquals(testName, remoteSource.getAddress(), "expected given address");
        // Naive test-only handling
        serverSender.setSource(remoteSource.copy());

        serverSender.sendQueueDrainHandler(s -> {
          // Verify the initial credit when the handler is first called and send a message
          if (firstSendQDrainHandlerCall.compareAndSet(false, true)) {
            context.assertEquals(initialCredit, ((ProtonSenderImpl) s).getCredit(), "unexpected initial credit");
            context.assertFalse(s.sendQueueFull(), "expected send queue not to be full");

            asyncInitialCredit.complete();

            // send message
            org.apache.qpid.proton.message.Message protonMsg = Proton.message();
            protonMsg.setBody(new AmqpValue(sentContent));

            serverSender.send(protonMsg);
          }
        });

        serverSender.open();
      });
    });

    // === Bridge consumer handling ====

    AmqpBridge bridge = AmqpBridge.create(vertx);
    ((AmqpBridgeImpl) bridge).setReplyHandlerSupported(false);
    bridge.start("localhost", server.actualPort(), res -> {
      LOG.trace("Startup complete");

      MessageConsumer<JsonObject> consumer = bridge.createConsumer(testName);
      if (setMaxBuffered) {
        consumer.setMaxBufferedMessages(initialCredit);
      }
      consumer.handler(msg -> {
        // Received message, verify it
        JsonObject jsonObject = msg.body();
        context.assertNotNull(jsonObject, "message jsonObject body was null");
        Object amqpBodyContent = jsonObject.getValue(AmqpConstants.BODY);
        context.assertNotNull(amqpBodyContent, "amqp message body content was null");
        context.assertEquals(sentContent, amqpBodyContent, "amqp message body not as expected");

        LOG.trace("Shutting down");
        bridge.close(shutdownRes -> {
          LOG.trace("Shutdown complete");
          context.assertTrue(shutdownRes.succeeded());
          asyncShutdown.complete();
        });
      });
    });

    try {
      asyncInitialCredit.awaitSuccess();
      asyncShutdown.awaitSuccess();
    } finally {
      server.close();
    }
  }
}
