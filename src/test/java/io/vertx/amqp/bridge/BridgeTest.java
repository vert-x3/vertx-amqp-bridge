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

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.amqp.bridge.Bridge;
import io.vertx.amqp.bridge.impl.BridgeImpl;
import io.vertx.amqp.bridge.impl.BridgeMetaDataSupportImpl;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;

@RunWith(VertxUnitRunner.class)
public class BridgeTest extends ActiveMQTestBase {

  private static Logger LOG = LoggerFactory.getLogger(BridgeTest.class);

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

    Bridge bridge = Bridge.bridge(vertx);
    bridge.start("localhost", getBrokerAmqpConnectorPort(), res -> {
      LOG.trace("Startup complete");
      context.assertTrue(res.succeeded());

      context.assertEquals(1L, getBrokerAdminView(context).getTotalConnectionsCount(),
          "unexpected total connection count during");
      context.assertEquals(1, getBrokerAdminView(context).getCurrentConnectionsCount(),
          "unexpected current connection count during");

      bridge.shutdown(shutdownRes -> {
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

    Bridge bridge = Bridge.bridge(vertx);
    ((BridgeImpl) bridge).setDisableReplyHandlerSupport(true);
    bridge.start("localhost", server.actualPort(), res -> {
      LOG.trace("Startup complete");
      asyncMetaData.awaitSuccess();

      LOG.trace("Shutting down");
      bridge.shutdown(shutdownRes -> {
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

    Bridge bridge = Bridge.bridge(vertx);
    bridge.start("localhost", port, res -> {
      LOG.trace("Startup complete");

      // Set up a consumer using the bridge
      bridge.createConsumer(testName).handler(msg -> {
        JsonObject jsonObject = msg.body();
        context.assertNotNull(jsonObject, "message jsonObject body was null");

        Object amqpBodyContent = jsonObject.getValue(MessageHelper.BODY);
        context.assertNotNull(amqpBodyContent, "amqp message body content was null");

        context.assertEquals(sentContent, amqpBodyContent, "amqp message body was not as expected");

        LOG.trace("Shutting down");
        bridge.shutdown(shutdownRes -> {
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

    Bridge bridge = Bridge.bridge(vertx);
    bridge.start("localhost", port, res -> {
      // Set up a sender using the bridge
      context.assertTrue(res.succeeded());

      MessageProducer<JsonObject> producer = bridge.createProducer(testName);

      JsonObject body = new JsonObject();
      body.put("body", sentContent);

      producer.send(body);
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

    Bridge bridge = Bridge.bridge(vertx);
    bridge.start("localhost", getBrokerAmqpConnectorPort(), startResult -> {
      context.assertTrue(startResult.succeeded());

      MessageProducer<JsonObject> producer = bridge.createProducer(destinationName);

      JsonObject body = new JsonObject();
      body.put(MessageHelper.BODY, content);

      producer.<JsonObject> send(body, reply -> {
        LOG.trace("Client got reply");
        context.assertEquals(replyContent, reply.result().body().getValue(MessageHelper.BODY),
            "unexpected reply msg content");

        LOG.trace("Shutting down");
        bridge.shutdown(shutdownRes -> {
          LOG.trace("Shutdown complete");
          context.assertTrue(shutdownRes.succeeded());
          asyncShutdown.complete();
        });
      });
      LOG.trace("Client sent msg");

      MessageConsumer<JsonObject> consumer = bridge.createConsumer(destinationName);
      consumer.handler(msg -> {
        JsonObject receivedMsgBody = msg.body();
        LOG.trace("Client got msg: " + receivedMsgBody);

        context.assertNotNull(receivedMsgBody, "expected msg body but none found");
        context.assertEquals(content, receivedMsgBody.getValue(MessageHelper.BODY), "unexpected msg content");

        JsonObject replyBody = new JsonObject();
        replyBody.put(MessageHelper.BODY, replyContent);

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

    Bridge bridge = Bridge.bridge(vertx);
    bridge.start("localhost", getBrokerAmqpConnectorPort(), startResult -> {
      context.assertTrue(startResult.succeeded());

      MessageProducer<JsonObject> producer = bridge.createProducer(destinationName);

      JsonObject body = new JsonObject();
      body.put(MessageHelper.BODY, content);

      producer.<JsonObject> send(body, reply -> {
        LOG.trace("Client got reply");
        Message<JsonObject> replyMessage = reply.result();
        context.assertEquals(replyContent, replyMessage.body().getValue(MessageHelper.BODY),
            "unexpected reply msg content");

        replyRecievedAsync.complete();

        JsonObject replyToReplyBody = new JsonObject();
        replyToReplyBody.put(MessageHelper.BODY, replyToReplyContent);

        replyMessage.reply(replyToReplyBody);
      });
      LOG.trace("Client sent msg");

      MessageConsumer<JsonObject> consumer = bridge.createConsumer(destinationName);
      consumer.handler(msg -> {
        JsonObject receivedMsgBody = msg.body();
        LOG.trace("Client got msg: " + receivedMsgBody);

        context.assertNotNull(receivedMsgBody, "expected msg body but none found");
        context.assertEquals(content, receivedMsgBody.getValue(MessageHelper.BODY), "unexpected msg content");

        JsonObject replyBody = new JsonObject();
        replyBody.put(MessageHelper.BODY, replyContent);

        msg.<JsonObject> reply(replyBody, replyToReply -> {
          LOG.trace("Client got reply to reply");
          Message<JsonObject> replyToReplyMessage = replyToReply.result();
          context.assertEquals(replyToReplyContent, replyToReplyMessage.body().getValue(MessageHelper.BODY),
              "unexpected reply msg content");

          LOG.trace("Shutting down");
          bridge.shutdown(shutdownRes -> {
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

    Bridge bridge = Bridge.bridge(vertx);
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

          Object amqpBodyContent = jsonObject.getValue(MessageHelper.BODY);
          context.assertNotNull(amqpBodyContent, "amqp message " + msgNum + " body content was null");

          context.assertEquals(sentContent, amqpBodyContent, "amqp message " + msgNum + " body not as expected");

          if (msgNum == msgCount) {
            LOG.trace("Shutting down");
            bridge.shutdown(shutdownRes -> {
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

    Bridge bridge = Bridge.bridge(vertx);
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

        Object amqpBodyContent = jsonObject.getValue(MessageHelper.BODY);
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
          bridge.shutdown(shutdownRes -> {
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
}
