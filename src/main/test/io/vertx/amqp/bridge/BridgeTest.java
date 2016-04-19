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

import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.qpid.proton.amqp.Symbol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.amqp.bridge.Bridge;
import io.vertx.amqp.bridge.impl.BridgeMetaDataSupportImpl;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

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

    Bridge bridge = Bridge.bridge(vertx, getBrokerAmqpConnectorPort());
    bridge.start(res -> {
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
            "product property key not present");
        context.assertEquals(BridgeMetaDataSupportImpl.VERSION, properties.get(BridgeMetaDataSupportImpl.VERSION_KEY),
            "unexpected product property value");

        asyncMetaData.complete();
      });

    });

    Bridge bridge = Bridge.bridge(vertx, server.actualPort());
    bridge.start(res -> {
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
}