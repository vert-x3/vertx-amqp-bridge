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

import java.util.concurrent.ExecutionException;

import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.Vertx;
import io.vertx.core.http.ClientAuth;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.PfxOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonServerOptions;

@RunWith(VertxUnitRunner.class)
public class BridgeSslTest {

  private static Logger LOG = LoggerFactory.getLogger(BridgeSslTest.class);

  private static final String PASSWORD = "password";
  private static final String KEYSTORE = "src/test/resources/broker-pkcs12.keystore";
  private static final String WRONG_HOST_KEYSTORE = "src/test/resources/broker-wrong-host-pkcs12.keystore";
  private static final String TRUSTSTORE = "src/test/resources/client-pkcs12.truststore";
  private static final String KEYSTORE_CLIENT = "src/test/resources/client-pkcs12.keystore";
  private static final String OTHER_CA_TRUSTSTORE = "src/test/resources/other-ca-pkcs12.truststore";
  private static final String VERIFY_HTTPS = "HTTPS";
  private static final String NO_VERIFY = "";

  private Vertx vertx;
  private MockServer mockServer;

  @Before
  public void setup() {
    vertx = Vertx.vertx();
  }

  @After
  public void tearDown() {
    try {
      if (mockServer != null) {
        mockServer.close();
      }
    } finally {
      vertx.close();
    }
  }

  @Test(timeout = 20000)
  public void testConnectWithSslSucceeds(TestContext context) throws Exception {
    Async async = context.async();

    ProtonServerOptions serverOptions = new ProtonServerOptions();
    serverOptions.setSsl(true);
    PfxOptions serverPfxOptions = new PfxOptions().setPath(KEYSTORE).setPassword(PASSWORD);
    serverOptions.setPfxKeyCertOptions(serverPfxOptions);

    mockServer = new MockServer(vertx, conn -> {
      handleBridgeStartupProcess(conn, context);
    }, serverOptions);

    // Start the bridge and verify is succeeds
    BridgeOptions bridgeOptions = new BridgeOptions();
    bridgeOptions.setSsl(true);
    PfxOptions clientPfxOptions = new PfxOptions().setPath(TRUSTSTORE).setPassword(PASSWORD);
    bridgeOptions.setPfxTrustOptions(clientPfxOptions);

    Bridge bridge = Bridge.bridge(vertx, bridgeOptions);
    bridge.start("localhost", mockServer.actualPort(), res -> {
      // Expect start to succeed
      context.assertTrue(res.succeeded(), "expected start to suceed");
      async.complete();
    });

    async.awaitSuccess();
  }

  @Test(timeout = 20000)
  public void testConnectWithSslToNonSslServerFails(TestContext context) throws Exception {
    Async async = context.async();

    // Create a server that doesn't use ssl
    ProtonServerOptions serverOptions = new ProtonServerOptions();
    serverOptions.setSsl(false);

    mockServer = new MockServer(vertx, conn -> {
      handleBridgeStartupProcess(conn, context);
    }, serverOptions);

    // Try to start the bridge and expect it to fail
    BridgeOptions bridgeOptions = new BridgeOptions();
    bridgeOptions.setSsl(true);
    PfxOptions pfxOptions = new PfxOptions().setPath(TRUSTSTORE).setPassword(PASSWORD);
    bridgeOptions.setPfxTrustOptions(pfxOptions);

    Bridge bridge = Bridge.bridge(vertx, bridgeOptions);
    bridge.start("localhost", mockServer.actualPort(), res -> {
      // Expect start to fail due to remote peer not doing SSL
      context.assertFalse(res.succeeded(), "expected start to fail due to server not using secure transport");
      async.complete();
    });

    async.awaitSuccess();
  }

  @Test(timeout = 20000)
  public void testConnectWithSslToServerWithUntrustedKeyFails(TestContext context) throws Exception {
    Async async = context.async();

    ProtonServerOptions serverOptions = new ProtonServerOptions();
    serverOptions.setSsl(true);
    PfxOptions serverPfxOptions = new PfxOptions().setPath(KEYSTORE).setPassword(PASSWORD);
    serverOptions.setPfxKeyCertOptions(serverPfxOptions);

    mockServer = new MockServer(vertx, conn -> {
      handleBridgeStartupProcess(conn, context);
    }, serverOptions);

    // Try to start the bridge and expect it to fail due to not trusting the server
    BridgeOptions bridgeOptions = new BridgeOptions();
    bridgeOptions.setSsl(true);
    PfxOptions pfxOptions = new PfxOptions().setPath(OTHER_CA_TRUSTSTORE).setPassword(PASSWORD);
    bridgeOptions.setPfxTrustOptions(pfxOptions);

    Bridge bridge = Bridge.bridge(vertx, bridgeOptions);
    bridge.start("localhost", mockServer.actualPort(), res -> {
      // Expect start to fail due to remote peer not being trusted
      context.assertFalse(res.succeeded(), "expected start to fail due to untrusted server");
      async.complete();
    });

    async.awaitSuccess();
  }

  @Test(timeout = 20000)
  public void testConnectWithSslToServerWhileUsingTrustAll(TestContext context) throws Exception {
    Async async = context.async();

    ProtonServerOptions serverOptions = new ProtonServerOptions();
    serverOptions.setSsl(true);
    PfxOptions serverPfxOptions = new PfxOptions().setPath(KEYSTORE).setPassword(PASSWORD);
    serverOptions.setPfxKeyCertOptions(serverPfxOptions);

    mockServer = new MockServer(vertx, conn -> {
      handleBridgeStartupProcess(conn, context);
    }, serverOptions);

    // Try to start the bridge and expect it to succeed due to trusting all certs
    BridgeOptions bridgeOptions = new BridgeOptions();
    bridgeOptions.setSsl(true);
    bridgeOptions.setTrustAll(true);

    Bridge bridge = Bridge.bridge(vertx, bridgeOptions);
    bridge.start("localhost", mockServer.actualPort(), res -> {
      // Expect start to succeed
      context.assertTrue(res.succeeded(), "expected start to suceed due to trusting all certs");
      async.complete();
    });

    async.awaitSuccess();
  }

  @Test(timeout = 20000)
  public void testConnectWithSslWithoutRequiredClientKeyFails(TestContext context) throws Exception {
    doClientCertificateTestImpl(context, false);
  }

  @Test(timeout = 20000)
  public void testConnectWithSslWithRequiredClientKeySucceeds(TestContext context) throws Exception {
    doClientCertificateTestImpl(context, true);
  }

  private void doClientCertificateTestImpl(TestContext context, boolean supplyClientCert) throws InterruptedException,
                                                                                          ExecutionException {
    Async async = context.async();

    ProtonServerOptions serverOptions = new ProtonServerOptions();
    serverOptions.setSsl(true);
    serverOptions.setClientAuth(ClientAuth.REQUIRED);
    PfxOptions serverPfxOptions = new PfxOptions().setPath(KEYSTORE).setPassword(PASSWORD);
    serverOptions.setPfxKeyCertOptions(serverPfxOptions);

    PfxOptions pfxOptions = new PfxOptions().setPath(TRUSTSTORE).setPassword(PASSWORD);
    serverOptions.setPfxTrustOptions(pfxOptions);

    mockServer = new MockServer(vertx, conn -> {
      handleBridgeStartupProcess(conn, context);
    }, serverOptions);

    // Try to start the bridge
    BridgeOptions bridgeOptions = new BridgeOptions();
    bridgeOptions.setSsl(true);
    bridgeOptions.setPfxTrustOptions(pfxOptions);

    if (supplyClientCert) {
      PfxOptions clientKeyPfxOptions = new PfxOptions().setPath(KEYSTORE_CLIENT).setPassword(PASSWORD);
      bridgeOptions.setPfxKeyCertOptions(clientKeyPfxOptions);
    }

    Bridge bridge = Bridge.bridge(vertx, bridgeOptions);
    bridge.start("localhost", mockServer.actualPort(), res -> {
      if (supplyClientCert) {
        // Expect start to succeed
        context.assertTrue(res.succeeded(), "expected start to suceed due to supplying client certs");
      } else {
        // Expect start to fail
        context.assertFalse(res.succeeded(), "expected start to fail due to withholding client cert");
      }
      async.complete();
    });

    async.awaitSuccess();
  }

  @Test(timeout = 20000)
  public void testConnectWithHostnameVerification(TestContext context) throws Exception {
    doHostnameVerificationTestImpl(context, true);
  }

  @Test(timeout = 20000)
  public void testConnectWithoutHostnameVerification(TestContext context) throws Exception {
    doHostnameVerificationTestImpl(context, false);
  }

  private void doHostnameVerificationTestImpl(TestContext context, boolean verifyHost) throws Exception {

    Async async = context.async();

    ProtonServerOptions serverOptions = new ProtonServerOptions();
    serverOptions.setSsl(true);
    PfxOptions serverPfxOptions = new PfxOptions().setPath(WRONG_HOST_KEYSTORE).setPassword(PASSWORD);
    serverOptions.setPfxKeyCertOptions(serverPfxOptions);

    mockServer = new MockServer(vertx, conn -> {
      handleBridgeStartupProcess(conn, context);
    }, serverOptions);

    // Start the bridge
    BridgeOptions bridgeOptions = new BridgeOptions();
    bridgeOptions.setSsl(true);
    PfxOptions clientPfxOptions = new PfxOptions().setPath(TRUSTSTORE).setPassword(PASSWORD);
    bridgeOptions.setPfxTrustOptions(clientPfxOptions);

    // Verify/update the hostname verification settings
    context.assertEquals(VERIFY_HTTPS, bridgeOptions.getHostnameVerificationAlgorithm(),
        "expected host verification to be on by default");
    if (!verifyHost) {
      bridgeOptions.setHostnameVerificationAlgorithm(NO_VERIFY);
    }

    Bridge bridge = Bridge.bridge(vertx, bridgeOptions);
    bridge.start("localhost", mockServer.actualPort(), res -> {
      if (verifyHost) {
        // Expect start to fail
        context.assertFalse(res.succeeded(), "expected start to fail due to server cert not matching hostname");
      } else {
        // Expect start to succeed
        context.assertTrue(res.succeeded(), "expected start to suceed due to not verifying server hostname");
      }
      async.complete();
    });

    async.awaitSuccess();
  }

  private void handleBridgeStartupProcess(ProtonConnection serverConnection, TestContext context) {
    // Expect a connection
    serverConnection.openHandler(serverSender -> {
      LOG.trace("Server connection open");
      serverConnection.open();
    });

    // Expect a session to open, when the sender/receiver is created by the bridge startup
    serverConnection.sessionOpenHandler(serverSession -> {
      LOG.trace("Server session open");
      serverSession.open();
    });

    // Expect a receiver link open for the anonymous-relay reply sender
    serverConnection.receiverOpenHandler(serverReceiver -> {
      LOG.trace("Server receiver open");
      Target remoteTarget = (Target) serverReceiver.getRemoteTarget();
      context.assertNotNull(remoteTarget, "target should not be null");
      context.assertFalse(remoteTarget.getDynamic(), "target should not be dynamic");
      context.assertNull(remoteTarget.getAddress(), "expected null address");

      serverReceiver.setTarget(remoteTarget);

      serverReceiver.open();
    });

    // Expect a sender link open for a dynamic address
    serverConnection.senderOpenHandler(serverSender -> {
      LOG.trace("Server sender open");
      Source remoteSource = (Source) serverSender.getRemoteSource();
      context.assertNotNull(remoteSource, "source should not be null");
      context.assertTrue(remoteSource.getDynamic(), "source should be dynamic");
      context.assertNull(remoteSource.getAddress(), "expected null address");

      Source source = (Source) remoteSource.copy();
      source.setAddress("should-be-random-generated-address");
      serverSender.setSource(source);

      serverSender.open();
    });
  }
}
