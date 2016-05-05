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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class BridgeSaslTest extends ActiveMQTestBase {

  private static Logger LOG = LoggerFactory.getLogger(BridgeSaslTest.class);

  private Vertx vertx;

  private boolean anonymousAccessAllowed = false;

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

  @Override
  protected boolean isAnonymousAccessAllowed() {
    return anonymousAccessAllowed;
  }

  @Test(timeout = 20000)
  public void testConnectWithValidUserPassSucceeds(TestContext context) throws Exception {
    doConnectWithGivenCredentialsTestImpl(context, USERNAME_GUEST, PASSWORD_GUEST, true);
  }

  @Test(timeout = 20000)
  public void testConnectWithInvalidUserPassFails(TestContext context) throws Exception {
    doConnectWithGivenCredentialsTestImpl(context, USERNAME_GUEST, "wrongpassword", false);
  }

  @Test(timeout = 20000)
  public void testConnectAnonymousWithoutUserPass(TestContext context) throws Exception {
    doConnectWithGivenCredentialsTestImpl(context, null, null, false);
    anonymousAccessAllowed = true;
    restartBroker();
    doConnectWithGivenCredentialsTestImpl(context, null, null, true);
  }

  @Test(timeout = 20000)
  public void testRestrictSaslMechanisms(TestContext context) throws Exception {
    BridgeOptions options = new BridgeOptions();

    // Try with the wrong password, with anonymous access disabled, expect start to fail
    doStartWithGivenCredentialsTestImpl(context, options, USERNAME_GUEST, "wrongpassword", false);

    // Try with the wrong password, with anonymous access enabled, expect start still to fail
    anonymousAccessAllowed = true;
    restartBroker();
    doStartWithGivenCredentialsTestImpl(context, options, USERNAME_GUEST, "wrongpassword", false);

    // Now restrict the allows SASL mechanisms to ANONYMOUS, then expect start to succeed as it wont use the invalid
    // credentials
    options.addEnabledSaslMechanism("ANONYMOUS");
    doStartWithGivenCredentialsTestImpl(context, options, USERNAME_GUEST, "wrongpassword", true);
  }

  private void doConnectWithGivenCredentialsTestImpl(TestContext context, String username, String password,
                                                     boolean expectConnectToSucceed) {
    doStartWithGivenCredentialsTestImpl(context, new BridgeOptions(), username, password, expectConnectToSucceed);
  }

  private void doStartWithGivenCredentialsTestImpl(TestContext context, BridgeOptions options, String username,
                                                   String password, boolean expectStartToSucceed) {
    Async async = context.async();

    // Start the bridge and verify whether it succeeds
    Bridge bridge = Bridge.bridge(vertx, options);
    bridge.start("localhost", getBrokerAmqpConnectorPort(), username, password, res -> {
      if (expectStartToSucceed) {
        // Expect connect to succeed
        context.assertTrue(res.succeeded());
      } else {
        // Expect connect to fail
        context.assertFalse(res.succeeded());
      }

      LOG.trace("Bridge start complete");
      async.complete();
    });

    async.awaitSuccess();
  }
}
