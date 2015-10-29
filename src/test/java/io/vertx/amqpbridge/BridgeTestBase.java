package io.vertx.amqpbridge;

import io.vertx.test.core.VertxTestBase;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class BridgeTestBase extends VertxTestBase {

  protected MockServer server;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    try {
      server = new MockServer(vertx);
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void tearDown() throws Exception {
    server.close();
    super.tearDown();
  }

}
