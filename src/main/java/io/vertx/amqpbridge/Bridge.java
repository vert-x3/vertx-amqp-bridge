package io.vertx.amqpbridge;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.SendContext;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonSession;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class Bridge implements Handler<SendContext> {

  private String vertxAddressPrefix;
  private ProtonClient client;
  private volatile ProtonConnection connection;
  private ProtonSession session;

  public Bridge(Vertx vertx, String vertxAddressPrefix, String amqpAddressPrefix) {
    this.vertxAddressPrefix = vertxAddressPrefix;

    //TODO
    // Setup a consumer on the ampqp addresses and when a message arrives there forward to Vert.x event bus

    client = ProtonClient.create(vertx);
  }

  public void start(Handler<AsyncResult<Void>> resultHandler) {
    client.connect("localhost", 5672, res -> {
      if (res.succeeded()) {
        connection = res.result();
        resultHandler.handle(Future.succeededFuture());
      } else {
        resultHandler.handle(Future.failedFuture(res.cause()));
      }
    });
  }

  @Override
  public void handle(SendContext sendContext) {
    if (sendContext.message().address().startsWith(vertxAddressPrefix)) {
      handleSend(sendContext);
    } else {
      sendContext.next();
    }
  }

  protected void handleSend(SendContext sendContext) {
    // Send to the AMQP destination
  }
}
