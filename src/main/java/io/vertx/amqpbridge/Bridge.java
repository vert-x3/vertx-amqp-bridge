package io.vertx.amqpbridge;

import io.vertx.amqpbridge.impl.LogManager;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.SendContext;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonReceiver;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.message.Message;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static io.vertx.proton.ProtonHelper.tag;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 * @author <a href="mailto:rajith@rajith.lk">Rajith Muditha Attapattu</a>
 */
public class Bridge implements Handler<SendContext> {

  static final LogManager LOG = LogManager.get("Bridge:", Bridge.class);
  private final Vertx vertx;
  private ProtonClient client;
  private BridgeOptions config;
  private MessageTranslator msgTranslator;
  private Router router;
  private volatile ProtonConnection connection;
  private AtomicInteger counter = new AtomicInteger();

  public Bridge(Vertx vertx, BridgeOptions options) {
    this.vertx = vertx;
    client = ProtonClient.create(vertx);
    config = options;
    msgTranslator = MessageTranslator.get();
    router = Router.get();
  }

  /*
   * Maps a Vert.x address to an AMQP destination
   */
  public Bridge addOutgoingRoute(String vertxAddress, String amqpAddress) {
    router.addOutgoingRoute(vertxAddress, amqpAddress);
    return this;
  }

  /*
   * Maps an AMQP subscription (Ex. Queue, Topic ..etc) to a Vert.x address.
   */
  public Bridge addIncomingRoute(String amqpAddress, String vertxAddress) {
    // Receive messages from an AMQP endpoint
    // TODO keep a map so we can cancel and manage credit.
    final ProtonReceiver receiver = connection.receiver().setSource(amqpAddress).handler((delivery, msg) -> {

      LOG.debug("Received message from AMQP with content: " + msg.getBody());
      // Now forward it to the Vert.x destination
      // TODO doing a publish now. Need to diff btw pub and send.
      // TODO handle reply-to
      vertx.eventBus().publish(router.routeIncoming(msg), msgTranslator.toVertx(msg));
      // TODO for now we just ack everything
      delivery.disposition(Accepted.getInstance());
      // TODO credit-handling receiver.flow(1);

    }).flow(config.getDefaultPrefetch()) // TODO handle flow-control
      .open();
    router.addIncomingRoute(amqpAddress, vertxAddress);
    return this;
  }

  // TODO handle removing routes/unsubscribing.

  public void start(Handler<AsyncResult<Void>> resultHandler) {
    final CountDownLatch connectionReady = new CountDownLatch(1);
    client.connect(config.getOutboundAMQPHost(), config.getOutboundAMQPPort(), res -> {
      if (res.succeeded()) {
        connection = res.result();
        connection.open();
        resultHandler.handle(Future.succeededFuture());
        connectionReady.countDown();
      } else {
        resultHandler.handle(Future.failedFuture(res.cause()));
      }
    });
    // Block start until the connection is ready. Any other operation
    // doesn't make sense until then.
    try {
      connectionReady.await();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
    }
  }

  @Override
  public void handle(SendContext sendContext) {
    String amqpAddress = router.routeOutgoing(sendContext.message());
    if (amqpAddress != null) {
      handleSend(amqpAddress, sendContext);
    } else {
      sendContext.next();
    }
  }

  protected void handleSend(String amqpAddress, SendContext sendContext) {
    Message message = msgTranslator.toAMQP(sendContext.message());
    message.setAddress(amqpAddress);
    connection.send(tag(String.valueOf(counter)), message, delivery -> {
      // handle acks/flow
    });
  }
}