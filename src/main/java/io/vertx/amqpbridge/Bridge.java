package io.vertx.amqpbridge;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.eventbus.SendContext;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonReceiver;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.message.Message;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.vertx.proton.ProtonHelper.tag;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 * @author <a href="mailto:rajith@rajith.lk">Rajith Muditha Attapattu</a>
 */
public class Bridge implements Handler<SendContext> {

  private static final Logger log = LoggerFactory.getLogger(Bridge.class);

  private final Vertx vertx;
  private final EventBus eb;
  private ProtonClient client;
  private BridgeOptions config;
  private MessageTranslator msgTranslator;
  private volatile ProtonConnection connection;
  private AtomicInteger counter = new AtomicInteger();

  private final Map<String, String> outboundRoutes = new ConcurrentHashMap<>();
  private final Map<String, ReceiverHolder> inboundRoutes = new ConcurrentHashMap<>();

  private static final class ReceiverHolder {
    final ProtonReceiver receiver;
    final String vertxAddress;

    public ReceiverHolder(ProtonReceiver receiver, String vertxAddress) {
      this.receiver = receiver;
      this.vertxAddress = vertxAddress;
    }
  }


  public Bridge(Vertx vertx, BridgeOptions options) {
    this.vertx = vertx;
    this.eb = vertx.eventBus();
    client = ProtonClient.create(vertx);
    config = options;
    msgTranslator = MessageTranslator.get();
  }

  /*
   * Maps a Vert.x address to an AMQP destination
   */
  public Bridge addOutgoingRoute(String vertxAddress, String amqpAddress) {
    outboundRoutes.put(vertxAddress, amqpAddress);
    return this;
  }

  /*
   * Maps an AMQP subscription (Ex. Queue, Topic ..etc) to a Vert.x address.
   */
  public Bridge addIncomingRoute(String amqpAddress, String vertxAddress) {

    MessageProducer<Object> producer = eb.sender(vertxAddress);

    // Receive messages from an AMQP endpoint
    ProtonReceiver receiver = connection.receiver();
    receiver.setSource(amqpAddress).handler((delivery, msg) -> {

      log.debug("Received message from AMQP with content: " + msg.getBody());
      // Now forward it to the Vert.x destination

      producer.<Boolean>send(msgTranslator.toVertx(msg), res -> {
        if (res.succeeded()) {
          boolean acked = res.result().body();
          if (acked) {
            // Ack to AMQP
            delivery.disposition(Accepted.getInstance());
          }
        } else {
          log.error("Failed to forward message to Vert.x", res.cause());
        }
      });

      if (producer.writeQueueFull()) {
        producer.drainHandler(v -> {
          receiver.flow(1);
        });
      } else {
        receiver.flow(1);
      }

    });
    // Send some initial credits so we receive messages
    receiver.flow(config.getDefaultPrefetch()).open();
    inboundRoutes.put(amqpAddress, new ReceiverHolder(receiver, vertxAddress));
    return this;

  }

  public Bridge removeIncomingRoute(String amqpAddress) {
    ReceiverHolder holder = inboundRoutes.remove(amqpAddress);
    if (holder != null) {
      holder.receiver.close();
    }
    return this;
  }

  public Bridge removeOutgoingRoute(String vertxAddress) {
    outboundRoutes.remove(vertxAddress);
    return this;
  }

  public void start(Handler<AsyncResult<Void>> resultHandler) {
    client.connect(config.getOutboundAMQPHost(), config.getOutboundAMQPPort(), res -> {
      if (res.succeeded()) {
        connection = res.result();
        connection.open();
        resultHandler.handle(Future.succeededFuture());
      } else {
        resultHandler.handle(Future.failedFuture(res.cause()));
      }
    });
  }

  @Override
  public void handle(SendContext sendContext) {
    String amqpAddress = outboundRoutes.get(sendContext.message().address());
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
      sendContext.message().reply(true);
    });
  }
}