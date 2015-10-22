package io.vertx.amqpbridge;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.SendContext;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.vertx.proton.ProtonHelper.message;
import static io.vertx.proton.ProtonHelper.tag;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class Bridge implements Handler<SendContext> {

  private final Vertx vertx;
  private ProtonClient client;
  private volatile ProtonConnection connection;
  private Map<String, String> mappings = new ConcurrentHashMap<>();

  public Bridge(Vertx vertx) {
    this.vertx = vertx;
    client = ProtonClient.create(vertx);

    // Set up some routes
    addRoute("amqp.queue1", "queue://queue1");
    addRoute("amqp.queue2", "queue://queue2");
  }

  /*
  Maps a vert.x address to an AMQP destination
   */
  public Bridge addRoute(String vertxAddress, String amqpDestination) {
    // Receive messages from a queue
    connection.receiver().setSource(amqpDestination)
      .handler((receiver, delivery, msg) -> {

        Section body = msg.getBody();
        if (body instanceof AmqpValue) {
          String content = (String) ((AmqpValue) body).getValue();
          System.out.println("Received message from AMQP with content: " + content);

          // Now forward it to the Vert.x destination
          vertx.eventBus().send(vertxAddress, content);
        }

        // We could nack if we need to.
        // delivery.disposition(new Rejected());
        delivery.settle(); // This acks the message
        receiver.flow(1);

      })
      .flow(10)  // Prefetch up to 10 messages
      .open();
    mappings.put(vertxAddress, amqpDestination);
    return this;
  }

  public void start(Handler<AsyncResult<Void>> resultHandler) {
    client.connect("localhost", 5672, res -> {
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
    if (mappings.containsKey(sendContext.message().address())) {
      handleSend(sendContext);
    } else {
      sendContext.next();
    }
  }

  protected void handleSend(SendContext sendContext) {
    // Send to the AMQP destination
    // Send messages to a queue..
    Message message = message(sendContext.message().body().toString());
    message.setAddress(sendContext.message().address());
    connection.send(tag("m1"),message).handler(delivery -> {
      if (delivery.remotelySettled()) {
        System.out.println("The message was sent");
      }
    });
  }
}
