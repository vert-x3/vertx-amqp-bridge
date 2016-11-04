package io.vertx;

import io.vertx.amqpbridge.AmqpBridge;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;

import java.io.File;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class Main {

  public static final int PORT = 2000;
  public static final String ADDRESS = "myAmqpAddress";

  public static void main(String[] args) throws Exception {

    File tmp = File.createTempFile("activemq", ".tmp");
    tmp.delete();
    tmp.mkdirs();
    tmp.deleteOnExit();

    BrokerService brokerService = new BrokerService();
    brokerService.setBrokerName("localhost");
    brokerService.setDeleteAllMessagesOnStartup(true);
    brokerService.setUseJmx(true);
    brokerService.getManagementContext().setCreateConnector(false);
    brokerService.setDataDirectory(tmp.getAbsolutePath());
    brokerService.setPersistent(false);
    brokerService.setSchedulerSupport(false);
    brokerService.setAdvisorySupport(false);

    TransportConnector connector = brokerService
        .addConnector("amqp://0.0.0.0:" + PORT + "?transport.transformer=jms"
            + "&transport.socketBufferSize=65536&ioBufferSize=8192");
    connector.setName("amqp");

    brokerService.start();
    brokerService.waitUntilStarted();


    Vertx vertx = Vertx.vertx();

    AmqpBridge bridge = AmqpBridge.create(vertx);
    bridge.start("localhost", PORT, res -> {
      if (res.succeeded()) {
        MessageConsumer<JsonObject> consumer = bridge.createConsumer(ADDRESS);
        consumer.handler(vertxMsg -> {
          System.out.println("Received message: " + vertxMsg.body().getValue("body"));
        });
      }
    });

    AmqpBridge bridge2 = AmqpBridge.create(vertx);
    bridge2.start("localhost", PORT, res -> {
      if (res.succeeded()) {
        MessageProducer<JsonObject> producer = bridge2.createProducer(ADDRESS);
        vertx.setPeriodic(1000, id -> {
          producer.send(new JsonObject().put("body", "the-content"));
          System.out.println("Sent message");
        });
      }
    });

    System.in.read();
  }

}
