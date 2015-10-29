package io.vertx.amqpbridge;

import io.vertx.core.eventbus.EventBus;
import org.junit.Test;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class BridgeTest extends BridgeTestBase {

  @Test
  public void testSimpleSendConsume() {

    EventBus eb = vertx.eventBus();

    Bridge bridge = new Bridge(vertx, new BridgeOptions());


    bridge.start(res ->{
      if (res.succeeded()) {
        System.out.println("Connection to AMQP peer was succesfull");

        bridge.addOutgoingRoute("usa.nyc", "/Broadcast/usa.nyc");
        bridge.addIncomingRoute("/Broadcast/usa.nyc", "usa.nyc");

        eb.addInterceptor(bridge);

        eb.consumer("usa.nyc", message -> {
          System.out.println("Received Weather : " + message.body());
          testComplete();
        });

        vertx.setPeriodic(1000, v -> eb.publish("usa.nyc", "It's nice and sunny in the big apple!"));

      } else {
        res.cause().printStackTrace();
        fail("Connection to AMQP peer was not succesfull");

      }
    });

    await();

  }
}
