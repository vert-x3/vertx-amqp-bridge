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

        bridge.addOutgoingRoute("send.usa.nyc", "/Broadcast/usa.nyc");
        bridge.addIncomingRoute("/Broadcast/usa.nyc", "recv.usa.nyc");

        eb.addInterceptor(bridge);

        eb.consumer("recv.usa.nyc", message -> {
          System.out.println("Received Weather : " + message.body());
          testComplete();
        });

        eb.publish("send.usa.nyc", "It's nice and sunny in the big apple!");

      } else {
        res.cause().printStackTrace();
        fail("Connection to AMQP peer was not succesfull");

      }
    });

    await();

  }
}
