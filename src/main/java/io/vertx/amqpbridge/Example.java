package io.vertx.amqpbridge;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class Example {

  public static void main(String[] args) {

    Vertx vertx = Vertx.vertx();
    EventBus eb = vertx.eventBus();
    Bridge bridge = new Bridge(vertx);
    bridge.addRoute("amqp.queue1", "queue://queue1");
    bridge.addRoute("amqp.queue2", "queue://queue2");
    eb.addInterceptor(bridge);

  }
}
