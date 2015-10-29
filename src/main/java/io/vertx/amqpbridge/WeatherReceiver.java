package io.vertx.amqpbridge;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;

/**
 * @author <a href="mailto:rajith@rajith.lk">Rajith Muditha Attapattu</a>
 */
public class WeatherReceiver extends AbstractVerticle {
	@Override
	public void start() throws Exception {
		EventBus eb = vertx.eventBus();
		eb.consumer("usa.nyc", message -> System.out.println("Received Weather : " + message.body()));
		System.out.println("Ready to receive the weather for usa.nyc");
	}

	public static void main(String[] args) {
		// Default subscribe to all.
		Vertx vertx = Vertx.vertx();
		EventBus eb = vertx.eventBus();
		Bridge bridge = new Bridge(vertx, new BridgeOptions());
		bridge.start(res -> {
			if (res.succeeded()) {
				System.out.println("Connection to AMQP peer was succesfull");
			} else {
				System.out.println("Connection to AMQP peer was not succesfull");
				res.cause().printStackTrace();
				throw new Error("Connection to AMQP peer was not succesfull. Aborting!");
			}
		});
		bridge.addIncomingRoute("/Broadcast/usa.nyc", "usa.nyc");
		eb.addInterceptor(bridge);
		vertx.deployVerticle(new WeatherReceiver());
	}
}