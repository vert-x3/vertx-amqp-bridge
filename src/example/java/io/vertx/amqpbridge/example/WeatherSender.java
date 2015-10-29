package io.vertx.amqpbridge.example;

import io.vertx.amqpbridge.Bridge;
import io.vertx.amqpbridge.BridgeOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;

/**
 * @author <a href="mailto:rajith@rajith.lk">Rajith Muditha Attapattu</a>
 */
public class WeatherSender extends AbstractVerticle {

	@Override
	public void start() throws Exception {
		EventBus eb = vertx.eventBus();
		vertx.setPeriodic(1000, v -> eb.publish("usa.nyc", "It's nice and sunny in the big apple!"));
	}

	public static void main(String[] args) {

		Vertx vertx = Vertx.vertx();
		EventBus eb = vertx.eventBus();
		Bridge bridge = new Bridge(vertx, new BridgeOptions());
		bridge.start(res ->{
			if (res.succeeded()) {
				System.out.println("Connection to AMQP peer was succesfull");
			}
			else{
				System.out.println("Connection to AMQP peer was not succesfull");
				throw new Error("Connection to AMQP peer was not succesfull. Aborting!");
			}			
		});
		bridge.addOutgoingRoute("usa.nyc", "/Broadcast/usa.nyc");
		eb.addInterceptor(bridge);
		vertx.deployVerticle(new WeatherSender());
	}
}