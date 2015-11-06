package io.vertx.amqpbridge;

import io.vertx.core.eventbus.EventBus;
import org.junit.Test;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class BridgeTest extends BridgeTestBase {

	protected EventBus eb;

	protected void setUpAndRun(Runnable runner) {
		eb = vertx.eventBus();
		Bridge bridge = new Bridge(vertx, new BridgeOptions());
		bridge.start(res -> {
			if (res.succeeded()) {
				bridge.addOutgoingRoute("send.my-queue", "queue/my-queue");
				bridge.addIncomingRoute("queue/my-queue", "recv.my-queue");
				eb.addInterceptor(bridge);
				runner.run();
			} else {
				fail("Connection to AMQP peer was not succesfull");
			}
		});

	}

	@Test
	public void testSimpleSendConsume() {
		setUpAndRun(() -> {
			eb.consumer("recv.my-queue", message -> {
				System.out.println("Received Weather : " + message.body());
				testComplete();
			});

			eb.send("send.my-queue", "It's nice and sunny in the big apple!");
		});

		await();
	}

	@Test
	public void testSendWithAck() {
		setUpAndRun(() -> {
			eb.consumer("recv.my-queue", message -> {
				System.out.println("Received Weather : " + message.body());
			});

			eb.<Boolean>send("send.my-queue", "It's nice and sunny in the big apple!", res -> {
				assertTrue(res.succeeded());
				assertTrue(res.result().body());
				testComplete();
			});
		});

		await();
	}

	@Test
	public void testConsumeWithAck() {
		setUpAndRun(() -> {
			eb.consumer("recv.my-queue", message -> {
				System.out.println("Received Weather : " + message.body());
				// Now ack it
				message.reply(true);
				// TODO we need to verify it actually gets acked in AMQP, e.g. by trying to consume it again
				testComplete();
			});

			eb.<Boolean>send("send.my-queue", "It's nice and sunny in the big apple!");
		});

		await();
	}
}
