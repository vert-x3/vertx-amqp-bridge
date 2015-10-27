package io.vertx.amqpbridge;

import static io.vertx.proton.ProtonHelper.tag;
import io.vertx.amqpbridge.impl.LogManager;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.SendContext;
import io.vertx.core.json.JsonObject;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonServer;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.message.Message;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 * @author <a href="mailto:rajith@rajith.lk">Rajith Muditha Attapattu</a>
 * 
 */
public class Bridge implements Handler<SendContext> {

	static final LogManager LOG = LogManager.get("Bridge:", Bridge.class);
	private final Vertx vertx;
	private ProtonClient client;
	private ProtonServer server;
	private BridgeOptions config;
	private MessageTranslator msgTranslator;
	private Router router;
	private volatile ProtonConnection connection;
	private AtomicInteger counter = new AtomicInteger();

	public Bridge(Vertx vertx, BridgeOptions options) {
		this.vertx = vertx;
		client = ProtonClient.create(vertx);
		server = ProtonServer.create(vertx);
		config = options;
		msgTranslator = MessageTranslator.get();
		router = Router.get(config.getDefaultIncomingAddress(), config.getDefaultOutgoingAddress());
	}

	/*
	 * Maps a Vert.x address pattern to an AMQP destination
	 */
	public Bridge addOutgoingRoute(String pattern, String amqpAddress) {

		router.addOutgoingRoute(pattern, amqpAddress);
		return this;
	}

	/*
	 * Maps an AMQP subscription (Ex. Queue, Topic ..etc) to a Vert.x address
	 * pattern
	 */
	public Bridge addIncomingRoute(String pattern, String amqpAddress) {
		// Receive messages from an AMQP endpoint
		// TODO keep a map so we can cancel and manage credit.
		final ProtonReceiver receiver = connection.receiver().setSource(amqpAddress).handler((delivery, msg) -> {

			LOG.debug("Received message from AMQP with content: " + msg.getBody());
			// Now forward it to the Vert.x destination
			    List<String> addrList = router.routeIncoming(msg);
			    JsonObject vertxMsg = msgTranslator.toVertx(msg);
			    // TODO doing a publish now. Need to diff btw pub and send.
			    for (String addr : addrList) {
				    vertx.eventBus().send(addr, vertxMsg);
			    }
			    // TODO for now we just ack everything
			    delivery.disposition(Accepted.getInstance());
			    // TODO credit-handling receiver.flow(1);

		    }).flow(config.getDefaultPrefetch()) // TODO handle flow-control
		        .open();
		router.addIncomingRoute(pattern, amqpAddress);
		return this;
	}

	// TODO handle removing routes/unsubscribing.

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
		String amqpAddress = router.routeOutgoing(sendContext.message());
		if (amqpAddress != null) {
			handleSend(amqpAddress, sendContext);
		} else {
			sendContext.next();
		}
	}

	protected void handleSend(String amqpAddress, SendContext sendContext) {
		// Send to the AMQP destination
		// Send messages to a queue..
		Message message = msgTranslator.toAMQP(sendContext.message());
		message.setAddress(amqpAddress);
		connection.send(tag(String.valueOf(counter)), message, delivery -> {
			System.out.println("The message was sent : Remote state is " + delivery.getRemoteState());
		});
	}
}
