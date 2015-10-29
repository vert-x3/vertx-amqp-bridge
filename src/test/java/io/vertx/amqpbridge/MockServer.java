/**
 * Copyright 2015 Red Hat, Inc.
 */
package io.vertx.amqpbridge;

import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonDelivery;
import io.vertx.proton.ProtonSender;
import io.vertx.proton.ProtonServer;
import org.apache.qpid.proton.message.Message;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class MockServer {

	private ProtonServer server;
	private final Map<String, List<StoreEntry>> store = new HashMap<>();
	private final Map<String, ProtonSender> senders = new HashMap<>();
	private final AtomicInteger counter = new AtomicInteger();

	public MockServer(Vertx vertx) throws ExecutionException, InterruptedException {
		server = ProtonServer.create(vertx);
		server.connectHandler(this::processConnection);
		FutureHandler<ProtonServer, AsyncResult<ProtonServer>> handler = FutureHandler.asyncResult();
		server.listen(5672, handler);
		handler.get();
	}

	private void processConnection(ProtonConnection connection) {
		connection.sessionOpenHandler(session -> session.open());
		connection.receiverOpenHandler(receiver -> {
			receiver.handler((delivery, msg) -> {
				String address = msg.getAddress();
				if (address == null) {
					address = receiver.getRemoteTarget().getAddress();
				}
				if (senders.containsKey(address)) {
					senders.get(address).send((String.valueOf(counter.incrementAndGet())).getBytes(), msg);
				} else {
					storeMessage(address, delivery, msg);
				}

			}).flow(100000).open();
		});
		connection.senderOpenHandler(sender -> {
			sender.setSource(sender.getRemoteSource()).open();
			String address = sender.getSource().getAddress();
			senders.put(address, sender);
			if (store.containsKey(address)) {
				for (Iterator<StoreEntry> it = store.get(address).iterator(); it.hasNext();) {
					sender.send(String.valueOf(counter.incrementAndGet()).getBytes(), it.next().msg);
					it.remove();
				}
			}
		});
		connection.openHandler(result -> {
			connection.setContainer("pong: " + connection.getRemoteContainer()).open();
		});

	}

	public synchronized void storeMessage(String address, ProtonDelivery delivery, Message msg) {
		if (store.containsKey(address)) {
			store.get(address).add(new StoreEntry(delivery, msg));
		} else {
			store.put(address, new ArrayList<>()).add(new StoreEntry(delivery, msg));
		}
	}

	public void close() {
		server.close();
	}

	public int actualPort() {
		return server.actualPort();
	}

	private static class StoreEntry {

		StoreEntry(ProtonDelivery del, Message m) {
			delivery = del;
			msg = m;
		}

		ProtonDelivery delivery;
		Message msg;
	}
}
