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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.message.Message;

/**
 * @author <a href="mailto:rajith@rajith.lk">Rajith Muditha Attapattu</a>
 */
public class MockServer {

	private ProtonServer server;
	private Map<String, List<StoreEntry>> store = new HashMap<String, List<StoreEntry>>();
	private Map<String, Queue> queueSubscribers = new HashMap<String, Queue>();
	private Map<String, List<ProtonSender>> topicSubscribers = new HashMap<String, List<ProtonSender>>();

	private AtomicInteger counter = new AtomicInteger();

	public MockServer(Vertx vertx) throws ExecutionException, InterruptedException {
		server = ProtonServer.create(vertx);
		server.connectHandler((connection) -> processConnection(vertx, connection));
		FutureHandler<ProtonServer, AsyncResult<ProtonServer>> handler = FutureHandler.asyncResult();
		server.listen(5672, handler);
		handler.get();
	}

	private void processConnection(Vertx vertx, ProtonConnection connection) {
		connection.sessionOpenHandler(session -> session.open());
		connection.receiverOpenHandler(receiver -> {
			receiver.handler((delivery, msg) -> {
				String address = msg.getAddress();
				if (address == null) {
					address = receiver.getRemoteTarget().getAddress();
				}
				if (address.startsWith("queue")) {
					if (queueSubscribers.containsKey(address)) {
						queueSubscribers.get(address).sendToNextConsumer(msg);
					} else {
						storeMessage(address, delivery, msg);
					}
				} else if (address.startsWith("topic")) {
					if (topicSubscribers.containsKey(address)) {
						for (ProtonSender sender : topicSubscribers.get(address)) {
							sender.send(tag(), msg);
						}
					}
				} else {
					ErrorCondition error = new ErrorCondition();
					error.setDescription("address needs to be prefixed with 'queue' or 'topic'");
					receiver.setCondition(error);
					receiver.close();
				}

			}).flow(100000).open();
		});
		connection.senderOpenHandler(sender -> {
			sender.setSource(sender.getRemoteSource()).open();
			String address = sender.getSource().getAddress();
			if (address.startsWith("queue")) {
				if (queueSubscribers.containsKey(address)) {
					queueSubscribers.get(address).consumers.add(sender);
				} else {
					Queue queue = new Queue();
					queue.consumers.add(sender);
					queueSubscribers.put(address, queue);
					if (store.containsKey(address)) {
						for (Iterator<StoreEntry> it = store.get(address).iterator(); it.hasNext();) {
							sender.send(String.valueOf(counter.incrementAndGet()).getBytes(), it.next().msg);
							it.remove();
						}
					}
				}
			} else if (address.startsWith("topic")) {
				if (topicSubscribers.containsKey(address)) {
					topicSubscribers.get(address).add(sender);
				} else {
					topicSubscribers.put(address, new ArrayList<ProtonSender>()).add(sender);
				}
			} else {
				ErrorCondition error = new ErrorCondition();
				error.setDescription("address needs to be prefixed with 'queue' or 'topic'");
				sender.setCondition(error);
				sender.close();
			}
			sender.closeHandler(result -> {
				if (address.startsWith("queue")) {
					if (queueSubscribers.containsKey(address)) {
						queueSubscribers.get(address).consumers.remove(sender);
						if (queueSubscribers.get(address).consumers.size() == 0) {
							queueSubscribers.remove(address);
						}

					}
				} else {
					if (topicSubscribers.containsKey(address)) {
						topicSubscribers.get(address).remove(sender);
						if (topicSubscribers.get(address).size() == 0) {
							topicSubscribers.remove(address);
						}

					}
				}

			});
		});

		connection.openHandler(result -> {
			connection.setContainer("pong: " + connection.getRemoteContainer()).open();
		});

	}

	public void storeMessage(String address, ProtonDelivery delivery, Message msg) {
		if (store.containsKey(address)) {
			store.get(address).add(new StoreEntry(delivery, msg));
		} else {
			store.put(address, new ArrayList<StoreEntry>()).add(new StoreEntry(delivery, msg));
		}
	}

	byte[] tag() {
		return String.valueOf(counter.incrementAndGet()).getBytes();
	}

	public void close() {
		server.close();
	}

	private class StoreEntry {
		public StoreEntry(ProtonDelivery del, Message m) {
			delivery = del;
			msg = m;
		}

		ProtonDelivery delivery;
		Message msg;
	}

	private class Queue {
		int tag = 0;
		List<ProtonSender> consumers = new ArrayList<ProtonSender>();
		int index = 0;

		void sendToNextConsumer(Message msg) {
			if (consumers.size() > 0) {
				next().send(tag(), msg);
			}
		}

		ProtonSender next() {
			if (consumers.size() == 1) {
				return consumers.get(0);
			} else {
				ProtonSender s = consumers.get(index);
				if (index == consumers.size() - 1) {
					index = 0;
				} else {
					index++;
				}
				return s;
			}
		}

		byte[] tag() {
			return String.valueOf(tag++).getBytes();
		}
	}
	
	public static void main(String[] args) throws ExecutionException, InterruptedException{
		MockServer s = new MockServer(Vertx.vertx());
	}
}