package io.vertx.amqpbridge.impl;

import io.vertx.amqpbridge.Router;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A generic AMQP router that maps AMQP Address <--> Vert.x Address
 * 
 * @author <a href="mailto:rajith@rajith.lk">Rajith Muditha Attapattu</a>
 *
 */
public class DefaultRouter implements Router {

	static final LogManager LOG = LogManager.get("MessageRouter:", DefaultRouter.class);
	String defaultOutgoingAddress;
	String defaultIncomingAddress;

	Map<String, String> _outboundRoutes = new ConcurrentHashMap<String, String>();

	Map<String, String> _inboundRoutes = new ConcurrentHashMap<String, String>();

	public DefaultRouter(String defaultIncomingAddr, String defaultOutgoingAddr) {
		defaultIncomingAddress = defaultIncomingAddr;
		defaultOutgoingAddress = defaultOutgoingAddr;
		if (LOG.isInfoEnabled()) {
			StringBuilder b = new StringBuilder();
			b.append("========== DefaultAMQPRouter ===========");
			b.append("Default Outgoing AMQP Address : " + defaultOutgoingAddress);
			b.append("Default Incoming Vertx Address : " + defaultIncomingAddress);
			b.append("========================================");
			LOG.info(b.toString());
		}
	}

	/*
	 * Maps an AMQP subscription (Ex. Queue, Topic ..etc) to a Vert.x address.
	 */
	@Override
	public void addIncomingRoute(String amqpAddress, String vertxAddress) {
		_inboundRoutes.put(amqpAddress, vertxAddress);
	}

	/* Maps a Vert.x address to an AMQP destination */
	@Override
	public void addOutgoingRoute(String vertxAddress, String amqpAddress) {
		_outboundRoutes.put(vertxAddress, amqpAddress);
	}

	@Override
	public void removeIncomingRoute(String amqpAddress, String vertxAddress) {
		_inboundRoutes.remove(amqpAddress);
	}

	@Override
	public void removeOutgoingRoute(String vertxAddress, String amqpAddress) {
		_outboundRoutes.remove(vertxAddress);
	}

	@Override
	public String routeIncoming(org.apache.qpid.proton.message.Message amqpMsg) {
		// Could look at other message properties later on
		if (_inboundRoutes.containsKey(amqpMsg.getAddress())) {
			return _inboundRoutes.get(amqpMsg.getAddress());
		} else {
			return defaultIncomingAddress;
		}
	}

	@Override
	public String routeOutgoing(io.vertx.core.eventbus.Message<?> vertxMsg) {

		if (_outboundRoutes.containsKey(vertxMsg.address())) {
			return _outboundRoutes.get(vertxMsg.address());
		} else {
			return defaultOutgoingAddress;
		}
	}
}