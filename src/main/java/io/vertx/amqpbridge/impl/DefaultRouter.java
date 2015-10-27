package io.vertx.amqpbridge.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import io.vertx.amqpbridge.Router;
import io.vertx.core.json.JsonObject;

/**
 * A generic AMQP router that maps AMQP Address <--> Vert.x Address
 * 
 * @author <a href="mailto:rajith@rajith.lk">Rajith Muditha Attapattu</a>
 *
 */
public class DefaultRouter implements Router {

	static final LogManager LOG = LogManager.get("MessageRouter:", DefaultRouter.class);

	// If present will be used as the routing-key instead of address.
	static String VERTX_ROUTING_KEY = "vertx.routing-key";
	static String VERTX_MSG_PROPS = "properties";
	static String VERTX_MSG_APP_PROPS = "application-properties";

	String defaultOutgoingAddress;
	String defaultIncomingAddress;

	Map<String, RouteEntry> _outboundRoutes = new ConcurrentHashMap<String, RouteEntry>();

	Map<String, RouteEntry> _inboundRoutes = new ConcurrentHashMap<String, RouteEntry>();

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
	 * Maps an AMQP subscription (Ex. Queue, Topic ..etc) to a Vert.x address
	 * pattern For now this is going to be a one to one. But once we get
	 * notified of the handler/registration and de-registration we can do wild
	 * card matching
	 */
	@Override
	public void addIncomingRoute(String pattern, String amqpAddress) {
		if (_inboundRoutes.containsKey(pattern)) {
			_inboundRoutes.get(pattern).add(amqpAddress);
		} else {
			_inboundRoutes.put(pattern, new RouteEntry(Pattern.compile(pattern), amqpAddress));
		}
	}

	/* Maps a Vert.x address pattern to an AMQP destination */
	@Override
	public void addOutgoingRoute(String pattern, String amqpAddress) {
		if (_outboundRoutes.containsKey(pattern)) {
			_outboundRoutes.get(pattern).add(amqpAddress);
		} else {
			_outboundRoutes.put(pattern, new RouteEntry(Pattern.compile(pattern), amqpAddress));
		}
	}

	@Override
	public void removeIncomingRoute(String pattern, String amqpAddress) {
		if (_inboundRoutes.containsKey(pattern)) {
			_inboundRoutes.get(pattern).remove(amqpAddress);
			if (_inboundRoutes.get(pattern).getAddressListSize() == 0) {
				_inboundRoutes.remove(pattern);
			}
		}
	}

	@Override
	public void removeOutgoingRoute(String pattern, String amqpAddress) {
		if (_outboundRoutes.containsKey(pattern)) {
			_outboundRoutes.get(pattern).remove(amqpAddress);
			if (_outboundRoutes.get(pattern).getAddressListSize() == 0) {
				_outboundRoutes.remove(pattern);
			}
		}
	}

	@Override
	public List<String> routeIncoming(org.apache.qpid.proton.message.Message amqpMsg) {
		// Could look at other message properties later on
		String routingKey = amqpMsg.getAddress();
		List<String> addrList = new ArrayList<String>();
		for (String k : _inboundRoutes.keySet()) {
			RouteEntry route = _inboundRoutes.get(k);
			if (route.getPattern().matcher(routingKey).matches()) {
				addrList.addAll(route.getAddressList());
			}
		}
		if (addrList.size() == 0) {
			addrList.add(defaultIncomingAddress);
		}
		return addrList;
	}

	@Override
	public String routeOutgoing(io.vertx.core.eventbus.Message<?> vertxMsg) {

		String routingKey = extractOutgoingRoutingKey(vertxMsg);
		String addr = null;
		for (String key : _outboundRoutes.keySet()) {
			RouteEntry route = _outboundRoutes.get(key);
			if (route.getPattern().matcher(routingKey).matches()) {
				addr = route.getAddressList().get(0);
			}
		}
		if (addr == null) {
			return defaultOutgoingAddress;
		} else {
			return addr;
		}
	}

	// In case you want to override the address as the routing-key
	String extractOutgoingRoutingKey(io.vertx.core.eventbus.Message<?> vertxMsg) {
		String routingKey = routingKey = vertxMsg.address();

		if (vertxMsg.body() instanceof JsonObject) {
			JsonObject body = (JsonObject) vertxMsg.body();
			if (body.containsKey(VERTX_ROUTING_KEY)) {
				routingKey = body.getString(VERTX_ROUTING_KEY);
			} else if (body.containsKey("VERTX_MSG_PROPS") && body.getJsonObject("VERTX_MSG_PROPS") instanceof Map
			        && body.getJsonObject("VERTX_MSG_PROPS").containsKey(VERTX_ROUTING_KEY)) {
				routingKey = body.getJsonObject("VERTX_MSG_PROPS").getString(VERTX_ROUTING_KEY);
			} else if (body.containsKey("VERTX_MSG_APP_PROPS")
			        && body.getJsonObject("VERTX_MSG_APP_PROPS") instanceof Map
			        && body.getJsonObject("VERTX_MSG_APP_PROPS").containsKey(VERTX_ROUTING_KEY)) {
				routingKey = body.getJsonObject("VERTX_MSG_APP_PROPS").getString(VERTX_ROUTING_KEY);
			}
		}
		return routingKey;
	}
}