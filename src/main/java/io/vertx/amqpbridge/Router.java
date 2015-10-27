package io.vertx.amqpbridge;

import io.vertx.amqpbridge.MessageTranslator.Factory;
import io.vertx.amqpbridge.impl.DefaultRouter;
import io.vertx.amqpbridge.impl.DefalultMessageTranslator;
import io.vertx.core.json.JsonObject;

import java.lang.reflect.Constructor;
import java.util.List;

/**
 * Allows different routing implementations to be used.
 * 
 * @author <a href="mailto:rajith@rajith.lk">Rajith Muditha Attapattu</a>
 * 
 */
public interface Router {

	static Router get(String defaultIncomingAddr, String defaultOutgoingAddr) {
		return Factory.create(defaultIncomingAddr, defaultOutgoingAddr);
	}

	/**
	 * Maps an AMQP subscription (Ex. Queue, Topic ..etc) to a Vert.x address
	 * pattern
	 * 
	 * @param pattern
	 * @param amqpAddress
	 */
	void addIncomingRoute(String pattern, String amqpAddress);

	/**
	 * Maps a Vert.x address pattern to an AMQP destination
	 * 
	 * @param pattern
	 * @param amqpAddress
	 */
	void addOutgoingRoute(String pattern, String amqpAddress);

	/**
	 * Removes the mapping for an AMQP subscription (Ex. Queue, Topic ..etc) to
	 * a Vert.x address pattern
	 * 
	 * @param pattern
	 * @param vertxAddress
	 */
	void removeIncomingRoute(String pattern, String amqpAddress);

	/**
	 * Removes the mapping for a Vert.x address pattern to an AMQP destination
	 * 
	 * @param pattern
	 * @param amqpAddress
	 */
	void removeOutgoingRoute(String pattern, String amqpAddress);

	/**
	 * Maps an AMQP Message to one or many Vert.x addresses
	 * 
	 * @param amqpMsg
	 * @return A list Vert.x address
	 */
	List<String> routeIncoming(org.apache.qpid.proton.message.Message amqpMsg);

	/**
	 * Maps a Vert.x Message to an AMQP address
	 * 
	 * @param vertxMsg
	 * @return A list of AMQP addresses
	 */
	String routeOutgoing(io.vertx.core.eventbus.Message<?> vertxMsg);

	// TODO need to find a better way.
	static final class Factory {
		static Router instance;

		public static Router create(String defaultIncomingAddr, String defaultOutgoingAddr) {
			if (instance != null) {
				return instance;
			} else {
				try {
					Class<? extends Router> c = (Class<Router>) Class.forName(System.getProperty(
					        "vertx-amqp.message-translator", "io.vertx.amqpbridge.impl.DefalultMessageTranslator"));
					Constructor<? extends Router> ctor = c.getConstructor(String.class, String.class);
					instance = ctor.newInstance(defaultIncomingAddr, defaultOutgoingAddr);
				} catch (Exception e) {
					// TODO need to raise the exception or log a warning.
					instance = new DefaultRouter(defaultIncomingAddr, defaultOutgoingAddr);
				}
				return instance;
			}
		}
	}
}