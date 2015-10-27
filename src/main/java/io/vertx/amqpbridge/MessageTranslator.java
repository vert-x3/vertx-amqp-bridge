package io.vertx.amqpbridge;

import io.vertx.amqpbridge.impl.DefalultMessageTranslator;
import io.vertx.core.json.JsonObject;

import org.apache.qpid.proton.message.Message;

/**
 * 
 * Allows to plugin different messages translations between AMQP and Vert.x
 * 
 * @author <a href="mailto:rajith@rajith.lk">Rajith Muditha Attapattu</a>
 *
 */
public interface MessageTranslator {

	static MessageTranslator get() {
		return Factory.create();
	}

	/**
	 * Convert a Vert.x message into an AMQP message.
	 * 
	 * @param vertxMsg
	 * @return AMQP Message
	 */
	Message toAMQP(io.vertx.core.eventbus.Message<?> vertxMsg);

	/**
	 * Convert an AMQP message into a Vert.x message
	 * 
	 * @param amqpMsg
	 * @return Vert.x Msg
	 */
	Object toVertx(org.apache.qpid.proton.message.Message amqpMsg);

	// TODO need to find a better way.
	static final class Factory {
		static MessageTranslator instance;

		public static MessageTranslator create() {
			if (instance != null) {
				return instance;
			} else {
				try {
					Class<? extends MessageTranslator> c = (Class<MessageTranslator>) Class.forName(System.getProperty(
					        "vertx-amqp.message-translator", "io.vertx.amqpbridge.impl.DefalultMessageTranslator"));
					instance = c.newInstance();
				} catch (Exception e) {
					// TODO need to raise the exception or log a warning.
					instance = new DefalultMessageTranslator();
				}
				return instance;
			}
		}
	}
}