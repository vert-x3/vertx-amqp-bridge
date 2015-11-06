package io.vertx.amqpbridge.impl;

import io.vertx.amqpbridge.MessageTranslator;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.message.Message;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefalultMessageTranslator implements MessageTranslator {

	private static boolean isJMSAnnotated = Boolean.getBoolean("vertx-amqp.jms-annotation");
	static final Symbol JMS_ANNOTATION_HEADER = Symbol.valueOf("x-opt-jms-msg-type");
	static final byte JMS_MAP_MESSAGE = 2;
	public static final byte JMS_STREAM_MESSAGE = 4;

	@Override
	public Message toAMQP(io.vertx.core.eventbus.Message<?> vertxMsg) {
		Message out = Message.Factory.create();
		Object obj = vertxMsg.body();
		if (obj instanceof byte[]) {
			out.setBody(new Data(new Binary((byte[]) obj)));
		} else {
			if (isJMSAnnotated) {
				Map<Symbol, Object> annotations = new HashMap<Symbol, Object>();
				if (obj instanceof Map) {
					annotations.put(JMS_ANNOTATION_HEADER, JMS_MAP_MESSAGE);
				} else if (obj instanceof List) {
					annotations.put(JMS_ANNOTATION_HEADER, JMS_STREAM_MESSAGE);
				}
				out.setMessageAnnotations(new MessageAnnotations(annotations));
			}
			out.setBody(new AmqpValue(obj));
		}
		return out;
	}

	@Override
	public Object toVertx(Message amqpMsg) {
		Section body = amqpMsg.getBody();
		if (body instanceof AmqpValue) {
			return ((AmqpValue) body).getValue();
		} else if (body instanceof Data) {
			return ((Data) body).getValue().getArray();
		} else if (body instanceof AmqpSequence) {
			return ((AmqpSequence) body).getValue();
		} else {
			throw new RuntimeException("Unknown AMQP message body");
		}
	}
}