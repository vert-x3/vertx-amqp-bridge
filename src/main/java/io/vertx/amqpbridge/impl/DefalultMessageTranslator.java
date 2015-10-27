package io.vertx.amqpbridge.impl;

import io.vertx.amqpbridge.MessageTranslator;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.Map;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;

public class DefalultMessageTranslator implements MessageTranslator {
	@Override
	public Message toAMQP(io.vertx.core.eventbus.Message<?> vertxMsg) {
		Message out = Message.Factory.create();
		Object obj = vertxMsg.body();
		if (obj instanceof JsonObject) {
			JsonObject json = (JsonObject) obj;
			if (json.containsKey("properties")) {
				out.setProperties(new Properties());
				convert(json.getJsonObject("properties"), out.getProperties());
			}
			if (json.containsKey("application_properties")) {
				out.setApplicationProperties(new ApplicationProperties(json.getJsonObject("application_properties")
				        .getMap()));
			}
			handleVertxMsgBody(out, json.getString("body_type"), json.containsKey("body") ? json.getString("body")
			        : json);
		} else {
			handleVertxMsgBody(out, null, obj);
		}
		return out;
	}

	private void handleVertxMsgBody(Message out, String bodyType, Object o) {
		if (bodyType == null || bodyType.equals("value")) {
			if (o instanceof JsonObject) {
				o = ((JsonObject) o).getMap();
			} else if (o instanceof JsonArray) {
				o = ((JsonArray) o).getList();
			}
			out.setBody(new AmqpValue(o));
		} else if (bodyType.equals("data")) {
			out.setBody(new Data(new Binary((byte[]) o)));
		} else if (bodyType.equals("sequence")) {
			out.setBody(new AmqpSequence(((JsonArray) o).getList()));
		} else {
			// TODO Need to throw message format exception
			throw new RuntimeException("Unrecognised body type: " + bodyType);
		}
	}

	@Override
	public JsonObject toVertx(Message amqpMsg) {
		JsonObject out = new JsonObject();
		Properties p = amqpMsg.getProperties();
		if (p != null) {
			JsonObject props = new JsonObject();
			convert(p, props);
			out.put("properties", props);
		}
		ApplicationProperties ap = amqpMsg.getApplicationProperties();
		if (ap != null && ap.getValue() != null) {
			out.put("application_properties", new JsonObject(ap.getValue()));
		}
		Section body = amqpMsg.getBody();
		if (body instanceof AmqpValue) {
			out.put("body", toJsonable(((AmqpValue) body).getValue()));
			out.put("body_type", "value");
		} else if (body instanceof Data) {
			out.put("body", ((Data) body).getValue().getArray());
			out.put("body_type", "data");
		} else if (body instanceof AmqpSequence) {
			out.put("body", new JsonArray(((AmqpSequence) body).getValue()));
			out.put("body_type", "sequence");
		}
		return out;
	}

	private void convert(JsonObject in, Properties out) {
		if (in.containsKey("to")) {
			out.setTo(in.getString("to"));
		}
		if (in.containsKey("subject")) {
			out.setSubject(in.getString("subject"));
		}
		if (in.containsKey("reply_to")) {
			out.setReplyTo(in.getString("reply_to"));
		}
		if (in.containsKey("message_id")) {
			// TODO: handle other types (UUID and long)
			out.setMessageId(in.getString("message_id"));
		}
		if (in.containsKey("correlation_id")) {
			// TODO: handle other types (UUID and long)
			out.setCorrelationId(in.getString("correlation_id"));
		}
		// TODO: handle other fields
	}

	private void convert(Properties in, JsonObject out) {
		if (in.getTo() != null) {
			out.put("to", in.getTo());
		}
		if (in.getSubject() != null) {
			out.put("subject", in.getSubject());
		}
		if (in.getReplyTo() != null) {
			out.put("reply_to", in.getReplyTo());
		}
		if (in.getMessageId() != null) {
			out.put("message_id", in.getMessageId().toString());
		}
		if (in.getCorrelationId() != null) {
			out.put("correlation_id", in.getCorrelationId().toString());
		}
		// TODO: handle other fields
	}

	@SuppressWarnings("rawtypes")
	private static Object toJsonable(Object in) {
		if (in instanceof Number || in instanceof String) {
			return in;
		} else if (in instanceof Map) {
			JsonObject out = new JsonObject();
			for (Object o : ((Map) in).entrySet()) {
				Map.Entry e = (Map.Entry) o;
				out.put((String) e.getKey(), toJsonable(e.getValue()));
			}
			return out;
		} else if (in instanceof List) {
			JsonArray out = new JsonArray();
			for (Object i : (List) in) {
				out.add(toJsonable(i));
			}
			return out;
		} else if (in instanceof Binary) {
			Thread.dumpStack();
			return ((Binary) in).getArray();
		} else {
			throw new RuntimeException("Warning: can't convert object of type " + in.getClass() + " to JSON");
		}
	}
}