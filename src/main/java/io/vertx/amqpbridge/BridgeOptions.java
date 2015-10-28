package io.vertx.amqpbridge;

import io.vertx.core.json.JsonObject;

public class BridgeOptions {

	private int outboundAMQPPort = 5672;
	private String outboundAMQPHost = "localhost";
	private int inboundAMQPPort = 5673;
	private String inboundAMQPHost = "localhost";
	private int defaultPrefetch = 10;

	public BridgeOptions() {
	}

	public BridgeOptions(JsonObject config) {
		// TODO
	}

	public int getOutboundAMQPPort() {
		return outboundAMQPPort;
	}

	public void setOutboundAMQPPort(int outboundAMQPPort) {
		this.outboundAMQPPort = outboundAMQPPort;
	}

	public String getOutboundAMQPHost() {
		return outboundAMQPHost;
	}

	public void setOutboundAMQPHost(String outboundAMQPHost) {
		this.outboundAMQPHost = outboundAMQPHost;
	}

	public int getInboundAMQPPort() {
		return inboundAMQPPort;
	}

	public void setInboundAMQPPort(int inboundAMQPPort) {
		this.inboundAMQPPort = inboundAMQPPort;
	}

	public String getInboundAMQPHost() {
		return inboundAMQPHost;
	}

	public void setInboundAMQPHost(String inboundAMQPHost) {
		this.inboundAMQPHost = inboundAMQPHost;
	}

	public int getDefaultPrefetch() {
		return defaultPrefetch;
	}

	public void setDefaultPrefetch(int defaultPrefetch) {
		this.defaultPrefetch = defaultPrefetch;
	}
}