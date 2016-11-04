package io.vertx.groovy.amqpbridge;
public class GroovyStaticExtension {
  public static io.vertx.amqpbridge.AmqpBridge create(io.vertx.amqpbridge.AmqpBridge j_receiver, io.vertx.core.Vertx vertx, java.util.Map<String, Object> options) {
    return io.vertx.lang.groovy.ConversionHelper.wrap(io.vertx.amqpbridge.AmqpBridge.create(vertx,
      options != null ? new io.vertx.amqpbridge.AmqpBridgeOptions(io.vertx.lang.groovy.ConversionHelper.toJsonObject(options)) : null));
  }
}
