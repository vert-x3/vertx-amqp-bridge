package io.vertx.amqpbridge;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Converter for {@link io.vertx.amqpbridge.AmqpBridgeOptions}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.amqpbridge.AmqpBridgeOptions} original class using Vert.x codegen.
 */
public class AmqpBridgeOptionsConverter {

  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, AmqpBridgeOptions obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "connectTimeout":
          if (member.getValue() instanceof Number) {
            obj.setConnectTimeout(((Number)member.getValue()).intValue());
          }
          break;
        case "containerId":
          if (member.getValue() instanceof String) {
            obj.setContainerId((String)member.getValue());
          }
          break;
        case "crlPaths":
          if (member.getValue() instanceof JsonArray) {
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof String)
                obj.addCrlPath((String)item);
            });
          }
          break;
        case "crlValues":
          if (member.getValue() instanceof JsonArray) {
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof String)
                obj.addCrlValue(io.vertx.core.buffer.Buffer.buffer(java.util.Base64.getDecoder().decode((String)item)));
            });
          }
          break;
        case "enabledCipherSuites":
          if (member.getValue() instanceof JsonArray) {
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof String)
                obj.addEnabledCipherSuite((String)item);
            });
          }
          break;
        case "enabledSaslMechanisms":
          if (member.getValue() instanceof JsonArray) {
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof String)
                obj.addEnabledSaslMechanism((String)item);
            });
          }
          break;
        case "enabledSecureTransportProtocols":
          if (member.getValue() instanceof JsonArray) {
            java.util.LinkedHashSet<java.lang.String> list =  new java.util.LinkedHashSet<>();
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof String)
                list.add((String)item);
            });
            obj.setEnabledSecureTransportProtocols(list);
          }
          break;
        case "heartbeat":
          if (member.getValue() instanceof Number) {
            obj.setHeartbeat(((Number)member.getValue()).intValue());
          }
          break;
        case "hostnameVerificationAlgorithm":
          if (member.getValue() instanceof String) {
            obj.setHostnameVerificationAlgorithm((String)member.getValue());
          }
          break;
        case "idleTimeout":
          if (member.getValue() instanceof Number) {
            obj.setIdleTimeout(((Number)member.getValue()).intValue());
          }
          break;
        case "idleTimeoutUnit":
          if (member.getValue() instanceof String) {
            obj.setIdleTimeoutUnit(java.util.concurrent.TimeUnit.valueOf((String)member.getValue()));
          }
          break;
        case "jdkSslEngineOptions":
          if (member.getValue() instanceof JsonObject) {
            obj.setJdkSslEngineOptions(new io.vertx.core.net.JdkSSLEngineOptions((JsonObject)member.getValue()));
          }
          break;
        case "keyStoreOptions":
          if (member.getValue() instanceof JsonObject) {
            obj.setKeyStoreOptions(new io.vertx.core.net.JksOptions((JsonObject)member.getValue()));
          }
          break;
        case "localAddress":
          if (member.getValue() instanceof String) {
            obj.setLocalAddress((String)member.getValue());
          }
          break;
        case "logActivity":
          if (member.getValue() instanceof Boolean) {
            obj.setLogActivity((Boolean)member.getValue());
          }
          break;
        case "maxFrameSize":
          if (member.getValue() instanceof Number) {
            obj.setMaxFrameSize(((Number)member.getValue()).intValue());
          }
          break;
        case "metricsName":
          if (member.getValue() instanceof String) {
            obj.setMetricsName((String)member.getValue());
          }
          break;
        case "openSslEngineOptions":
          if (member.getValue() instanceof JsonObject) {
            obj.setOpenSslEngineOptions(new io.vertx.core.net.OpenSSLEngineOptions((JsonObject)member.getValue()));
          }
          break;
        case "pemKeyCertOptions":
          if (member.getValue() instanceof JsonObject) {
            obj.setPemKeyCertOptions(new io.vertx.core.net.PemKeyCertOptions((JsonObject)member.getValue()));
          }
          break;
        case "pemTrustOptions":
          if (member.getValue() instanceof JsonObject) {
            obj.setPemTrustOptions(new io.vertx.core.net.PemTrustOptions((JsonObject)member.getValue()));
          }
          break;
        case "pfxKeyCertOptions":
          if (member.getValue() instanceof JsonObject) {
            obj.setPfxKeyCertOptions(new io.vertx.core.net.PfxOptions((JsonObject)member.getValue()));
          }
          break;
        case "pfxTrustOptions":
          if (member.getValue() instanceof JsonObject) {
            obj.setPfxTrustOptions(new io.vertx.core.net.PfxOptions((JsonObject)member.getValue()));
          }
          break;
        case "proxyOptions":
          if (member.getValue() instanceof JsonObject) {
            obj.setProxyOptions(new io.vertx.core.net.ProxyOptions((JsonObject)member.getValue()));
          }
          break;
        case "receiveBufferSize":
          if (member.getValue() instanceof Number) {
            obj.setReceiveBufferSize(((Number)member.getValue()).intValue());
          }
          break;
        case "reconnectAttempts":
          if (member.getValue() instanceof Number) {
            obj.setReconnectAttempts(((Number)member.getValue()).intValue());
          }
          break;
        case "reconnectInterval":
          if (member.getValue() instanceof Number) {
            obj.setReconnectInterval(((Number)member.getValue()).longValue());
          }
          break;
        case "replyHandlingSupport":
          if (member.getValue() instanceof Boolean) {
            obj.setReplyHandlingSupport((Boolean)member.getValue());
          }
          break;
        case "reuseAddress":
          if (member.getValue() instanceof Boolean) {
            obj.setReuseAddress((Boolean)member.getValue());
          }
          break;
        case "reusePort":
          if (member.getValue() instanceof Boolean) {
            obj.setReusePort((Boolean)member.getValue());
          }
          break;
        case "sendBufferSize":
          if (member.getValue() instanceof Number) {
            obj.setSendBufferSize(((Number)member.getValue()).intValue());
          }
          break;
        case "sniServerName":
          if (member.getValue() instanceof String) {
            obj.setSniServerName((String)member.getValue());
          }
          break;
        case "soLinger":
          if (member.getValue() instanceof Number) {
            obj.setSoLinger(((Number)member.getValue()).intValue());
          }
          break;
        case "ssl":
          if (member.getValue() instanceof Boolean) {
            obj.setSsl((Boolean)member.getValue());
          }
          break;
        case "sslHandshakeTimeout":
          if (member.getValue() instanceof Number) {
            obj.setSslHandshakeTimeout(((Number)member.getValue()).longValue());
          }
          break;
        case "sslHandshakeTimeoutUnit":
          if (member.getValue() instanceof String) {
            obj.setSslHandshakeTimeoutUnit(java.util.concurrent.TimeUnit.valueOf((String)member.getValue()));
          }
          break;
        case "tcpCork":
          if (member.getValue() instanceof Boolean) {
            obj.setTcpCork((Boolean)member.getValue());
          }
          break;
        case "tcpFastOpen":
          if (member.getValue() instanceof Boolean) {
            obj.setTcpFastOpen((Boolean)member.getValue());
          }
          break;
        case "tcpKeepAlive":
          if (member.getValue() instanceof Boolean) {
            obj.setTcpKeepAlive((Boolean)member.getValue());
          }
          break;
        case "tcpNoDelay":
          if (member.getValue() instanceof Boolean) {
            obj.setTcpNoDelay((Boolean)member.getValue());
          }
          break;
        case "tcpQuickAck":
          if (member.getValue() instanceof Boolean) {
            obj.setTcpQuickAck((Boolean)member.getValue());
          }
          break;
        case "tcpUserTimeout":
          if (member.getValue() instanceof Number) {
            obj.setTcpUserTimeout(((Number)member.getValue()).intValue());
          }
          break;
        case "trafficClass":
          if (member.getValue() instanceof Number) {
            obj.setTrafficClass(((Number)member.getValue()).intValue());
          }
          break;
        case "trustAll":
          if (member.getValue() instanceof Boolean) {
            obj.setTrustAll((Boolean)member.getValue());
          }
          break;
        case "trustStoreOptions":
          if (member.getValue() instanceof JsonObject) {
            obj.setTrustStoreOptions(new io.vertx.core.net.JksOptions((JsonObject)member.getValue()));
          }
          break;
        case "useAlpn":
          if (member.getValue() instanceof Boolean) {
            obj.setUseAlpn((Boolean)member.getValue());
          }
          break;
        case "usePooledBuffers":
          if (member.getValue() instanceof Boolean) {
            obj.setUsePooledBuffers((Boolean)member.getValue());
          }
          break;
        case "vhost":
          if (member.getValue() instanceof String) {
            obj.setVhost((String)member.getValue());
          }
          break;
        case "virtualHost":
          if (member.getValue() instanceof String) {
            obj.setVirtualHost((String)member.getValue());
          }
          break;
      }
    }
  }

  public static void toJson(AmqpBridgeOptions obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(AmqpBridgeOptions obj, java.util.Map<String, Object> json) {
    json.put("connectTimeout", obj.getConnectTimeout());
    if (obj.getContainerId() != null) {
      json.put("containerId", obj.getContainerId());
    }
    if (obj.getCrlPaths() != null) {
      JsonArray array = new JsonArray();
      obj.getCrlPaths().forEach(item -> array.add(item));
      json.put("crlPaths", array);
    }
    if (obj.getCrlValues() != null) {
      JsonArray array = new JsonArray();
      obj.getCrlValues().forEach(item -> array.add(java.util.Base64.getEncoder().encodeToString(item.getBytes())));
      json.put("crlValues", array);
    }
    if (obj.getEnabledCipherSuites() != null) {
      JsonArray array = new JsonArray();
      obj.getEnabledCipherSuites().forEach(item -> array.add(item));
      json.put("enabledCipherSuites", array);
    }
    if (obj.getEnabledSaslMechanisms() != null) {
      JsonArray array = new JsonArray();
      obj.getEnabledSaslMechanisms().forEach(item -> array.add(item));
      json.put("enabledSaslMechanisms", array);
    }
    if (obj.getEnabledSecureTransportProtocols() != null) {
      JsonArray array = new JsonArray();
      obj.getEnabledSecureTransportProtocols().forEach(item -> array.add(item));
      json.put("enabledSecureTransportProtocols", array);
    }
    json.put("heartbeat", obj.getHeartbeat());
    if (obj.getHostnameVerificationAlgorithm() != null) {
      json.put("hostnameVerificationAlgorithm", obj.getHostnameVerificationAlgorithm());
    }
    json.put("idleTimeout", obj.getIdleTimeout());
    if (obj.getIdleTimeoutUnit() != null) {
      json.put("idleTimeoutUnit", obj.getIdleTimeoutUnit().name());
    }
    if (obj.getJdkSslEngineOptions() != null) {
      json.put("jdkSslEngineOptions", obj.getJdkSslEngineOptions().toJson());
    }
    if (obj.getKeyStoreOptions() != null) {
      json.put("keyStoreOptions", obj.getKeyStoreOptions().toJson());
    }
    if (obj.getLocalAddress() != null) {
      json.put("localAddress", obj.getLocalAddress());
    }
    json.put("logActivity", obj.getLogActivity());
    json.put("maxFrameSize", obj.getMaxFrameSize());
    if (obj.getMetricsName() != null) {
      json.put("metricsName", obj.getMetricsName());
    }
    if (obj.getOpenSslEngineOptions() != null) {
      json.put("openSslEngineOptions", obj.getOpenSslEngineOptions().toJson());
    }
    if (obj.getPemKeyCertOptions() != null) {
      json.put("pemKeyCertOptions", obj.getPemKeyCertOptions().toJson());
    }
    if (obj.getPemTrustOptions() != null) {
      json.put("pemTrustOptions", obj.getPemTrustOptions().toJson());
    }
    if (obj.getPfxKeyCertOptions() != null) {
      json.put("pfxKeyCertOptions", obj.getPfxKeyCertOptions().toJson());
    }
    if (obj.getPfxTrustOptions() != null) {
      json.put("pfxTrustOptions", obj.getPfxTrustOptions().toJson());
    }
    if (obj.getProxyOptions() != null) {
      json.put("proxyOptions", obj.getProxyOptions().toJson());
    }
    json.put("receiveBufferSize", obj.getReceiveBufferSize());
    json.put("reconnectAttempts", obj.getReconnectAttempts());
    json.put("reconnectInterval", obj.getReconnectInterval());
    json.put("replyHandlingSupport", obj.isReplyHandlingSupport());
    json.put("reuseAddress", obj.isReuseAddress());
    json.put("reusePort", obj.isReusePort());
    json.put("sendBufferSize", obj.getSendBufferSize());
    if (obj.getSniServerName() != null) {
      json.put("sniServerName", obj.getSniServerName());
    }
    json.put("soLinger", obj.getSoLinger());
    json.put("ssl", obj.isSsl());
    json.put("sslHandshakeTimeout", obj.getSslHandshakeTimeout());
    if (obj.getSslHandshakeTimeoutUnit() != null) {
      json.put("sslHandshakeTimeoutUnit", obj.getSslHandshakeTimeoutUnit().name());
    }
    json.put("tcpCork", obj.isTcpCork());
    json.put("tcpFastOpen", obj.isTcpFastOpen());
    json.put("tcpKeepAlive", obj.isTcpKeepAlive());
    json.put("tcpNoDelay", obj.isTcpNoDelay());
    json.put("tcpQuickAck", obj.isTcpQuickAck());
    json.put("tcpUserTimeout", obj.getTcpUserTimeout());
    json.put("trafficClass", obj.getTrafficClass());
    json.put("trustAll", obj.isTrustAll());
    if (obj.getTrustStoreOptions() != null) {
      json.put("trustStoreOptions", obj.getTrustStoreOptions().toJson());
    }
    json.put("useAlpn", obj.isUseAlpn());
    json.put("usePooledBuffers", obj.isUsePooledBuffers());
    if (obj.getVhost() != null) {
      json.put("vhost", obj.getVhost());
    }
    if (obj.getVirtualHost() != null) {
      json.put("virtualHost", obj.getVirtualHost());
    }
  }
}
