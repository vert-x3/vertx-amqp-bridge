/*
* Copyright 2016 the original author or authors.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package io.vertx.amqpbridge;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JdkSSLEngineOptions;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.KeyCertOptions;
import io.vertx.core.net.OpenSSLEngineOptions;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.core.net.PfxOptions;
import io.vertx.core.net.ProxyOptions;
import io.vertx.core.net.SSLEngineOptions;
import io.vertx.core.net.TrustOptions;
import io.vertx.proton.ProtonClientOptions;

/**
 * Options for configuring the AmqpBridge.
 */
@DataObject(generateConverter = true, inheritConverter = true)
public class AmqpBridgeOptions extends ProtonClientOptions {

  public AmqpBridgeOptions() {
  }

  public AmqpBridgeOptions(JsonObject json) {
    AmqpBridgeOptionsConverter.fromJson(json, this);
  }

  @Override
  public AmqpBridgeOptions addEnabledSaslMechanism(String saslMechanism) {
    super.addEnabledSaslMechanism(saslMechanism);
    return this;
  }

  @Override
  public AmqpBridgeOptions setSendBufferSize(int sendBufferSize) {
    super.setSendBufferSize(sendBufferSize);
    return this;
  }

  @Override
  public AmqpBridgeOptions setReceiveBufferSize(int receiveBufferSize) {
    super.setReceiveBufferSize(receiveBufferSize);
    return this;
  }

  @Override
  public AmqpBridgeOptions setReuseAddress(boolean reuseAddress) {
    super.setReuseAddress(reuseAddress);
    return this;
  }

  @Override
  public AmqpBridgeOptions setTrafficClass(int trafficClass) {
    super.setTrafficClass(trafficClass);
    return this;
  }

  @Override
  public AmqpBridgeOptions setTcpNoDelay(boolean tcpNoDelay) {
    super.setTcpNoDelay(tcpNoDelay);
    return this;
  }

  @Override
  public AmqpBridgeOptions setTcpKeepAlive(boolean tcpKeepAlive) {
    super.setTcpKeepAlive(tcpKeepAlive);
    return this;
  }

  @Override
  public AmqpBridgeOptions setSoLinger(int soLinger) {
    super.setSoLinger(soLinger);
    return this;
  }

  @Override
  public AmqpBridgeOptions setUsePooledBuffers(boolean usePooledBuffers) {
    super.setUsePooledBuffers(usePooledBuffers);
    return this;
  }

  @Override
  public AmqpBridgeOptions setIdleTimeout(int idleTimeout) {
    super.setIdleTimeout(idleTimeout);
    return this;
  }

  @Override
  public AmqpBridgeOptions setSsl(boolean ssl) {
    super.setSsl(ssl);
    return this;
  }

  @Override
  public AmqpBridgeOptions setKeyStoreOptions(JksOptions options) {
    super.setKeyStoreOptions(options);
    return this;
  }

  @Override
  public AmqpBridgeOptions setPfxKeyCertOptions(PfxOptions options) {
    super.setPfxKeyCertOptions(options);
    return this;
  }

  @Override
  public AmqpBridgeOptions setPemKeyCertOptions(PemKeyCertOptions options) {
    super.setPemKeyCertOptions(options);
    return this;
  }

  @Override
  public AmqpBridgeOptions setTrustStoreOptions(JksOptions options) {
    super.setTrustStoreOptions(options);
    return this;
  }

  @Override
  public AmqpBridgeOptions setPemTrustOptions(PemTrustOptions options) {
    super.setPemTrustOptions(options);
    return this;
  }

  @Override
  public AmqpBridgeOptions setPfxTrustOptions(PfxOptions options) {
    super.setPfxTrustOptions(options);
    return this;
  }

  @Override
  public AmqpBridgeOptions addEnabledCipherSuite(String suite) {
    super.addEnabledCipherSuite(suite);
    return this;
  }

  @Override
  public AmqpBridgeOptions addCrlPath(String crlPath) throws NullPointerException {
    super.addCrlPath(crlPath);
    return this;
  }

  @Override
  public AmqpBridgeOptions addCrlValue(Buffer crlValue) throws NullPointerException {
    super.addCrlValue(crlValue);
    return this;
  }

  @Override
  public AmqpBridgeOptions setTrustAll(boolean trustAll) {
    super.setTrustAll(trustAll);
    return this;
  }

  @Override
  public AmqpBridgeOptions setConnectTimeout(int connectTimeout) {
    super.setConnectTimeout(connectTimeout);
    return this;
  }

  @Override
  public AmqpBridgeOptions setReconnectAttempts(int attempts) {
    super.setReconnectAttempts(attempts);
    return this;
  }

  @Override
  public AmqpBridgeOptions setReconnectInterval(long interval) {
    super.setReconnectInterval(interval);
    return this;
  }

  @Override
  public AmqpBridgeOptions setUseAlpn(boolean useAlpn) {
    throw new UnsupportedOperationException();
  }

  @Override
  public AmqpBridgeOptions addEnabledSecureTransportProtocol(String protocol) {
    super.addEnabledSecureTransportProtocol(protocol);
    return this;
  }

  @Override
  public AmqpBridgeOptions setHostnameVerificationAlgorithm(String hostnameVerificationAlgorithm) {
    super.setHostnameVerificationAlgorithm(hostnameVerificationAlgorithm);
    return this;
  }

  @Override
  public AmqpBridgeOptions setKeyCertOptions(KeyCertOptions options) {
    super.setKeyCertOptions(options);
    return this;
  }

  @Override
  public AmqpBridgeOptions setLogActivity(boolean logEnabled) {
    super.setLogActivity(logEnabled);
    return this;
  }

  @Override
  public AmqpBridgeOptions setMetricsName(String metricsName) {
    super.setMetricsName(metricsName);
    return this;
  }

  @Override
  public AmqpBridgeOptions setProxyOptions(ProxyOptions proxyOptions) {
    super.setProxyOptions(proxyOptions);
    return this;
  }

  @Override
  public AmqpBridgeOptions setTrustOptions(TrustOptions options) {
    super.setTrustOptions(options);
    return this;
  }

  @Override
  public AmqpBridgeOptions setJdkSslEngineOptions(JdkSSLEngineOptions sslEngineOptions) {
    super.setJdkSslEngineOptions(sslEngineOptions);
    return this;
  }

  @Override
  public AmqpBridgeOptions setOpenSslEngineOptions(OpenSSLEngineOptions sslEngineOptions) {
    super.setOpenSslEngineOptions(sslEngineOptions);
    return this;
  }

  @Override
  public AmqpBridgeOptions setSslEngineOptions(SSLEngineOptions sslEngineOptions) {
    super.setSslEngineOptions(sslEngineOptions);
    return this;
  }
}
