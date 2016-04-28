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
package io.vertx.amqp.bridge;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.core.net.PfxOptions;
import io.vertx.core.net.SSLEngine;
import io.vertx.proton.ProtonClientOptions;

/**
 * Options for configuring {@link io.vertx.amqp.bridge.Bridge}.
 */
public class BridgeOptions extends ProtonClientOptions {

  @Override
  public BridgeOptions addEnabledSaslMechanism(String saslMechanism) {
    super.addEnabledSaslMechanism(saslMechanism);
    return this;
  }

  @Override
  public BridgeOptions setSendBufferSize(int sendBufferSize) {
    super.setSendBufferSize(sendBufferSize);
    return this;
  }

  @Override
  public BridgeOptions setReceiveBufferSize(int receiveBufferSize) {
    super.setReceiveBufferSize(receiveBufferSize);
    return this;
  }

  @Override
  public BridgeOptions setReuseAddress(boolean reuseAddress) {
    super.setReuseAddress(reuseAddress);
    return this;
  }

  @Override
  public BridgeOptions setTrafficClass(int trafficClass) {
    super.setTrafficClass(trafficClass);
    return this;
  }

  @Override
  public BridgeOptions setTcpNoDelay(boolean tcpNoDelay) {
    super.setTcpNoDelay(tcpNoDelay);
    return this;
  }

  @Override
  public BridgeOptions setTcpKeepAlive(boolean tcpKeepAlive) {
    super.setTcpKeepAlive(tcpKeepAlive);
    return this;
  }

  @Override
  public BridgeOptions setSoLinger(int soLinger) {
    super.setSoLinger(soLinger);
    return this;
  }

  @Override
  public BridgeOptions setUsePooledBuffers(boolean usePooledBuffers) {
    super.setUsePooledBuffers(usePooledBuffers);
    return this;
  }

  @Override
  public BridgeOptions setIdleTimeout(int idleTimeout) {
    super.setIdleTimeout(idleTimeout);
    return this;
  }

  @Override
  public BridgeOptions setSsl(boolean ssl) {
    super.setSsl(ssl);
    return this;
  }

  @Override
  public BridgeOptions setKeyStoreOptions(JksOptions options) {
    super.setKeyStoreOptions(options);
    return this;
  }

  @Override
  public BridgeOptions setPfxKeyCertOptions(PfxOptions options) {
    super.setPfxKeyCertOptions(options);
    return this;
  }

  @Override
  public BridgeOptions setPemKeyCertOptions(PemKeyCertOptions options) {
    super.setPemKeyCertOptions(options);
    return this;
  }

  @Override
  public BridgeOptions setTrustStoreOptions(JksOptions options) {
    super.setTrustStoreOptions(options);
    return this;
  }

  @Override
  public BridgeOptions setPemTrustOptions(PemTrustOptions options) {
    super.setPemTrustOptions(options);
    return this;
  }

  @Override
  public BridgeOptions setPfxTrustOptions(PfxOptions options) {
    super.setPfxTrustOptions(options);
    return this;
  }

  @Override
  public BridgeOptions addEnabledCipherSuite(String suite) {
    super.addEnabledCipherSuite(suite);
    return this;
  }

  @Override
  public BridgeOptions addCrlPath(String crlPath) throws NullPointerException {
    super.addCrlPath(crlPath);
    return this;
  }

  @Override
  public BridgeOptions addCrlValue(Buffer crlValue) throws NullPointerException {
    super.addCrlValue(crlValue);
    return this;
  }

  @Override
  public BridgeOptions setTrustAll(boolean trustAll) {
    super.setTrustAll(trustAll);
    return this;
  }

  @Override
  public BridgeOptions setConnectTimeout(int connectTimeout) {
    super.setConnectTimeout(connectTimeout);
    return this;
  }

  @Override
  public BridgeOptions setReconnectAttempts(int attempts) {
    super.setReconnectAttempts(attempts);
    return this;
  }

  @Override
  public BridgeOptions setReconnectInterval(long interval) {
    super.setReconnectInterval(interval);
    return this;
  }

  @Override
  public BridgeOptions setSslEngine(SSLEngine sslEngine) {
    super.setSslEngine(sslEngine);
    return this;
  }

  @Override
  public BridgeOptions setUseAlpn(boolean useAlpn) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BridgeOptions addEnabledSecureTransportProtocol(String protocol) {
    super.addEnabledSecureTransportProtocol(protocol);
    return this;
  }

  @Override
  public BridgeOptions setHostnameVerificationAlgorithm(String hostnameVerificationAlgorithm) {
    super.setHostnameVerificationAlgorithm(hostnameVerificationAlgorithm);
    return this;
  }

}
