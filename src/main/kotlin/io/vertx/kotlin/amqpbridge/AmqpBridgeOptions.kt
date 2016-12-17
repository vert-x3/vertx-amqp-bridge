package io.vertx.kotlin.amqpbridge

import io.vertx.amqpbridge.AmqpBridgeOptions

fun AmqpBridgeOptions(
    connectTimeout: Int? = null,
  containerId: String? = null,
  heartbeat: Int? = null,
  hostnameVerificationAlgorithm: String? = null,
  idleTimeout: Int? = null,
  localAddress: String? = null,
  logActivity: Boolean? = null,
  metricsName: String? = null,
  proxyOptions: io.vertx.core.net.ProxyOptions? = null,
  receiveBufferSize: Int? = null,
  reconnectAttempts: Int? = null,
  reconnectInterval: Long? = null,
  replyHandlingSupport: Boolean? = null,
  reuseAddress: Boolean? = null,
  sendBufferSize: Int? = null,
  soLinger: Int? = null,
  ssl: Boolean? = null,
  tcpKeepAlive: Boolean? = null,
  tcpNoDelay: Boolean? = null,
  trafficClass: Int? = null,
  trustAll: Boolean? = null,
  useAlpn: Boolean? = null,
  usePooledBuffers: Boolean? = null,
  vhost: String? = null): AmqpBridgeOptions = io.vertx.amqpbridge.AmqpBridgeOptions().apply {

  if (connectTimeout != null) {
    this.connectTimeout = connectTimeout
  }

  if (containerId != null) {
    this.containerId = containerId
  }

  if (heartbeat != null) {
    this.heartbeat = heartbeat
  }

  if (hostnameVerificationAlgorithm != null) {
    this.hostnameVerificationAlgorithm = hostnameVerificationAlgorithm
  }

  if (idleTimeout != null) {
    this.idleTimeout = idleTimeout
  }

  if (localAddress != null) {
    this.localAddress = localAddress
  }

  if (logActivity != null) {
    this.logActivity = logActivity
  }

  if (metricsName != null) {
    this.metricsName = metricsName
  }

  if (proxyOptions != null) {
    this.proxyOptions = proxyOptions
  }

  if (receiveBufferSize != null) {
    this.receiveBufferSize = receiveBufferSize
  }

  if (reconnectAttempts != null) {
    this.reconnectAttempts = reconnectAttempts
  }

  if (reconnectInterval != null) {
    this.reconnectInterval = reconnectInterval
  }

  if (replyHandlingSupport != null) {
    this.isReplyHandlingSupport = replyHandlingSupport
  }

  if (reuseAddress != null) {
    this.isReuseAddress = reuseAddress
  }

  if (sendBufferSize != null) {
    this.sendBufferSize = sendBufferSize
  }

  if (soLinger != null) {
    this.soLinger = soLinger
  }

  if (ssl != null) {
    this.isSsl = ssl
  }

  if (tcpKeepAlive != null) {
    this.isTcpKeepAlive = tcpKeepAlive
  }

  if (tcpNoDelay != null) {
    this.isTcpNoDelay = tcpNoDelay
  }

  if (trafficClass != null) {
    this.trafficClass = trafficClass
  }

  if (trustAll != null) {
    this.isTrustAll = trustAll
  }

  if (useAlpn != null) {
    this.isUseAlpn = useAlpn
  }

  if (usePooledBuffers != null) {
    this.isUsePooledBuffers = usePooledBuffers
  }

  if (vhost != null) {
    this.vhost = vhost
  }

}

