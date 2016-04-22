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
package io.vertx.amqp.bridge.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

public class AmqpMessageImpl implements Message<JsonObject> {

  private JsonObject body;
  private BridgeImpl bridge;
  private org.apache.qpid.proton.message.Message protonMessage;

  public AmqpMessageImpl(JsonObject body, BridgeImpl bridge, org.apache.qpid.proton.message.Message protonMessage) {
    // TODO: ensure non-null body?
    this.body = body;
    this.bridge = bridge;
    this.protonMessage = protonMessage;
  }

  @Override
  public String address() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  @Override
  public MultiMap headers() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  @Override
  public JsonObject body() {
    return body;
  }

  @Override
  public String replyAddress() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  private <R> void doReply(Object replyMessageBody, Handler<AsyncResult<Message<R>>> replyHandler) {
    bridge.sendReply(protonMessage, replyMessageBody, replyHandler);
  }

  @Override
  public void reply(Object replyMessageBody) {
    doReply(replyMessageBody, null);
  }

  @Override
  public <R> void reply(Object replyMessageBody, Handler<AsyncResult<Message<R>>> replyHandler) {
    doReply(replyMessageBody, replyHandler);
  }

  @Override
  public void reply(Object messageBody, DeliveryOptions options) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  @Override
  public <R> void reply(Object messageBody, DeliveryOptions options, Handler<AsyncResult<Message<R>>> replyHandler) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  @Override
  public void fail(int failureCode, String message) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }
}
