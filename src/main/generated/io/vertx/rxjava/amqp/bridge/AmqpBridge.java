/*
 * Copyright 2014 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.vertx.rxjava.amqp.bridge;

import java.util.Map;
import rx.Observable;
import io.vertx.amqp.bridge.AmqpBridgeOptions;
import io.vertx.rxjava.core.Vertx;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.rxjava.core.eventbus.MessageConsumer;
import io.vertx.rxjava.core.eventbus.MessageProducer;

/**
 * Vert.x AMQP Bridge. Facilitates sending and receiving AMQP 1.0 messages.
 *
 * <p/>
 * NOTE: This class has been automatically generated from the {@link io.vertx.amqp.bridge.AmqpBridge original} non RX-ified interface using Vert.x codegen.
 */

public class AmqpBridge {

  final io.vertx.amqp.bridge.AmqpBridge delegate;

  public AmqpBridge(io.vertx.amqp.bridge.AmqpBridge delegate) {
    this.delegate = delegate;
  }

  public Object getDelegate() {
    return delegate;
  }

  /**
   * Creates a Bridge.
   * @param vertx the vertx instance to use
   * @return the (not-yet-started) bridge.
   */
  public static AmqpBridge create(Vertx vertx) { 
    AmqpBridge ret = AmqpBridge.newInstance(io.vertx.amqp.bridge.AmqpBridge.create((io.vertx.core.Vertx)vertx.getDelegate()));
    return ret;
  }

  /**
   * Creates a Bridge with the given options.
   * @param vertx the vertx instance to use
   * @param options the options
   * @return the (not-yet-started) bridge.
   */
  public static AmqpBridge create(Vertx vertx, AmqpBridgeOptions options) { 
    AmqpBridge ret = AmqpBridge.newInstance(io.vertx.amqp.bridge.AmqpBridge.create((io.vertx.core.Vertx)vertx.getDelegate(), options));
    return ret;
  }

  /**
   * Starts the bridge, establishing the underlying connection.
   * @param hostname the host name to connect to
   * @param port the port to connect to
   * @param username the username
   * @param password the password
   * @param resultHandler the result handler
   */
  public void start(String hostname, int port, String username, String password, Handler<AsyncResult<AmqpBridge>> resultHandler) { 
    delegate.start(hostname, port, username, password, new Handler<AsyncResult<io.vertx.amqp.bridge.AmqpBridge>>() {
      public void handle(AsyncResult<io.vertx.amqp.bridge.AmqpBridge> ar) {
        if (ar.succeeded()) {
          resultHandler.handle(io.vertx.core.Future.succeededFuture(AmqpBridge.newInstance(ar.result())));
        } else {
          resultHandler.handle(io.vertx.core.Future.failedFuture(ar.cause()));
        }
      }
    });
  }

  /**
   * Starts the bridge, establishing the underlying connection.
   * @param hostname the host name to connect to
   * @param port the port to connect to
   * @param username the username
   * @param password the password
   * @return 
   */
  public Observable<AmqpBridge> startObservable(String hostname, int port, String username, String password) { 
    io.vertx.rx.java.ObservableFuture<AmqpBridge> resultHandler = io.vertx.rx.java.RxHelper.observableFuture();
    start(hostname, port, username, password, resultHandler.toHandler());
    return resultHandler;
  }

  /**
   * Starts the bridge, establishing the underlying connection.
   * @param hostname the host name to connect to
   * @param port the port to connect to
   * @param resultHandler the result handler
   */
  public void start(String hostname, int port, Handler<AsyncResult<AmqpBridge>> resultHandler) { 
    delegate.start(hostname, port, new Handler<AsyncResult<io.vertx.amqp.bridge.AmqpBridge>>() {
      public void handle(AsyncResult<io.vertx.amqp.bridge.AmqpBridge> ar) {
        if (ar.succeeded()) {
          resultHandler.handle(io.vertx.core.Future.succeededFuture(AmqpBridge.newInstance(ar.result())));
        } else {
          resultHandler.handle(io.vertx.core.Future.failedFuture(ar.cause()));
        }
      }
    });
  }

  /**
   * Starts the bridge, establishing the underlying connection.
   * @param hostname the host name to connect to
   * @param port the port to connect to
   * @return 
   */
  public Observable<AmqpBridge> startObservable(String hostname, int port) { 
    io.vertx.rx.java.ObservableFuture<AmqpBridge> resultHandler = io.vertx.rx.java.RxHelper.observableFuture();
    start(hostname, port, resultHandler.toHandler());
    return resultHandler;
  }

  /**
   * Creates a consumer on the given AMQP address.
   *
   * This method MUST be called from the bridge Context thread, as used in the result handler callback from the start
   * methods. The bridge MUST be successfully started before the method is called.
   * @param amqpAddress the address to consume from
   * @return the consumer
   */
  public <T> MessageConsumer<T> createConsumer(String amqpAddress) { 
    MessageConsumer<T> ret = MessageConsumer.newInstance(delegate.createConsumer(amqpAddress));
    return ret;
  }

  /**
   * Creates a producer to the given AMQP address.
   *
   * This method MUST be called from the bridge Context thread, as used in the result handler callback from the start
   * methods. The bridge MUST be successfully started before the method is called.
   * @param amqpAddress the address to produce to
   * @return the producer
   */
  public <T> MessageProducer<T> createProducer(String amqpAddress) { 
    MessageProducer<T> ret = MessageProducer.newInstance(delegate.createProducer(amqpAddress));
    return ret;
  }

  /**
   * Shuts the bridge down, closing the underlying connection.
   * @param resultHandler the result handler
   */
  public void close(Handler<AsyncResult<Void>> resultHandler) { 
    delegate.close(new Handler<AsyncResult<java.lang.Void>>() {
      public void handle(AsyncResult<java.lang.Void> ar) {
        if (ar.succeeded()) {
          resultHandler.handle(io.vertx.core.Future.succeededFuture(ar.result()));
        } else {
          resultHandler.handle(io.vertx.core.Future.failedFuture(ar.cause()));
        }
      }
    });
  }

  /**
   * Shuts the bridge down, closing the underlying connection.
   * @return 
   */
  public Observable<Void> closeObservable() { 
    io.vertx.rx.java.ObservableFuture<Void> resultHandler = io.vertx.rx.java.RxHelper.observableFuture();
    close(resultHandler.toHandler());
    return resultHandler;
  }


  public static AmqpBridge newInstance(io.vertx.amqp.bridge.AmqpBridge arg) {
    return arg != null ? new AmqpBridge(arg) : null;
  }
}
