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

import java.util.concurrent.ExecutionException;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonServer;
import io.vertx.proton.ProtonServerOptions;

public class MockServer {
  private ProtonServer server;

  // Toggle to (re)use a fixed port, e.g for capture.
  private int bindPort = 0;
  private boolean reuseAddress = false;

  public MockServer(Vertx vertx, Handler<ProtonConnection> connectionHandler) throws ExecutionException, InterruptedException {
    this(vertx, connectionHandler, null);
  }

  public MockServer(Vertx vertx, Handler<ProtonConnection> connectionHandler, ProtonServerOptions protonServerOptions) throws ExecutionException, InterruptedException {
    if(protonServerOptions == null) {
      protonServerOptions = new ProtonServerOptions();
    }

    protonServerOptions.setReuseAddress(reuseAddress);
    server = ProtonServer.create(vertx, protonServerOptions);
    server.connectHandler(connectionHandler);

    FutureHandler<ProtonServer, AsyncResult<ProtonServer>> handler = FutureHandler.asyncResult();
    server.listen(bindPort, handler);
    handler.get();
  }

  public int actualPort() {
    return server.actualPort();
  }

  public void close() {
    server.close();
  }

  ProtonServer getProtonServer() {
    return server;
  }
}
