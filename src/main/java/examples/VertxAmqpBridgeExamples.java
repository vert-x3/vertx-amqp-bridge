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
package examples;

import io.vertx.amqpbridge.AmqpBridge;
import io.vertx.amqpbridge.AmqpBridgeOptions;
import io.vertx.amqpbridge.AmqpConstants;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.PfxOptions;
import io.vertx.docgen.Source;

@Source
public class VertxAmqpBridgeExamples {

  /*
   * Basic example of creating a producer and sending a message.
   */
  public void example1(Vertx vertx) {
    AmqpBridge bridge = AmqpBridge.create(vertx);
    // Start the bridge, then use the event loop thread to process things thereafter.
    bridge.start("localhost", 5672, res -> {
      // Set up a producer using the bridge, send a message with it.
      MessageProducer<JsonObject> producer = bridge.createProducer("myAmqpAddress");

      JsonObject amqpMsgPayload = new JsonObject();
      amqpMsgPayload.put("body", "myStringContent");

      producer.send(amqpMsgPayload);
    });
  }

  /*
   * Basic example of creating a consumer, registering a handler, and printing out received message.
   */
  public void example2(Vertx vertx) {
    AmqpBridge bridge = AmqpBridge.create(vertx);
    // Start the bridge, then use the event loop thread to process things thereafter.
    bridge.start("localhost", 5672, res -> {
      // Set up a consumer using the bridge, register a handler for it.
      MessageConsumer<JsonObject> consumer = bridge.createConsumer("myAmqpAddress");
      consumer.handler(vertxMsg -> {
        JsonObject amqpMsgPayload = vertxMsg.body();
        Object amqpBody = amqpMsgPayload.getValue("body");

        System.out.println("Received a message with body: " + amqpBody);
      });
    });
  }

  /*
   * Basic example of sending a message with application properties.
   */
  public void example3(MessageProducer<JsonObject> producer) {
    JsonObject applicationProperties = new JsonObject();
    applicationProperties.put("name", "value");

    JsonObject amqpMsgPayload = new JsonObject();
    amqpMsgPayload.put("application_properties", applicationProperties);

    producer.send(amqpMsgPayload);
  }

  /*
   * Basic example of receiving a message with application properties.
   */
  @SuppressWarnings("unused")
  public void example4(JsonObject amqpMsgPayload) {
    // Check the application properties section was present before use, it may not be
    JsonObject appProps = amqpMsgPayload.getJsonObject("application_properties");
    if(appProps != null) {
      Object propValue = appProps.getValue("propertyName");
    }
  }

  /*
   * Basic example of connecting the bridge to a server using SSL with a PKCS12 based trust store.
   */
  public void example5(Vertx vertx) {
    AmqpBridgeOptions bridgeOptions = new AmqpBridgeOptions();
    bridgeOptions.setSsl(true);

    PfxOptions trustOptions = new PfxOptions().setPath("path/to/pkcs12.truststore").setPassword("password");
    bridgeOptions.setPfxTrustOptions(trustOptions);

    AmqpBridge bridge = AmqpBridge.create(vertx, bridgeOptions);
    bridge.start("localhost", 5672, "username", "password", res -> {
      // ..do things with the bridge..
    });
  }

  /*
   * Basic example of connecting the bridge to a server using SSL Client Certificate Authentication with
   * PKCS12 based key and trust stores.
   */
  public void example6(Vertx vertx) {
    AmqpBridgeOptions bridgeOptions = new AmqpBridgeOptions();
    bridgeOptions.setSsl(true);

    PfxOptions trustOptions = new PfxOptions().setPath("path/to/pkcs12.truststore").setPassword("password");
    bridgeOptions.setPfxTrustOptions(trustOptions);

    PfxOptions keyCertOptions = new PfxOptions().setPath("path/to/pkcs12.keystore").setPassword("password");
    bridgeOptions.setPfxKeyCertOptions(keyCertOptions);

    AmqpBridge bridge = AmqpBridge.create(vertx, bridgeOptions);
    bridge.start("localhost", 5672, res -> {
      // ..do things with the bridge..
    });
  }

  /*
   * Basic example of sending a message with a reply handler.
   */
  @SuppressWarnings("unused")
  public void example7(MessageProducer<JsonObject> producer) {
    JsonObject amqpMsgPayload = new JsonObject();
    amqpMsgPayload.put("body", "myRequest");

    producer.<JsonObject> send(amqpMsgPayload, res -> {
      JsonObject amqpReplyMessagePayload = res.result().body();
      // ...do something with reply message...
    });
  }

  /*
   * Basic example of sending a reply to received message.
   */
  public void example8(MessageConsumer<JsonObject> consumer) {
    consumer.handler(msg -> {
      // ...do something with received message...then reply...
      String replyAddress = msg.replyAddress();
      if(replyAddress != null) {
        JsonObject amqpReplyMessagePayload = new JsonObject();
        amqpReplyMessagePayload.put("body", "myResponse");

        msg.reply(amqpReplyMessagePayload);
      }
    });
  }
}
