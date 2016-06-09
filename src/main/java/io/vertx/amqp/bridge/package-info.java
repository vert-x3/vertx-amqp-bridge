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

/**
 * = Vert.x AMQP Bridge
 *
 * This component provides AMQP 1.0 producer and consumer support via a bridging layer implementing the Vert.x event bus
 * MessageProducer and MessageConsumer APIs over the top of link:https://github.com/vert-x3/vertx-proton/[vertx-proton].
 *
 * WARNING: this module has the tech preview status, this means the API can change between versions.
 *
 * == Using Vert.x AMQP Bridge
 *
 * To use Vert.x AMQP Bridge, add the following dependency to the _dependencies_ section of your build descriptor:
 *
 * * Maven (in your `pom.xml`):
 *
 * [source,xml,subs="+attributes"]
 * ----
 * <dependency>
 *   <groupId>${maven.groupId}</groupId>
 *   <artifactId>${maven.artifactId}</artifactId>
 *   <version>${maven.version}</version>
 * </dependency>
 * ----
 *
 * * Gradle (in your `build.gradle` file):
 *
 * [source,groovy,subs="+attributes"]
 * ----
 * compile ${maven.groupId}:${maven.artifactId}:${maven.version}
 * ----
 *
 * === Sending a Message
 *
 * Here is a simple example of creating a {@link io.vertx.core.eventbus.MessageProducer} and sending a message with it.
 * First, an {@link io.vertx.amqp.bridge.AmqpBridge} is created and started to establish the underlying AMQP connection,
 * then when this is complete the producer is created and a message sent using it. You can also optionally supply
 * {@link io.vertx.amqp.bridge.AmqpBridgeOptions} when creating the bridge in order to configure various options, such
 * as SSL connections.
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxAmqpBridgeExamples#example1}
 * ----
 *
 * === Receiving a Message
 *
 * Here is a simple example of creating a {@link io.vertx.core.eventbus.MessageConsumer} and registering a handler with it.
 * First, an {@link io.vertx.amqp.bridge.AmqpBridge} is created and started to establish the underlying AMQP connection,
 * then when this is complete the consumer is created and a handler registered that prints the body of incoming AMQP
 * messages.
 *
 * [source,$lang]
 * ----
 * {@link examples.VertxAmqpBridgeExamples#example2}
 * ----
 */
@Document(fileName = "index.adoc")
@ModuleGen(name = "vertx-amqp-bridge", groupPackage = "io.vertx")
package io.vertx.amqp.bridge;

import io.vertx.codegen.annotations.ModuleGen;
import io.vertx.docgen.Document;
