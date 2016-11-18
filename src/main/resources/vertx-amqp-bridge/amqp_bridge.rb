require 'vertx/vertx'
require 'vertx/message_consumer'
require 'vertx/message_producer'
require 'vertx/util/utils.rb'
# Generated from io.vertx.amqpbridge.AmqpBridge
module VertxAmqpBridge
  #  Vert.x AMQP Bridge. Facilitates sending and receiving AMQP 1.0 messages.
  class AmqpBridge
    # @private
    # @param j_del [::VertxAmqpBridge::AmqpBridge] the java delegate
    def initialize(j_del)
      @j_del = j_del
    end
    # @private
    # @return [::VertxAmqpBridge::AmqpBridge] the underlying java delegate
    def j_del
      @j_del
    end
    @@j_api_type = Object.new
    def @@j_api_type.accept?(obj)
      obj.class == AmqpBridge
    end
    def @@j_api_type.wrap(obj)
      AmqpBridge.new(obj)
    end
    def @@j_api_type.unwrap(obj)
      obj.j_del
    end
    def self.j_api_type
      @@j_api_type
    end
    def self.j_class
      Java::IoVertxAmqpbridge::AmqpBridge.java_class
    end
    #  Creates a Bridge with the given options.
    # @param [::Vertx::Vertx] vertx the vertx instance to use
    # @param [Hash] options the options
    # @return [::VertxAmqpBridge::AmqpBridge] the (not-yet-started) bridge.
    def self.create(vertx=nil,options=nil)
      if vertx.class.method_defined?(:j_del) && !block_given? && options == nil
        return ::Vertx::Util::Utils.safe_create(Java::IoVertxAmqpbridge::AmqpBridge.java_method(:create, [Java::IoVertxCore::Vertx.java_class]).call(vertx.j_del),::VertxAmqpBridge::AmqpBridge)
      elsif vertx.class.method_defined?(:j_del) && options.class == Hash && !block_given?
        return ::Vertx::Util::Utils.safe_create(Java::IoVertxAmqpbridge::AmqpBridge.java_method(:create, [Java::IoVertxCore::Vertx.java_class,Java::IoVertxAmqpbridge::AmqpBridgeOptions.java_class]).call(vertx.j_del,Java::IoVertxAmqpbridge::AmqpBridgeOptions.new(::Vertx::Util::Utils.to_json_object(options))),::VertxAmqpBridge::AmqpBridge)
      end
      raise ArgumentError, "Invalid arguments when calling create(#{vertx},#{options})"
    end
    #  Starts the bridge, establishing the underlying connection.
    # @param [String] hostname the host name to connect to
    # @param [Fixnum] port the port to connect to
    # @param [String] username the username
    # @param [String] password the password
    # @yield the result handler
    # @return [void]
    def start(hostname=nil,port=nil,username=nil,password=nil)
      if hostname.class == String && port.class == Fixnum && block_given? && username == nil && password == nil
        return @j_del.java_method(:start, [Java::java.lang.String.java_class,Java::int.java_class,Java::IoVertxCore::Handler.java_class]).call(hostname,port,(Proc.new { |ar| yield(ar.failed ? ar.cause : nil, ar.succeeded ? ::Vertx::Util::Utils.safe_create(ar.result,::VertxAmqpBridge::AmqpBridge) : nil) }))
      elsif hostname.class == String && port.class == Fixnum && username.class == String && password.class == String && block_given?
        return @j_del.java_method(:start, [Java::java.lang.String.java_class,Java::int.java_class,Java::java.lang.String.java_class,Java::java.lang.String.java_class,Java::IoVertxCore::Handler.java_class]).call(hostname,port,username,password,(Proc.new { |ar| yield(ar.failed ? ar.cause : nil, ar.succeeded ? ::Vertx::Util::Utils.safe_create(ar.result,::VertxAmqpBridge::AmqpBridge) : nil) }))
      end
      raise ArgumentError, "Invalid arguments when calling start(#{hostname},#{port},#{username},#{password})"
    end
    #  Creates a consumer on the given AMQP address.
    # 
    #  This method MUST be called from the bridge Context thread, as used in the result handler callback from the start
    #  methods. The bridge MUST be successfully started before the method is called.
    # @param [String] amqpAddress the address to consume from
    # @return [::Vertx::MessageConsumer] the consumer
    def create_consumer(amqpAddress=nil)
      if amqpAddress.class == String && !block_given?
        return ::Vertx::Util::Utils.safe_create(@j_del.java_method(:createConsumer, [Java::java.lang.String.java_class]).call(amqpAddress),::Vertx::MessageConsumer, nil)
      end
      raise ArgumentError, "Invalid arguments when calling create_consumer(#{amqpAddress})"
    end
    #  Creates a producer to the given AMQP address.
    # 
    #  This method MUST be called from the bridge Context thread, as used in the result handler callback from the start
    #  methods. The bridge MUST be successfully started before the method is called.
    # @param [String] amqpAddress the address to produce to
    # @return [::Vertx::MessageProducer] the producer
    def create_producer(amqpAddress=nil)
      if amqpAddress.class == String && !block_given?
        return ::Vertx::Util::Utils.safe_create(@j_del.java_method(:createProducer, [Java::java.lang.String.java_class]).call(amqpAddress),::Vertx::MessageProducer, nil)
      end
      raise ArgumentError, "Invalid arguments when calling create_producer(#{amqpAddress})"
    end
    #  Shuts the bridge down, closing the underlying connection.
    # @yield the result handler
    # @return [void]
    def close
      if block_given?
        return @j_del.java_method(:close, [Java::IoVertxCore::Handler.java_class]).call((Proc.new { |ar| yield(ar.failed ? ar.cause : nil) }))
      end
      raise ArgumentError, "Invalid arguments when calling close()"
    end
  end
end
