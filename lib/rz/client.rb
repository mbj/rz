require 'json'
require 'rz/context'

module RZ
  module Client
    include Context

    # Send a request to service
    #
    # @param [String|Symbol] name the name of service
    # @param [Hash] job the job definition
    #
    # @return self
    def request(name,job)
      rz_send(service_socket(name),[JSON.dump(job)])
      self
    end

    # Receive a response form service
    #
    # @param [String|Symbol] the name of service
    # @param [Hash] options the receive options
    #
    # @return [Hash,nil]
    #   the job result or nil in case of timeout
    #
    def receive(name,options={})
      timeout = options.fetch(:timeout,1)

      socket = service_socket(name)

      ready = ZMQ.select([socket],nil,nil,timeout)
      return unless ready

      message = rz_recv(socket)

      raise unless message.length == 1

      JSON.load(message.first)
    end

  private

    # Initialize client
    #
    # @param [Hash] opts the options to initialize client with
    # @option opts [Hash] :services the service addresses
    #
    # @return self
    #
    # @example:
    #   Worker.new(
    #     :service_a => "tcp://127.0.0.1:4000",
    #     :service_b => "tcp://127.0.0.1:4001",
    #     :service_c => "tcp://127.0.0.1:4002",
    #   )
    #
    def initialize_client(opts)
      @services = opts.fetch(:services) do 
        raise ArgumentError,'missing :services in options'
      end
      @rz_identity = opts.fetch(:rz_identity,nil)

      self
    end

    # Returns socket for service
    #
    # @param [String|Symbol] service name
    #
    # @return [ZMQ::Socket] 
    #   the service socket
    #
    def service_socket(name)
      service_sockets[name] ||= 
        begin
          socket = rz_socket(ZMQ::DEALER)
          socket.setsockopt(ZMQ::IDENTITY,@rz_identity) if @rz_identity
          socket.connect(service_address(name))
          socket
        end
    end

    # Return the service sockets
    #
    # @return [Hash] the service sockets
    #
    def service_sockets
      @service_sockets ||= {}
    end

    # Returns address of service
    #
    # @param [String|Symbol] the name of service
    # @raise [ArgumentException] raised when service does not exist
    #
    # @return [String] address of service
    #
    def service_address(name)
      @services.fetch(name) do 
        raise ArgumentError,"no address for service #{name.inspect}"
      end
    end
  end
end
