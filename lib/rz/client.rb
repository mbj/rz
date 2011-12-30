require 'json'
require 'rz/context'
require 'rz/errors'

module RZ
  module Client
    include Context

    class TimeoutError < RZ::Error
    end

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
    # @return [Hash]
    #   the job result or nil in case of timeout
    #
    def receive(name,options={})
      timeout = options.fetch(:timeout,1)

      message = rz_read_timeout(service_socket(name),timeout)

      unless message
        raise TimeoutError,"did not receive a message in #{timeout} seconds"
      end

      raise unless message.length == 1

      message = JSON.load(message.first)

      unless message.kind_of?(Hash)
        raise RZ::Error,"message loaded as #{message.class} exected Hash"
      end

      message
    end

    # Invoke an "relihable" rpc style request receive cycle
    # Pls make sure your calls are idempotent, since there is a small
    # chance your request is executed twice! Do not mix with streaming requests!
    #
    # @param [String|Symbol] the name of service
    # @param [Hash] options the rpc options
    def rpc(service_name,task_name,arguments,options={})
      max_retries = options.fetch(:max_retries,1)
      left_retries = max_retries

      loop do
        request_id = "#{@identity}-#{@counter+=1}"
        request(
          service_name,
          :name => task_name,
          :arguments => [*arguments],
          :request_id => request_id
        )
        begin
          return receive_rpc_response(service_name,request_id,options)
        rescue TimeoutError
          rz_warn { "rpc timeout, left retries #{left_retries}" }
          left_retries -= 1
          raise unless left_retries > 0
        end
      end
    rescue TimeoutError => exception
      raise TimeoutError,"#{exception.message} after #{max_retries} retries"
    end

    def receive_rpc_response(service_name,request_id,options)
      loop do
        message = receive_with_error_handling(service_name,options)
        unless message['request_id'] == request_id
          $stderr.puts message.inspect
          rz_warn { 'dropping duplicated or unexpected reply' }
          next
        end
        return message
      end
    end

    # Recive a response with error handling
    # @param [String|Symbol] then mae of service
    # @param [Hash] options the receive options
    #
    # @return [Hash]
    #   the job result
    def receive_with_error_handling(name,options={})
      message = receive(name,options)
      status = message['status']
      case status
      when 'success'
        message
      when 'error'
        raise_error_for_message(message)
      else
        $stderr.puts message.inspect
        raise RZ::Error,"message status #{status.inspect} is unkown"
      end
    end

  private

    class RemoteError < RZ::Error
      alias :local_backtrace :backtrace

      attr_reader :backtrace

      def initialize(message,backtrace)
        super(message)
        @backtrace = backtrace
      end
    end

    def raise_error_for_message(message)
      error = message['error']
      case error
      when Hash
        backtrace = error['backtrace']
        message = error['message']
        raise RemoteError.new(message,backtrace)
      else
        raise RZ::Error,'unable to construct exception' 
      end
    end

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
      @identity    = @rz_identity ||= "#{Socket.gethostname}-#{Process.pid}"
      @counter     = 0

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
