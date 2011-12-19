require 'json'
require 'rz/context'
require 'rz/hooking'
require 'rz/job_executor'

module RZ
  module PullWorker
    module InstanceMethods
      include RZ::Context
      attr_reader :response_address, :request_address_a, :request_address_b
   
      def run_worker
        setup_sockets
        run_hook(:before_run)
        process_loop
      ensure
        rz_cleanup
        run_hook(:after_run)
      end
   
    protected
    
      def process_loop
        loop do
          run_hook(:loop)
          process_loop_tick
          run_hook(:loop_end)
        end
      end

      def process_loop_tick
        request = pull_request
        case request
        when :noop
          @request_sockets.reverse!
        when Array
          process_request(request)
        else NilClass
          reconnect
        end
      end

      def reconnect
        close_sockets
        setup_sockets
      end

      def active_request_socket
        @request_sockets.first
      end

      def close_sockets
        @request_socket_a = @request_socket_b = @response_socket = nil
        rz_cleanup
      end

      def setup_sockets
        setup_request_socket_a
        setup_request_socket_b
        setup_response_socket

        @request_sockets = [@request_socket_a,@request_socket_b]
      end

      def setup_request_socket_a
        @request_socket_a = rz_socket(ZMQ::DEALER)
        if @rz_identity
          @request_socket_a.setsockopt(ZMQ::IDENTITY,"#{@rz_identity}.req.a") 
        end
        @request_socket_a.connect(request_address_a)
      end

      def setup_request_socket_b
        @request_socket_b = rz_socket(ZMQ::DEALER)
        if @rz_identity
          @request_socket_b.setsockopt(ZMQ::IDENTITY,"#{@rz_identity}.req.b") 
        end
        @request_socket_b.connect(request_address_b)
      end

      def setup_response_socket
        @response_socket = rz_socket(ZMQ::DEALER)
        if @rz_identity
          @response_socket.setsockopt(ZMQ::IDENTITY,"#{@rz_identity}.res") 
        end
        @response_socket.connect(response_address)
      end

      def fetch_option(options,key)
        options.fetch(key) do 
          raise ArgumentError,"missing #{key.inspect} in options"
        end
      end

      def initialize_worker(options)
        @response_address  = fetch_option(options,:response_address)
        @request_address_a = fetch_option(options,:request_address_a) 
        @request_address_b = fetch_option(options,:request_address_b)
        @rz_identity = options.fetch(:rz_identity,nil)
      end

      def process_request(request)
        client_address,job_body = request
        response = process_job_body(job_body)
        send_response(client_address,response)
      end

      def send_response(client_address,response)
        rz_send(@response_socket,[client_address,JSON.dump(response)])
      end

      def pull_request
        active_request_socket = self.active_request_socket

        rz_send(active_request_socket,DELIM)

        message = rz_read_timeout(active_request_socket,1.5) || return

        case message.length
        when 1
          raise unless message.first.empty?
          :noop
        when 2
          message
        else
          raise 'should not happen'
        end
      end
    end

    def self.included(base)
      base.send :include,JobExecutor
      base.send :include,InstanceMethods
      base.send :include,Hooking
      base.send :register,:echo do |value|
        value
      end
    end
  end
end
