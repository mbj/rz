require 'json'
require 'rz/context'
require 'rz/hooking'
require 'rz/job_executor'

module RZ
  module PullWorker
    module InstanceMethods
      include RZ::Context
      attr_reader :response_address, :request_address_a, :request_address_b
   
      def run
        open_sockets
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
        open_sockets
      end

      def active_request_socket
        @request_sockets.first
      end

      def close_sockets
        @request_socket_a = @request_socket_b = @response_socket = nil
        rz_cleanup
      end

      def open_sockets
        %w(request_socket_a request_socket_b response_socket).each do |socket_name|
          socket = rz_socket(ZMQ::DEALER)
          socket.connect(self.send(socket_name.gsub('socket','address')))
          instance_variable_set(:"@#{socket_name}",socket)
        end

        @request_sockets = [@request_socket_a,@request_socket_b]

        setup_socket_identities if @rz_identity
      end

      def setup_socket_identities
        @request_socket_a.setsockopt(ZMQ::IDENTITY,"#{@rz_identity}.req.a") 
        @request_socket_b.setsockopt(ZMQ::IDENTITY,"#{@rz_identity}.req.b") 
        @response_socket. setsockopt(ZMQ::IDENTITY,"#{@rz_identity}.res") 
      end

      def initialize_worker(options)
        @response_address = options.fetch(:response_address) do 
          raise ArgumentError,'missing :response_address in options'
        end
        @request_address_a = options.fetch(:request_address_a) do 
          raise ArgumentError,'missing :request_address_a in options'
        end
        @request_address_b = options.fetch(:request_address_b) do 
          raise ArgumentError,'missing :request_address_b in options'
        end
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
