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

        @active_request_socket=@request_socket_a

        loop do
          @now = Time.now
          run_hook(:loop)
          request = pull_request
          case request
          when :noop
            switch_active_request_socket
          when Array
            process_request(request)
          else NilClass
            active_a = @active_request_socket == @request_socket_a
            close_sockets
            open_sockets
            @active_request_socket = active_a ? @request_socket_b : @request_socket_a
          end
        end
      ensure
        rz_cleanup
        run_hook :after_run
      end
   
    private

      def switch_active_request_socket
        p :switch
        @active_request_socket = 
          case @active_request_socket
          when @request_socket_a then @request_socket_b
          when @request_socket_b then @request_socket_a
          else 
            raise 'should not happen'
          end
      end

      def close_sockets
        @request_socket_a = @request_socket_b = @response_socket = nil
        rz_cleanup
      end

      def open_sockets
        @request_socket_a = rz_socket(ZMQ::DEALER)
        @request_socket_a.connect(request_address_a)

        @response_socket = rz_socket(ZMQ::DEALER)
        @response_socket.connect(response_address)

        if @rz_identity
          @request_socket_a.setsockopt(ZMQ::IDENTITY,"#{@rz_identity}.req.a") 
          @request_socket_b.setsockopt(ZMQ::IDENTITY,"#{@rz_identity}.req.b") 
          @response_socket. setsockopt(ZMQ::IDENTITY,"#{@rz_identity}.res") 
        end
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
        message = JSON.dump(response)
        rz_send(@response_socket,DELIM + client_address + DELIM + [message])
      end

      def pull_request
        rz_send(@active_request_socket,DELIM)

        ready = ZMQ.select([@active_request_socket],nil,nil,1.5)

        return unless ready

        client_address,job_body =  rz_split(rz_recv(@active_request_socket))

        raise if job_body.length != 1

        job_body = job_body.first

        if job_body == 'NOOP'
          :noop
        else
          [client_address,job_body]
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
