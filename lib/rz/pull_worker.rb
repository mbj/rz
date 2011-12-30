require 'json'
require 'rz/context'
require 'rz/hooking'
require 'rz/job_executor'

module RZ
  module PullWorker
    module InstanceMethods
      include RZ::Context

      attr_reader :response_address, :request_address
   
      def run_worker
        setup_sockets
        run_hook(:before_run)
        process_loop
      rescue Interrupt, SignalException
        run_hook(:interrupted)
      ensure
        rz_cleanup
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

      def close_sockets
        rz_socket_close(@request_socket)
        rz_socket_close(@response_socket)
        @request_socket = @response_socket = nil
        rz_cleanup
      end

      def setup_sockets
        @ssc = @sequence = 0

        @request_socket = rz_socket(ZMQ::DEALER)

        if @rz_identity
          @request_socket.setsockopt(ZMQ::IDENTITY,"#{@rz_identity}.req") 
        end

        @request_socket.connect(request_address)

        @response_socket = rz_socket(ZMQ::DEALER)

        if @rz_identity
          @response_socket.setsockopt(ZMQ::IDENTITY,"#{@rz_identity}.res") 
        end

        @response_socket.connect(response_address)

        self
      end

      def initialize_worker(options)
        @response_address  = 
          Helpers.fetch_option(options,:response_address,String)
        @request_address = 
          Helpers.fetch_option(options,:request_address,String) 
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
        rz_send(@request_socket,[Helpers.pack_int(@sequence)])

        message = rz_read_timeout(@request_socket,2) || return

        case message.length
        when 1
          sequence = Helpers.unpack_int(message.first)
          if sequence == @sequence
            @ssc += 1
            reconnect if @ssc > 1
          else
            rz_debug { "sequence: #{sequence} old: #{@sequence} "}
            @sequence = sequence
            @ssc = 0
          end
          :noop
        when 2
          message
        else
          raise 'should not happen'
        end
      end
    end

    def self.included(base)
      base.send(:include,JobExecutor)
      base.send(:include,InstanceMethods)
      base.send(:include,Hooking)
      base.send(:register,:echo) do |value|
        value
      end
    end
  end
end
