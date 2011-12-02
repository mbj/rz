require 'json'
require 'rz/context'
require 'rz/hooking'

module RZ
  class JobExecutionError < RuntimeError
    attr_reader :original_exception
    attr_reader :job

    def initialize(original_exception,job)
      @original_exception,@job = original_exception,job
      super "job #{job.fetch('name').inspect} failed with #{original_exception.class}"
    end
  end


  module Worker

    module InstanceMethods
      include Context
   
      attr_reader :response_address,:request_address_a,:request_address_b,:identity
   
      def run
        run_hook :before_run
        request_socket_a
        request_socket_b
        self.active_request_socket=request_socket_a
        loop do
          response = pull_job
          case response
          when :noop
            switch_active_socket
            next
          when Array
            client_address,job = response
            process_job(client_address,job)
          else
            active = active_request_socket == request_socket_a ? :a : :b
            zmq_named_socket_close :request_socket_a
            zmq_named_socket_close :request_socket_b
            self.active_request_socket= active == :a ? request_socket_b : request_socket_a
          end
        end
      ensure
        zmq_cleanup
        run_hook :after_run
      end
   
    private
   
      def initialize_worker(options)
        @response_address  = options.fetch(:response_address) { raise ArgumentError,'missing :response_address in options' }
        @request_address_a = options.fetch(:request_address_a) { raise ArgumentError,'missing :request_address_a in options' }
        @request_address_b = options.fetch(:request_address_b) { raise ArgumentError,'missing :request_address_b in options' }
        @identity          = options.fetch(:identity,nil)
      end
   
      def active_request_socket
        @active_request_socket || raise("no req socket is currently active")
      end
   
      def switch_active_socket
        self.active_request_socket = case active_request_socket
        when request_socket_a then request_socket_b
        when request_socket_b then request_socket_a
        else 
          raise
        end
      end
   
      def active_request_socket=(socket)
        @active_request_socket=socket
        debug { "switched to socket: #{zmq_identity(socket)}" }
      end
   
      def dispatch_job(job)
        name      = job.fetch('name')      { raise ArgumentError,'missing "name" in options'      }
        arguments = job.fetch('arguments') { raise ArgumentError,'missing "arguments" in options' }
        block = self.class.requests[name]
   
        unless block
          raise ArgumentError,"job #{name.inspect} is not registred"
        end
   
        debug { "executing: #{name}, #{arguments.inspect}" }
   
        begin
          case block
          when Proc
            block.call *arguments
          when true
            send(name,*arguments)
          end
        rescue Interrupt, SignalException
          raise
        rescue Exception => exception
          raise JobExecutionError.new(exception,job)
        end
      end
   
      def process_job(client_address,job)
        raise unless job.length == 1
        job = JSON.load(job.first)
        result = begin
          result = dispatch_job(job)
	        job.delete 'arguments'
	        job.merge(:status => :success,:result => result,:worker_identity => identity)
        rescue JobExecutionError => job_execution_exception
          exception = job_execution_exception.original_exception
          error { "exception captured while dispatching: #{exception.class.name} #{exception.message}" }
          exception.backtrace.each do |trace|
            error { trace }
          end
          job.merge(:status => :error,:error => { :type => exception.class.name, :message => exception.message,:backtrace => exception.backtrace })
        end
        zmq_send(response_socket,DELIM + client_address + DELIM + [JSON.dump(result)])
      end
   
      def pull_job
        zmq_send active_request_socket,DELIM
        ready = ZMQ.select([active_request_socket],nil,nil,10)
        return unless ready
        client_address,job_body =  zmq_split(zmq_recv(active_request_socket))
        if job_body.first == 'NOOP'
          :noop
        else
          [client_address,job_body]
        end
      end
   
      def request_socket_a
        zmq_named_socket(:request_socket_a,ZMQ::DEALER) do |socket|
          socket.setsockopt(ZMQ::IDENTITY,"#{identity}.req.a") if identity
          socket.setsockopt(ZMQ::LINGER,0)
          socket.connect request_address_a
        end
      end
   
      def request_socket_b
        zmq_named_socket(:request_socket_b,ZMQ::DEALER) do |socket|
          socket.setsockopt(ZMQ::IDENTITY,"#{identity}.req.b") if identity
          socket.setsockopt(ZMQ::LINGER,0)
          socket.connect request_address_b
        end
      end
   
      def response_socket
        zmq_named_socket(:response_socket,ZMQ::DEALER) do |socket|
          socket.setsockopt(ZMQ::IDENTITY,"#{identity}.res") if identity
          socket.setsockopt(ZMQ::LINGER,0)
          socket.connect response_address
        end
      end
    end

    module ClassMethods
      def requests
        @requests ||= {}
      end

    private

      def register(name,&block)
        name = name.to_s
        if requests.key? name
          raise ArgumentError,"#{name.inspect} is already registred"
        end
        requests[name] = block || true
        self
      end
    end

    def self.included(base)
      base.send :extend,ClassMethods
      base.send :include,InstanceMethods
      base.send :include,Hooking
      base.send :register,:echo do |value|
        value
      end
    end
  end
end
