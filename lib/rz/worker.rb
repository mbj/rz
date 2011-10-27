require 'json'
require 'rz/context'

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
        @response_address = options.fetch(:response_address) { raise ArgumentError,'missing :response_address in options' }
        @request_address_a = options.fetch(:request_address_a) { raise ArgumentError,'missing :request_address_a in options' }
        @request_address_b = options.fetch(:request_address_b) { raise ArgumentError,'missing :request_address_b in options' }
        @identity     = options.fetch(:identity,nil)
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
        block = self.class.registry[name]
   
        unless block
          raise ArgumentError,"job #{name.inspect} is not registred"
        end
   
        info { "executing: #{name}, #{arguments.inspect}" }
   
        begin
          case block
          when Proc
            block.call *arguments
          when true
            send(name,*arguments)
          end
        rescue Exception => exception
          # Reraise interrupts unmodified to make sure we do not refuse to stopwe do not refuse to stop
          if exception.kind_of? Interrupt
            raise
          else
            raise JobExecutionError.new(exception,job)
          end
        end
      end
   
      def process_job(client_address,job)
        raise unless job.length == 1
        job = JSON.load(job.first)
        begin
          result = dispatch_job(job)
          result = JSON.dump(:result => result)
          zmq_send(response_socket,DELIM + client_address + DELIM + [result])
        rescue JobExecutionError => job_execution_exception
          exception = job_execution_exception.original_exception
          error { "exception captured while dispatching: #{exception.class.name} #{exception.message}" }
          exception.backtrace.each do |trace|
            error { trace }
          end
        end
      end
   
      def pull_job
        zmq_send active_request_socket,DELIM + HALLO
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
   
      def run_hook(name)
        self.class.hooks[name].each do |hook|
          case hook
          when Proc
            instance_exec &hook
          when Symbol
            send hook
          else
            raise
          end
        end
      end
    end

    module ClassMethods
      def registry
        @registry ||= {}
      end

      def hook(name,method_name=nil,&block)
        raise ArgumentError,"provide method name or block not both" if method_name and block
        raise ArgumentError,"provide method name or block" unless method_name or block
        hooks_for(name) << (method_name || block)
      end
    
      def hooks
        @hooks ||= {}
      end
    
      def hooks_for(name)
        hooks[name] ||= []
      end

    private

      def register(name,&block)
        name = name.to_s
        if registry.key? name
          raise ArgumentError,"#{name.inspect} is already registred"
        end
        registry[name] = block || true
        self
      end
    end

    def self.included(base)
      base.send :extend,ClassMethods
      base.send :include,InstanceMethods
      base.send :register,:echo do |*arguments|
        arguments
      end
    end
  end
end
