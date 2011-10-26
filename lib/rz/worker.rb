require 'json'
require 'rz/context'

module RZ
  module Worker
    include Context

    attr_reader :peer_res_address,:peer_req_address_a,:peer_req_address_b,:identity

    def run
      request_socket_b
      self.active_req_socket=request_socket_a
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
          active = active_req_socket == request_socket_a ? :a : :b
          zmq_named_socket_close :request_socket_a
          zmq_named_socket_close :request_socket_b
          self.active_req_socket= active == :a ? request_socket_b : request_socket_a
        end
      end
    ensure
      zmq_cleanup
    end

  private

    def initialize_worker(options)
      @peer_res_address = options.fetch(:peer_res_address) { raise ArgumentError,'missing :peer_res_address in options' }
      @peer_req_address_a = options.fetch(:peer_req_address_a) { raise ArgumentError,'missing :peer_req_address_a in options' }
      @peer_req_address_b = options.fetch(:peer_req_address_b) { raise ArgumentError,'missing :peer_req_address_b in options' }
      @identity     = options.fetch(:identity,nil)
    end

    def active_req_socket
      @active_req_socket || raise("no req socket is currently active")
    end

    def switch_active_socket
      self.active_req_socket = case active_req_socket
      when request_socket_a then request_socket_b
      when request_socket_b then request_socket_a
      else 
        raise
      end
    end

    def active_req_socket=(socket)
      @active_req_socket=socket
      debug { "switched to socket: #{zmq_identity(socket)}" }
    end

    def dispatch_job(job)
      name = job.fetch 'name'
      arguments = job.fetch 'arguments'
      block = self.class.registry[name]
      unless block
        warn { "name: #{name.inspect} is not registred" }
        return
      end
      info { "executing: #{name}, #{arguments.inspect}" }
      case block
      when Proc
        block.call *arguments
      when true
        send(name,*arguments)
      else
        raise
      end
    end

    def process_job(client_address,job)
      raise unless job.length == 1
      job = JSON.load(job.first)
      result = dispatch_job(job)
      result = JSON.dump(:result => result)
      zmq_send(response_socket,DELIM + client_address + DELIM + [result])
    end

    def pull_job
      zmq_send active_req_socket,DELIM + HALLO
      ready = ZMQ.select([active_req_socket],nil,nil,10)
      return unless ready
      client_address,job_body =  zmq_split(zmq_recv(active_req_socket))
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
        socket.connect peer_req_address_a
      end
    end

    def request_socket_b
      zmq_named_socket(:request_socket_b,ZMQ::DEALER) do |socket|
        socket.setsockopt(ZMQ::IDENTITY,"#{identity}.req.b") if identity
        socket.setsockopt(ZMQ::LINGER,0)
        socket.connect peer_req_address_b
      end
    end

    def response_socket
      zmq_named_socket(:response_socket,ZMQ::DEALER) do |socket|
        socket.setsockopt(ZMQ::IDENTITY,"#{identity}.res") if identity
        socket.setsockopt(ZMQ::LINGER,0)
        socket.connect peer_res_address
      end
    end

    module ClassMethods

      def registry
        @registry ||= {}
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
      base.send :register,:echo do |*arguments|
        arguments
      end
    end
  end
end
