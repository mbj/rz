require 'json'
require 'rz/context'

module RZ
  module Worker
    include Context

    attr_reader :peer_address,:identity

    def run
      loop do
        response = pull_job
        case response
        when :noop
          next
        when Array
          client_address,job = response
          process_job(client_address,job)
        else
          zmq_named_socket_close :worker
        end
      end
    ensure
      zmq_cleanup
    end

  private

    def initialize_worker(options)
      @peer_address = options.fetch(:peer_address) { raise ArgumentError,'missing :peer_address in options' }
      @identity     = options.fetch(:identity,nil)
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
      block.call *arguments
    end

    def process_job(client_address,job)
      raise unless job.length == 1
      job = JSON.load(job.first)
      result = dispatch_job(job)
      result = JSON.dump(:result => result)
      zmq_send(worker_socket,DELIM + client_address + DELIM + [result])
    end

    def pull_job
      ready = ZMQ.select(nil,[worker_socket],nil,1)
      return unless ready
      zmq_send worker_socket,DELIM + HALLO
      ready = ZMQ.select([worker_socket],nil,nil,1)
      return unless ready
      client_address,job_body =  zmq_split(zmq_recv(worker_socket))
      if job_body.first == 'NOOP'
        return :noop
      else
        [client_address,job_body]
      end
    end

    def worker_socket
      zmq_named_socket(:worker,ZMQ::DEALER) do |socket|
        socket.connect peer_address
        socket.setsockopt(ZMQ::LINGER,0)
        socket.setsockopt(ZMQ::IDENTITY,identity) if identity
      end
    end

    module ClassMethods

      def registry
        @registry ||= {}
      end

    private

      def register(name,method=nil,&block)
        name = name.to_s
        if registry.key? name
          raise ArgumentError,"#{type} #{name} is already registred"
        end
        if method and block
          raise ArgumentError,'method or block must be given not both'
        end
        unless method or block
          raise ArgumentError,'method or block must be given'
        end
        registry[name]= block || self.method(method)
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
