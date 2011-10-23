require 'json'
require 'rz/context'

module RZ
  module Client
    include Context

    attr_reader :peer_address,:identity

    def request(name,*arguments)
      job = { :name => name, :arguments => arguments }
      zmq_send(service_socket,['',JSON.dump(job)])
    end

    def receive(options={})
      timeout = options.fetch(:timeout,1)
      ready = ZMQ.select([service_socket],nil,nil,timeout)
      return unless ready
      body = zmq_split(zmq_recv(service_socket)).last
      raise unless body.length == 1
      result = JSON.load(body.first).fetch('result')
    end

  private

    def initialize_client(options)
      @peer_address = options.fetch(:peer_address) { raise ArgumentError,'missing :peer_address in options' }
      @identity     = options.fetch(:identity,nil)
    end

    def service_socket
      zmq_named_socket :service,ZMQ::DEALER do |socket|
        socket.connect peer_address
        socket.setsockopt(ZMQ::IDENTITY,identity) if identity
      end
    end
  end
end
