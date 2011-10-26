require 'json'
require 'rz/context'

module RZ
  module Client
    include Context

    attr_reader :service_address,:identity

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
      @service_address = options.fetch(:service_address) { raise ArgumentError,'missing :service_address in options' }
      @identity        = options.fetch(:identity,nil)
    end

    def service_socket
      zmq_named_socket :service,ZMQ::DEALER do |socket|
        socket.setsockopt(ZMQ::IDENTITY,identity) if identity
        socket.connect service_address
      end
    end
  end
end
