require 'json'
require 'rz/context'

module RZ
  module Client
    include Context

    attr_reader :services,:identity

    def request(name,job)
      zmq_send(service_socket(name),['',JSON.dump(job)])
    end

    def receive(name,options={})
      timeout = options.fetch(:timeout,1)

      ready = ZMQ.select([service_socket(name)],nil,nil,timeout)
      return unless ready
      body = zmq_split(zmq_recv(service_socket(name))).last
      raise unless body.length == 1
      JSON.load(body.first)
    end

  private

    # @example:
    #   Worker.new(
    #     :service_a => "tcp://127.0.0.1:4000",
    #     :service_b => "tcp://127.0.0.1:4001",
    #     :service_c => "tcp://127.0.0.1:4002",
    #   )

    def initialize_client(options)
      @services = options.fetch(:services) { raise ArgumentError,'missing :service_address in options' }
      @identity        = options.fetch(:identity,nil)
    end

    def service_socket(name)
      zmq_named_socket "service_#{name}",ZMQ::DEALER do |socket|
        socket.setsockopt(ZMQ::IDENTITY,identity) if identity
        socket.connect service_address(name)
      end
    end

    def service_address(name)
      services.fetch(name) { raise ArgumentError,"no address for service #{name.inspect}" }
    end
  end
end
