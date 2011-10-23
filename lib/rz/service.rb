require 'json'
require 'rz/context'

module RZ
  module Service
    include Context

    def run
      loop do
        ready = ZMQ.select([backend],nil,nil,1)
        next unless ready
        read,write,error = ready
        read.each do |socket|
          case socket
          when backend
            process_backend
          else
            raise
          end
        end
      end
    end

  private

    attr_reader :identity,:frontend_address,:backend_address
   
    def initialize_service(options)
      @frontend_address = options.fetch(:frontend_address) { raise ArgumentError,'missing :frontend_address' }
      @backend_address  = options.fetch(:backend_address)  { raise ArgumentError,'missing :backend_address'  }
      @identity         = options.fetch(:identity,nil)
    end

    def process_backend
      addr,body = zmq_split(zmq_recv(backend))

      if body == HALLO
        process_job_req(addr,body)
      else
        zmq_send(frontend,body)
      end
    end

    def process_job_req(addr,body)
      ready = ZMQ.select([frontend],nil,nil,1)
  
      unless ready
        zmq_send(backend,addr + DELIM + %w(NOOP))
      else 
        job = zmq_recv(frontend)
        req_addr,job_body = zmq_split(job)
        zmq_send(backend,addr + req_addr + DELIM + job_body)
      end
    end

    def backend
      zmq_named_socket :backend,ZMQ::ROUTER do |socket|
        socket.bind backend_address
        socket.setsockopt(ZMQ::IDENTITY,"#{identity}.backend") if identity
      end
    end

    def frontend
      zmq_named_socket :frontend,ZMQ::ROUTER do |socket|
        socket.bind frontend_address
        socket.setsockopt(ZMQ::IDENTITY,"#{identity}.frontend") if identity
      end
    end
  end
end
