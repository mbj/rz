require 'json'
require 'rz/context'

module RZ
  module Service
    include Context

    def run
      # initializing sockets
      frontend
      backend_req_b
      self.active_req_socket = backend_req_a
      self.blocking_socket   = frontend
      loop do
        ready = ZMQ.select([backend_res,blocking_socket],nil,nil,1)
        if ready
          process_ready ready.first
        else
          noop
        end
      end
    end

  private

    attr_reader :identity,:frontend_address,:backend_res_address,:backend_req_address_a,:backend_req_address_b

    attr_reader :active_req_socket, :blocking_socket
    private :active_req_socket, :blocking_socket

    def initialize_service(options)
      @frontend_address      = options.fetch(:frontend_address)      { raise ArgumentError,'missing :frontend_address' }
      @backend_req_address_a = options.fetch(:backend_req_address_a) { raise ArgumentError,'missing :backend_req_address_a'  }
      @backend_req_address_b = options.fetch(:backend_req_address_b) { raise ArgumentError,'missing :backend_req_address_b'  }
      @backend_res_address   = options.fetch(:backend_res_address)   { raise ArgumentError,'missing :backend_res_address'  }
      @identity              = options.fetch(:identity,nil)
    end

    def active_req_socket=(socket)
      if active_req_socket == blocking_socket
        self.blocking_socket = socket
      end
      @active_req_socket=socket
      debug { "switched active req socket to: #{zmq_identity(socket)}" }
    end

    def blocking_socket=(socket)
      return if @blocking_socket == socket
      @blocking_socket = socket
      debug { "switched blocking socket to: #{zmq_identity(socket)}" }
    end

    def switch_active_req_socket
      self.active_req_socket = case active_req_socket
      when backend_req_a then backend_req_b
      when backend_req_b then backend_req_a
      else
        raise
      end
    end

    def process_ready(ready)
      ready.each do |socket|
        case socket
        when backend_res
          p :backend
          # Pusing response to client
          addr,body = zmq_split zmq_recv(backend_res)
          zmq_send frontend,body
        when frontend
          p :frontend
          # Find worker for job
          message = zmq_recv(active_req_socket,ZMQ::NOBLOCK)
          if message
            job_addr,job_body = zmq_split zmq_recv(frontend)
            worker_addr,worker_body = zmq_split message
            zmq_send active_req_socket,worker_addr + job_addr + DELIM + job_body
          else
            self.blocking_socket = active_req_socket
          end
        when active_req_socket
          # Find job for worker
          message = zmq_recv frontend,ZMQ::NOBLOCK
          if message
            worker_addr,worker_body = zmq_split zmq_recv(active_req_socket)
            job_addr,job_body = zmq_split message
            zmq_send active_req_socket,worker_addr + job_addr + DELIM + job_body
          else
            self.blocking_socket = frontend
          end
        else
          raise
        end
      end
    end

    def noop
      loop do
        message = zmq_recv active_req_socket,ZMQ::NOBLOCK
        break unless message
        addr,body = zmq_split message
        zmq_send(active_req_socket,addr + DELIM + NOOP)
      end
      switch_active_req_socket
    end

    def backend_req_a
      zmq_named_socket :backend_req_a,ZMQ::ROUTER do |socket|
        socket.setsockopt ZMQ::IDENTITY,"#{identity}.req.backend.a" if identity
        socket.bind backend_req_address_a
      end
    end

    def backend_req_b
      zmq_named_socket :backend_req_b,ZMQ::ROUTER do |socket|
        socket.setsockopt ZMQ::IDENTITY,"#{identity}.req.backend.b" if identity
        socket.bind backend_req_address_b
      end
    end

    def backend_res
      zmq_named_socket :backend_res,ZMQ::ROUTER do |socket|
        socket.setsockopt ZMQ::IDENTITY,"#{identity}.res.backend" if identity
        socket.bind backend_res_address
      end
    end

    def frontend
      zmq_named_socket :frontend,ZMQ::ROUTER do |socket|
        socket.setsockopt(ZMQ::IDENTITY,"#{identity}.frontend") if identity
        socket.bind frontend_address
      end
    end
  end
end
