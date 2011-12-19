require 'json' 
require 'rz/context'
require 'rz/hooking'
require 'rz/logging'

module RZ
  module Service
    include Context
    include Logging

    def run
      run_hook :before_run

      open_sockets

      @active_request_socket   = @request_socket_a
      @inactive_request_socket = @request_socket_b

      @blocking_socket = @frontend_socket

      run_hook :before_loop

      loop do
        run_hook :loop_start
        ready = rz_select([@response_socket,@blocking_socket],1)
        if ready
          process_ready(ready)
        else
          noop
        end
        run_hook :loop_end
      end
    rescue Interrupt, SignalException
      run_hook :interrupted
      raise
    ensure
      rz_cleanup
    end

  private

    def open_sockets
      debug { "opening sockets" }
      @response_socket = rz_socket(ZMQ::ROUTER)
      @response_socket.bind(response_address)

      @request_socket_a = rz_socket(ZMQ::ROUTER)
      @request_socket_a.bind(request_address_a)

      @request_socket_b = rz_socket(ZMQ::ROUTER)
      @request_socket_b.bind(request_address_b)

      @frontend_socket = rz_socket(ZMQ::ROUTER)
      @frontend_socket.bind(frontend_address)

      if identity
        @request_socket_a.setsockopt(ZMQ::IDENTITY,"#{identity}.req.backend.a") 
        @request_socket_b.setsockopt(ZMQ::IDENTITY,"#{identity}.req.backend.b") 
        @response_socket.setsockopt(ZMQ::IDENTITY,"#{identity}.res") 
        @frontend_socket.setsockopt(ZMQ::IDENTITY,"#{identity}.frontend") 
      end
    end

    attr_reader :identity,
                :frontend_address,
                :response_address,
                :request_address_a,
                :request_address_b

    def initialize_service(options)
      @frontend_address = options.fetch(:frontend_address) do
        raise ArgumentError,'missing :frontend_address'
      end
      @request_address_a = options.fetch(:request_address_a) do
        raise ArgumentError,'missing :request_address_a'
      end
      @request_address_b = options.fetch(:request_address_b) do
        raise ArgumentError,'missing :request_address_b'
      end
      @response_address = options.fetch(:response_address) do
        raise ArgumentError,'missing :response_address'
      end
      @identity = options.fetch(:identity,nil)
    end

    def process_response_socket
      rz_consume_all(@response_socket) do |message|
        addr,body = rz_split2(message)
        rz_send(@frontend_socket,body)
      end
    end

    def process_pipe(active,passive)
      message = rz_recv(passive,ZMQ::NOBLOCK)
      if message
        active_addr,active_body = rz_split2(message)
        passive_addr,passive_body = rz_split2(rz_recv(active))
        message =
          if active == @frontend_socket
            active_addr + passive_addr + passive_body
          else
            passive_addr + active_addr + active_body
          end
        rz_send(passive,message)
        run_hook(:request)
      else
        @blocking_socket = passive
      end
    end

    def process_ready(ready)
      ready.each do |socket|
        case socket
        when @response_socket
          process_response_socket
        when @frontend_socket
          process_pipe(@frontend_socket,@active_request_socket)
        when @active_request_socket
          process_pipe(@active_request_socket,@frontend_socket)
        else
          raise 'should not happen'
        end
        # catch workers waiting on the wrong socket
        p :noop_inactive
        noop_socket(@inactive_request_socket)
      end
      self
    end

    def noop_socket(socket)
      rz_consume_all(socket) do |message|
        addr,body = rz_split2(message)
        rz_send(socket,addr + DELIM)
      end
    end

    def switch_active_request_socket
      if @active_request_socket == @request_socket_a
          @active_request_socket = @request_socket_b
        @inactive_request_socket = @request_socket_a
      else
        @active_request_socket   = @request_socket_a
        @inactive_request_socket = @request_socket_b
      end
    end

    def noop
      noop_socket(@active_request_socket)

      switch_active_request_socket

      @blocking_socket = @frontend_socket

      run_hook :noop

      self
    end

    def self.included(base)
      base.send :include,Hooking
    end
  end
end
