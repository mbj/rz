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

      @active_req_socket = @request_socket_a
      @blocking_socket   = @frontend_socket

      run_hook :before_loop

      loop do
        if @active_req_socket == @request_socket_a
          p :acitve_a
        end
        if @active_req_socket == @request_socket_b
          p :acitve_b
        end
        if @blocking_socket == @active_req_socket
          p :block_req
        end
        if @blocking_socket == @frontend_socket
          p :block_frontend
        end
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
        addr,body = rz_split(message)
        rz_send(@frontend_socket,body)
      end
    end

    def process_frontend_socket
      count = rz_consume_all(@active_req_socket) do |message|
        worker_addr,worker_body = rz_split(message)
        job_addr,job_body = rz_split(rz_recv(@frontend_socket))
        parts = worker_addr + job_addr + DELIM + job_body
        rz_send(@active_req_socket,parts)
        run_hook :request
      end
      @blocking_socket = @active_req_socket if count.zero?
      self
    end

    def process_active_req_socket
      count = rz_consume_all(@frontend_socket) do |message|
        job_addr,job_body = rz_split(message)
        worker_addr,worker_body = rz_split(rz_recv(@active_req_socket))
        parts = worker_addr + job_addr + DELIM + job_body
        rz_send(@active_req_socket,parts)
        run_hook :request
      end
      @blocking_socket = @frontend_socket if count.zero?
    end

    def process_ready(ready)
      ready.each do |socket|
        case socket
        when @response_socket
          process_response_socket
        when @frontend_socket
          process_frontend_socket
        when @active_req_socket
          process_active_req_socket
        else
          raise 'should not happen'
        end
      end
      self
    end

    def noop_socket(socket)
      rz_consume_all(socket) do |message|
        addr,body = rz_split(message)
        rz_send(socket,addr + DELIM + NOOP)
      end
    end

    def switch_active_req_socket
      @active_req_socket = 
        case @active_req_socket
        when @request_socket_a then @request_socket_b
        when @request_socket_b then @request_socket_a
        else
          raise 'should not happen'
        end
    end

    def noop
      noop_socket(@active_req_socket)

      switch_active_req_socket

      @blocking_socket = @frontend_socket

      run_hook :noop

      self
    end

    def self.included(base)
      base.send :include,Hooking
    end
  end
end
