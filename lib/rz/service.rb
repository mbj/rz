require 'json' 
require 'rz/context'
require 'rz/hooking'
require 'rz/logging'
require 'rz/helpers'

module RZ
  module Service
    include Context
    include Logging

    def run_service
      run_hook(:before_run)
      open_sockets
      @request_sockets = [@request_socket_a,@request_socket_b]

      @blocking_socket = @frontend_socket

      process_loop
    rescue Interrupt, SignalException
      run_hook(:interrupted)
    ensure
      rz_cleanup
    end


  protected

    def process_loop
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
    end

    def open_sockets
      rz_debug { "opening sockets" }
      %w(response_socket request_socket_a request_socket_b frontend_socket).
        each do |socket_name|
          socket = rz_socket(ZMQ::ROUTER)
          socket.bind(self.send(socket_name.gsub('socket','address')))
          instance_variable_set(:"@#{socket_name}",socket)
        end


      setup_socket_identities if @rz_identity
    end

    def setup_socket_identities
      @request_socket_a.setsockopt(ZMQ::IDENTITY,"#{@rz_identity}.req.backend.a") 
      @request_socket_b.setsockopt(ZMQ::IDENTITY,"#{@rz_identity}.req.backend.b") 
      @response_socket.setsockopt(ZMQ::IDENTITY,"#{@rz_identity}.res") 
      @frontend_socket.setsockopt(ZMQ::IDENTITY,"#{@rz_identity}.frontend") 
    end

    attr_reader :frontend_address,
                :response_address,
                :request_address_a,
                :request_address_b

    def initialize_service(options)
      @frontend_address  = 
        Helpers.fetch_option(options,:frontend_address,String)
      @request_address_a = 
        Helpers.fetch_option(options,:request_address_a,String)
      @request_address_b = 
        Helpers.fetch_option(options,:request_address_b,String)
      @response_address  = 
        Helpers.fetch_option(options,:response_address,String)
      @rz_identity = options.fetch(:rz_identity,nil)
    end

    def fetch_option(options,key)
      options.fetch(key) do 
        raise ArgumentError,"missing #{key.inspect} in options"
      end
    end

    def process_response_socket
      rz_consume_all(@response_socket) do |message|
        addr,body = rz_split(message)
        rz_send(@frontend_socket,body)
      end
    end

    def process_pipe_message(active,passive,message)
      active_addr,active_body = rz_split(message)
      passive_addr,passive_body = rz_split(rz_recv(active))
      message =
        if active == @frontend_socket
          active_addr + passive_addr + passive_body
        else
          passive_addr + active_addr + active_body
        end
      rz_send(passive,message)
    end

    def process_pipe(active,passive)
      message = rz_recv(passive,ZMQ::NOBLOCK)
      if message
        process_pipe_message(active,passive,message)
        run_hook(:request)
      else
        @blocking_socket = passive
      end
    end

    def invert_socket(socket)
      first = @request_sockets.first
      if first == socket
        @request_sockets.last
      else
        first
      end
    end

    def process_socket(socket)
      active_request_socket = self.active_request_socket
      case socket
      when @response_socket
        process_response_socket
      when @frontend_socket, active_request_socket
        process_pipe(socket,invert_socket(socket))
      else
        raise 'should not happen'
      end
    end

    def process_ready(ready)
      ready.each do |socket|
        process_socket(socket)
        # catch workers waiting on the wrong socket
        noop_socket(inactive_request_socket)
      end
      self
    end

    def noop_socket(socket)
      rz_consume_all(socket) do |message|
        addr,body = rz_split(message)
        rz_send(socket,addr + DELIM)
      end
    end

    def active_request_socket
      @request_sockets.first
    end

    def inactive_request_socket
      @request_sockets.last
    end

    def switch_active_request_socket
      @request_sockets.reverse!
    end

    def noop
      noop_socket(active_request_socket)

      switch_active_request_socket

      @blocking_socket = @frontend_socket

      run_hook :noop

      self
    end

    def self.included(base)
      base.send(:include,Hooking)
    end
  end
end
