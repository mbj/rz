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
        sockets = [@response_socket,@blocking_socket]
        ready = rz_select(sockets,1)
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
      %w(response_socket request_socket frontend_socket).
        each do |socket_name|
          socket = rz_socket(ZMQ::ROUTER)
          socket.bind(
            Helpers.fetch_option(
              @options,socket_name.gsub('socket','address').to_sym
            )
          )
          instance_variable_set(:"@#{socket_name}",socket)
        end

      setup_socket_identities if @rz_identity
    end

    def setup_socket_identities
      @request_socket.setsockopt(
        ZMQ::IDENTITY,"#{@rz_identity}.req.backend"
      )
      @response_socket.setsockopt(ZMQ::IDENTITY,"#{@rz_identity}.res") 
      @frontend_socket.setsockopt(ZMQ::IDENTITY,"#{@rz_identity}.frontend") 
    end

    def initialize_service(options)
      @sequence = 1
      @options = options
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
      rz_send(@request_socket,message)
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

    def process_socket(socket)
      case socket
      when @response_socket
        process_response_socket
      when @frontend_socket
        process_pipe(@frontend_socket,@request_socket)
      when @request_socket
        process_pipe(@request_socket,@frontend_socket)
      else
        raise 'should not happen'
      end
    end

    def process_ready(ready)
      ready.each do |socket|
        process_socket(socket)
      end
      self
    end

    def noop_workers
      @sequence += 1
      sequence = Helpers.pack_int(@sequence)
      consumes = 0
      rz_consume_all(@request_socket) do |message|
        consumes += 1
        addr,body = rz_split(message)
        worker_sequence = Helpers.unpack_int(body.first)
        if worker_sequence == @sequence
          rz_send(@request_socket,addr + [Helpers.pack_int(0)])
          break
        else
          rz_send(@request_socket,addr + [sequence])
        end
      end
      rz_debug { "noop consumes: #{consumes}" }
    end

    def noop
      noop_workers

      @blocking_socket = @frontend_socket

      run_hook :noop

      self
    end

    def self.included(base)
      base.send(:include,Hooking)
    end
  end
end
