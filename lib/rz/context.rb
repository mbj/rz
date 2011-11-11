require 'zmq'

module RZ
  module Context

  NOOP  = ['NOOP'].freeze
  DELIM = [''].freeze

  private

    # NOOP override this if you whant to log messages
    def log(level,&block)
    end

    def debug(&block)
      log :debug,&block 
    end

    def warn(&block)
      log :warn,&block 
    end

    def info(&block)
      log :info,&block 
    end

    def error(&block)
      log :error,&block 
    end

    def zmq_cleanup
      zmq_named_sockets.each_key do |name|
        zmq_named_socket_close name
      end
      zmq_sockets.each do |socket|
        zmq_socket_close socket
      end
      raise unless zmq_sockets.length.zero?
      zmq_context.close 
      @zmq_named_sockets = @zmq_context = @zmq_sockets = nil
    end

    def zmq_context
      @zmq_context ||= ZMQ::Context.new
    end

    def zmq_split(message)
      delim = nil
      message.each_with_index do |part,idx|
        if part.length == 0 
          delim = idx 
          break
        end
      end
      raise 'undelimited message' unless delim
      [message[0...delim],message[(delim+1)..-1]]
    end

    def zmq_recv(socket,flags=0)
      debug { "#{zmq_identity(socket)} recv message" }
      message = []
      loop do
        part = socket.recv(flags)
        return unless part
        message << part
        more = socket.getsockopt ZMQ::RCVMORE
        opts = 0
        opts |= ZMQ::SNDMORE if more
        break unless more
      end
      debug { "#{zmq_identity(socket)} recved message: #{message.inspect}" }
      message
    end

    def zmq_identity(socket)
      socket.getsockopt(ZMQ::IDENTITY) || 'UNKOWN'
    end

    def zmq_send(socket,message)
      debug { "#{zmq_identity(socket)} send message: #{message.inspect}" }
      message.each_with_index do |part,idx|
        last = message.length == idx+1
        flags = 0
        flags |= ZMQ::SNDMORE unless last
        unless socket.send part,flags
          raise
        end
      end
      debug { "#{zmq_identity(socket)} send finished" }
      self
    end

    def zmq_named_socket(name,type)
      zmq_named_sockets[name] ||= begin
                                    socket = zmq_socket(type)
                                    yield socket if block_given?
                                    socket
                                  end
    end

    def zmq_named_socket_close(name)
      socket = zmq_named_sockets.delete name
      unless socket
        warn { "requested to close inexistend named socket: #{name.inspect}" } 
      end
      zmq_socket_close socket if socket
    end

    def zmq_named_sockets
      @zmq_named_sockets ||= {}
    end

    def zmq_socket(type)
      socket = zmq_context.socket(type)
      zmq_sockets << socket
      socket
    end

    def zmq_socket_close(socket)
      socket.close
      zmq_sockets.delete socket
    end

    def zmq_sockets
      @zmq_sockets ||= []
    end
  end
end
