require 'zmq'
require 'rz/logging'

module RZ
  module Context
    include Logging

    NOOP = ['NOOP'].freeze
    DELIM = [''].freeze

  protected

    # Close all registred zmq sockets
    # @return self
    def rz_sockets_close
      rz_sockets = self.rz_sockets.dup
      rz_sockets.each do |socket|
        #socket.setsockopt(ZMQ::LINGER,0)
        rz_socket_close(socket)
      end
      @rz_sockets = nil
    end

    # Cleanup this zmq environment. 
    # zmq context and all registred sockets are closed.
    #
    # @return self
    #
    def rz_cleanup
      rz_debug { 'rz cleanup' }

      rz_sockets_close

      rz_context.close 

      @rz_context = nil

      self
    end

    def rz_select(sockets,timeout)
      rz_debug do
        identities = sockets.map { |socket| socket.getsockopt(ZMQ::IDENTITY) }
        "select: #{identities}, #{timeout}"
      end

      ready = ZMQ.select(sockets,nil,nil,timeout)
      unless ready
        rz_debug { "select done nil" }
        return
      end

      ready = ready.first

      rz_debug do
        identities = ready.map { |socket| socket.getsockopt(ZMQ::IDENTITY) }
        "select done #{identities}"
      end

      ready
    end

    # This contexts zmq context
    #
    # @return [ZMQ::Context]
    def rz_context
      @rz_context ||= ZMQ::Context.new
    end

    # Recive all messages from socket until the first block
    #
    # @param[ZMQ::Socket] socket
    #   the socket where messages should be recieved from 
    #
    # @yield [Array<String>] 
    #   each received message
    #
    # @return Integer
    #   The number of messages yielded
    def rz_consume_all(socket)
      count = 0
      while message = rz_recv(socket,ZMQ::NOBLOCK)
        count +=1
        yield message
      end

      count
    end

    # Split message into two parts. 
    #
    # The first part is an array with only the first element.
    # The second part is an array of the rest.
    #
    # @raise ArgumnetError 
    #   if message has fewer than 2 elements.
    #
    # @param [Array<String>] message
    #   the message to be splitted
    #
    # @return 
    #   a two element array
    #
    # @example
    #   message = ['my','message','with','multiple','parts']
    #   rz_split(message) => [['my'],['message','with','multiple','parts']]
    #
    def rz_split(parts)
      if parts.length < 2
        raise ArgumnentError,'message must have at least two parts'
      end
      [[parts.first],parts[1..-1]]
    end

    # Recive all parts of a message at once with logging
    #
    # @param [ZMQ::Socket] socket
    #   the socket where message should be received from
    #
    # @param [Integer] flags
    #   the zmq flags used for recieving messages
    #   @see http://zeromq.github.com/rbzmq/classes/ZMQ/Context.html#M000004
    #
    # @return [Array<String>,nil] 
    #   the recievied message parts as an array of strings or nil when 
    #   no message was recieved
    #
    def rz_recv(socket,flags=0)
      identity = socket.getsockopt(ZMQ::IDENTITY)
      rz_debug { "#{identity} recv message" }
      message = []
      loop do
        part = socket.recv(flags)
        break unless part
        message << part
        more = socket.getsockopt(ZMQ::RCVMORE)
        break unless more
      end
      
      if message.empty?
        rz_debug { "#{identity} recved no message" }
        nil
      else
        rz_debug { "#{identity} recved message: #{message.inspect}" }
        message
      end
    end

    # Send all parts of a message at once with logging
    #
    # @param [ZMQ::Socket] socket
    #   the destination socket
    #
    # @param [Array<String>] message
    #   the array of parts to be send
    #
    # @return self
    #
    def rz_send(socket,message)
      identity = socket.getsockopt(ZMQ::IDENTITY)
      rz_debug { "#{identity} send message: #{message.inspect}" }
      max = message.length-1
      message.each_with_index do |part,idx|
        socket.send(part,idx == max ? 0 : ZMQ::SNDMORE) || raise
      end
      rz_debug { "#{identity} send finished" }

      self
    end

    # Read blocking from socket with timeout
    def rz_read_timeout(socket,timeout)
      ready = rz_select([socket],timeout)
      return unless ready
      rz_recv(ready.first)
    end

    # Close a socket and unregister it
    #
    # @param [ZMQ::Socket] socket to be closed
    #
    # @return self
    #
    def rz_socket_close(socket)
      identity = socket.getsockopt(ZMQ::IDENTITY)
      rz_debug { "closing socket: #{identity}" }
      socket.close
      rz_debug { "closed socket: #{identity}" }
      rz_sockets.delete(socket)

      self
    end

    def rz_socket(type)
      rz_debug { "creating socket" }
      socket = rz_context.socket(type) 
      rz_sockets << socket
      socket
    end

    # Return all registred zmq sockets
    #
    # @return [Array<ZMQ::Socket>]
    #
    def rz_sockets
      @rz_sockets ||= []
    end
  end
end
