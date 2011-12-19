require 'zmq'
require 'rz/logging'

module RZ
  module Context
    include Logging

    NOOP = ['NOOP'].freeze
    DELIM = [''].freeze

  protected

    # Cleanup this zmq environment. 
    # zmq context and all registred sockets are closed.
    #
    # @return self
    #
    def rz_cleanup
      rz_sockets = self.rz_sockets.dup
      rz_sockets.each do |socket|
        socket.setsockopt(ZMQ::LINGER,0)
        rz_socket_close(socket)
      end

      rz_context.close 

      @rz_context = @rz_sockets = nil

      self
    end

    def rz_select(sockets,timeout)
      debug { "select: #{rz_identities(sockets).inspect}, #{timeout}" }
      ready = ZMQ.select(sockets,[],[],timeout)
      debug do 
        message = 
          if ready
            rz_identities(ready.first)
          end
        debug { "select done #{message.inspect}" }
      end
      ready.first if ready
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
    #   each recived message
    #
    # @return Integer
    #   The number of messages yielded
    def rz_consume_all(socket)
      count = 0
      loop do
        message = rz_recv(socket,ZMQ::NOBLOCK)
        break unless message
        count +=1
        yield message
      end

      count
    end

    # Recive one message from socket
    #
    # @param[ZMQ::Socket] socket
    #   the socket where message should be recived from
    #
    # @yield [Array<String>] 
    #   the recived message
    #
    # @return [true|false]
    #   true if message was yielded otherwise false 
    #
    def rz_consume_one(socket)
      message = rz_recv(socket,ZMQ::NOBLOCK)
      yield message if message

      !!message
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


    def rz_socket_type(type)
      %W(DEALER ROUTER REQ RES PUSH PULL).each do |name|
        ZMQ.const_get(name) == type
        return name
      end
      nil
    end

    # Recive all parts of a message at once with logging
    #
    # @param [ZMQ::Socket] socket
    #   the socket where message should be recived from
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
      debug { "#{rz_identity(socket)} recv message" }
      message = []
      loop do
        part = socket.recv(flags)
        unless part
          debug { "#{rz_identity(socket)} recved no message" }
          return
        end
        message << part
        more = socket.getsockopt(ZMQ::RCVMORE)
        break unless more
      end
      debug { "#{rz_identity(socket)} recved message: #{message.inspect}" }

      message
    end

    # Query multiple socket identities
    #
    # This is a helper method for debugging
    #
    # @param [Array<ZMQ::Socket>] sockets
    #   the sockets to be queried
    #
    # @return [Array<String>] 
    #   the sockets identities
    #
    def rz_identities(sockets)
      sockets.map { |socket| rz_identity(socket) }
    end

    # Query socket identity
    #
    # @param [ZMQ::Socket] socket
    #   the socket to be queried
    #
    # @return [String] 
    #   the sockets identity
    #
    def rz_identity(socket)
      socket.getsockopt(ZMQ::IDENTITY)
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
      debug { "#{rz_identity(socket)} send message: #{message.inspect}" }
      message.each_with_index do |part,idx|
        last = message.length == idx+1
        flags = 0
        flags |= ZMQ::SNDMORE unless last
        unless socket.send part,flags
          raise
        end
      end
      debug { "#{rz_identity(socket)} send finished" }

      self
    end

    # Create a registred zmq socket
    # Registred zmq sockets will be closed when #rz_cleanup is called.
    #
    # @param [ZMQ::{REQ,REP,PUB,SUB,PUSH,PULL,PAIR]]
    #   the zmq socket type to be created
    #
    # @return [ZMQ::Socket] the created socket
    #
    def rz_socket(type)
      debug { "creating socket of type: #{rz_socket_type(type)}" }
      socket = rz_context.socket(type)
      debug { "created socket: #{socket}" }
      rz_sockets << socket

      socket
    end

    # Close a socket and unregister it
    #
    # @param [ZMQ::Socket] socket to be closed
    #
    # @return self
    #
    def rz_socket_close(socket)
      debug { "closing socket: #{socket}" }
      socket.close
      debug { "closed socket: #{socket}" }
      rz_sockets.delete(socket)

      self
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
