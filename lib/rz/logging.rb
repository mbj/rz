module RZ
  # Log module
  #
  # @example logging with this module
  #   class SomeClass
  #     include RZ::Logging
  #
  #     def my_method
  #        info { "my method was called" }
  #     end
  #
  #     # to actualy see the opts
  #
  #     def rz_log(level,&block)
  #       $stderr.puts "#{level} #{block.call}"
  #     end
  #   end
  module Logging
    # Log message at debug level
    #
    # @param [Proc] &block block called to determine message
    def rz_debug(&block)
      rz_log(:debug,&block)
    end

    # Log message at warn level
    #
    # @param [Proc] &block block called to determine message
    def rz_warn(&block)
      rz_log(:warn,&block)
    end

    # Log message at info level
    #
    # @param [Proc] &block block called to determine message
    def rz_info(&block)
      rz_log(:info,&block)
    end

    # Log message at error level
    #
    # @param [Proc] &block block called to determine message
    def rz_error(&block)
      rz_log(:error,&block)
    end
  private

    # Noop log implementation
    # You should override this if you want logs.
    # @param Symbol level log level 
    # @param Proc &block block called to determine message
    # @return self
    def rz_log(level,&block)
      self
    end
  end
end
