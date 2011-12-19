require 'rz/errors'
require 'rz/logging'

module RZ
  module JobExecutor
    include Logging

    module ClassMethods

      # Return registred requests
      #
      # @return [Hash<Symbol,Block|Symbol>]
      #   the registred requests
      def requests
        @requests ||= {}
      end

    private

      # Register new request
      # @param Symbol name
      #   Name of request
      # @yield yield request if block given
      # @return self
      # @example register a request
      #   class MyWorker
      #     include JobExecutor
      #
      #     # Block form
      #     register :echo_block do |*arguments|
      #       arguments
      #     end
      #
      #     # Method form
      #
      #     def echo_method(*arguments)
      #       arguments
      #     end
      #
      #     register :echo_method
      #   end
      #
      def register(name,&block)
        requests = self.requests
        name = name.to_s
        if requests.key? name
          raise ArgumentError,"#{name.inspect} is already registred"
        end
        requests[name] = block || true
        self
      end
    end

    module InstanceMethods

      def process_job_body(body)
        job = begin
                JSON.load(body)
              rescue
                raise ClientError,'job body is not valid json'
              end

        unless job.is_a?(Hash)
          raise ClientError,'job was not a json object'
        end

        result = process_job(job)

        response = job.merge(
          'state' => :success,
          'result' => result
        )
      rescue ClientError => exception
        JobExecutor.format_exception_response(exception)
      end

      def process_job(job)
        name = job.fetch('name') do
          raise ClientError,'missing "name" in job'
        end

        arguments = job.fetch('arguments') do
          raise ClientError,'missing "arguments" in job'
        end

        unless name.is_a?(String)
          raise ClientError,'name is not a String'
        end

        unless arguments.is_a?(Array)
          raise ClientError,'arguments is not an Array'
        end

        block = self.class.requests[name]
     
        unless block
          raise ClientError,"job #{name.inspect} is not registred"
        end
     
        debug { "executing: #{name}, #{arguments.inspect}" }
     
        begin
          case block
          when Proc
            block.call(*arguments)
          when true
            send(name,*arguments)
          else
            raise 'should not happen'
          end
        rescue Interrupt, SignalException
          raise
        rescue Exception => exception
          raise ClientJobExecutionError.new(exception,job)
        end
      end

    end 

    def self.format_exception_response(exception)
      reported_exception,base = if exception.kind_of?(ClientJobExecutionError)
                                  [exception.original_exception,exception.job]
                                else
                                  [exception,{}]
                                end

      base.merge(
        'status' => 'error',
        'error' => { 
          'type' => reported_exception.class.name, 
          'message' => reported_exception.message,
          'backtrace' => reported_exception.backtrace 
        }
      )
    end

    def self.included(base)
      base.send :extend,ClassMethods
      base.send :include,InstanceMethods
    end
  end
end
