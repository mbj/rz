require 'rz/errors'
require 'rz/logging'
require 'rz/helpers'

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

      def inherited(base)
        base.requests.replace(requests.clone)
      end
    end

    module InstanceMethods

      def json_load(body)
        begin
          JSON.load(body)
        rescue
          raise ClientError,'job body is not valid json'
        end
      end

      def process_job_body(body)
        job = json_load(body)

        unless job.is_a?(Hash)
          raise ClientError,'job was not a json object'
        end

        result = process_job(job)

        response = job.merge(
          'status' => :success,
          'result' => result
        )
      rescue ClientError => exception
        JobExecutor.format_exception_response(exception)
      end

      def execute_job(job,block,arguments)
        begin
          case block
          when Proc
            self.instance_exec(*arguments,&block)
            #block.call(*arguments)
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

      def process_job(job)
        name = Helpers.fetch_option(job,'name',String)
        arguments = Helpers.fetch_option(job,'arguments',Array) 

        block = self.class.requests[name]
     
        unless block
          raise ClientError,"job #{name.inspect} is not registred"
        end
     
        rz_info { "executing: #{name}, #{arguments.inspect}" }

        execute_job(job,block,arguments)
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
