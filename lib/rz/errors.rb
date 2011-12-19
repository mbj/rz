module RZ
  # Standard RZ related error
  class Error < StandardError; end
  # Client request related error
  class ClientError < Error; end
  # Client job execution related error
  class ClientJobExecutionError < ClientError
    attr_reader :original_exception
    attr_reader :job

    # Initialize this class
    #
    # @param [Exception] original_exception 
    #   the exception to be wrapped
    # @param [Hash] job the job causing this error
    def initialize(original_exception,job)
      @original_exception,@job = original_exception,job

      message = 'job %s failed with %s' %
        [job.fetch('name').inspect,original_exception.class]

      super(message)
    end
  end
end
