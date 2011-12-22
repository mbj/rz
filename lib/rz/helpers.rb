module RZ
  module Helpers
    def self.fetch_option(job,name,klass=nil)
      value = job.fetch(name) do
        raise ClientError,"missing #{name.inspect} in job"
      end
      if klass and !value.is_a?(klass)
        raise ClientError,"#{name} is not a #{klass}"
      end
      value
    end
  end
end
