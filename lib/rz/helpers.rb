module RZ
  module Helpers
    def self.fetch_option(option,name,klass=nil)
      unless option.kind_of?(Hash)
        raise "only can fetch option from Hash got #{option.class}"
      end
      value = option.fetch(name) do
        raise ClientError,"missing #{name.inspect} in option"
      end
      if klass and !value.is_a?(klass)
        raise ClientError,"#{name} is not a #{klass}"
      end
      value
    end

    def self.pack_int(int)
      [int].pack('N')
    end

    def self.unpack_int(data)
      unless data.bytesize == 4
        raise 'data length must be 4'
      else
        data.unpack('N').first
      end
    end
  end
end
