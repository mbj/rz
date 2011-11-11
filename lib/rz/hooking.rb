module RZ
  module Hooking
    module InstanceMethods
      def run_hook(name)
        self.class.hooks_for(name).each do |hook|
          case hook
          when Proc
            instance_exec &hook
          when Symbol
            send hook
          else
            raise
          end
        end
      end
    end

    module ClassMethods
      def hook(name,method_name=nil,&block)
        method_name ||= name unless block
        raise ArgumentError,'provide method name or block not both' if method_name and block
        hooks_for(name) << (method_name || block)
      end
    
      def hooks
        @hooks ||= {}
      end
    
      def hooks_for(name)
        hooks[name] ||= []
      end
    end

    def self.included(base)
      base.send :extend,ClassMethods
      base.send :include,InstanceMethods
    end
  end
end
