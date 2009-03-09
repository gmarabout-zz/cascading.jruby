require "cascading/base"

module Cascading
  class CascadeFactory < Cascading::BaseFactory
    
    def initialize(*args, &block)
      super
      @flows = []
    end
    
    def flow(name, &block)
      if block.nil?
        flow = Cascading::Flow.get(name)
      else
        flow = Cascading::Flow.new(name, &block)
      end
      flows << flow
    end
    
    
    def make 
        instance_eval(&@block) if @block
    end
  end
end