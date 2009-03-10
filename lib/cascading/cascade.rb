require "cascading/base"

module Cascading
  class CascadeFactory < Cascading::BaseFactory
    
    def initialize(*args, &block)
      super
      @flows = []
    end
    
    def flow(name, &block)
      if block.nil?
        flow = Cascading::FlowFactory.get(name)
      else
        flow = Cascading::FlowFactory.new(name, &block)
      end
      @flows << flow
    end
    
    
    def make 
      super
      connector = Java::CascadingCascade::CascadeConnector.new
      flow_instances = @flows.map do |flow|
        flow.make
      end
      cascade = connector.connect(flow_instances.to_java(Java::CascadingFlow::Flow))
    end
    
    def complete
      cascade = make
      cascade.complete
    end
    
  end
end