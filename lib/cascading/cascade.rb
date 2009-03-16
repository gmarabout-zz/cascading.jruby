# cascade.rb
#
# Copyright 2009, Grégoire Marabout. All Rights Reserved.
#
# This is free software. Please see the LICENSE and COPYING files for details.

require "cascading/base"

module Cascading
  class CascadeFactory 

    def flow(node, *args, &block)
      if block.nil?
        return Cascading::Flow.get(name)
      else
        return Cascading::Flow.new(name)
      end
    end

  end

  class Cascade
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