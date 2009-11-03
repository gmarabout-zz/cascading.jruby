# cascade.rb
#
# Copyright 2009, Grégoire Marabout. All Rights Reserved.
#
# This is free software. Please see the LICENSE and COPYING files for details.

require "cascading/base"

module Cascading
  class CascadeFactory 

    def flow(node, *args, &block)
      name = args[0]
      if block.nil?
        return Cascading::Flow.get(name)
      else
        return Cascading::Flow.new(name, &block)
      end
    end

  end

  class Cascade < Cascading::Node
    attr_accessor :flows

    def initialize(name, parent=nil, &block)
      @flows = []
      super(name, parent, &block)
    end

    def flow(name, *args, &block)
      # Call method_missing to send to factory and side-effect @children
      @flows << method_missing(:flow, *args, &block)
    end

    def complete
      parameters = make_flows(@flows)
      cascade = Java::CascadingCascade::CascadeConnector.new().connect(parameters)
      cascade.complete
    end

    private

    def make_flows(flows)
      flow_instances = flows.map do |flow|
        flow.connect
      end
      flow_instances.to_java(Java::CascadingFlow::Flow)
    end
  end
end
