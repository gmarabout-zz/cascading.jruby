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
    def draw(dir, properties = nil)
      @children.each do |flow|
        flow.connect(properties).writeDOT("#{dir}/#{flow.name}.dot")
      end
    end

    def sink_metadata
      @children.inject({}) do |sink_fields, flow|
        sink_fields[flow.name] = flow.sink_metadata
        sink_fields
      end
    end

    def write_sink_metadata(file_name)
      File.open(file_name, 'w') do |file|
        YAML.dump(sink_metadata, file)
      end
    end

    def complete(properties = nil)
      parameters = make_flows(@children, properties)
      cascade = Java::CascadingCascade::CascadeConnector.new().connect(parameters)
      cascade.complete
    end

    private

    def make_flows(flows, properties)
      flow_instances = flows.map do |flow|
        cascading_flow = flow.connect(properties)
        flow.listeners.each { |l| cascading_flow.addListener(l) }
        cascading_flow
      end
      flow_instances.to_java(Java::CascadingFlow::Flow)
    end
  end
end
