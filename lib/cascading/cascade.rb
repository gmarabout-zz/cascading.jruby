# Copyright 2009, Grégoire Marabout. All Rights Reserved.
#
# This is free software. Please see the LICENSE and COPYING files for details.

require 'cascading/base'
require 'yaml'

module Cascading
  class Cascade < Cascading::Node
    def initialize(name, &block)
      super(name, nil, &block) # A Cascade cannot have a parent
    end

    def flow(name, &block)
      raise "Could not build flow '#{name}'; block required" unless block_given?
      add_child(Flow.new(name, self, &block))
    end

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
      begin
        parameters = make_flows(@children, properties)
        cascade = Java::CascadingCascade::CascadeConnector.new.connect(parameters)
        cascade.complete
      rescue NativeException => e
        raise CascadingException.new(e, 'Error completing cascade')
      end
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
