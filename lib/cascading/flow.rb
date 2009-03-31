# flow.rb
#
# Copyright 2009, Grégoire Marabout. All Rights Reserved.
#
# This is free software. Please see the LICENSE and COPYING files for details.

require "cascading/assembly"

module Cascading


  class FlowFactory
    def assembly(node, *args, &block)
      name = args[0]
      if block
        return Cascading::Assembly.new(name, &block)
      else
        return Cascading::Assembly.get(name)
      end
    end
  end


  class Flow < Cascading::Node

    attr_accessor :sources, :sinks    

    def initialize(name, parent=nil, &block)
      @sources = {}
      @sinks = {}
      super(name, parent, &block)
    end

    # Create a new sink for this flow, with the specified name.
    # "tap" can be either a tap (see Cascading.tap) or a string that will 
    # reference a path.
    def sink(*args)
      if (args.size == 2)
        @sinks[args[0]] = args[1]
      elsif (args.size == 1)
        @sinks[@name] =  args[0]
      end 
    end

    # Create a new source for this flow, with the specified name.
    # "tap" can be either a tap (see Cascading.tap) or a string that will 
    # reference a path.
    def source(*args)
      if (args.size == 2)
        @sources[args[0]] = args[1]
      elsif (args.size == 1)
        @sources[@name] = args[0]
      end
    end

    def complete
      parameters = build_connect_parameter()
      flow = Java::CascadingFlow::FlowConnector.new().connect(*parameters)
      flow.complete
    end
    
    private
    
    def build_connect_parameter
      sources = make_tap_parameter(@sources)
      sinks = make_tap_parameter(@sinks)
      pipes = make_pipes
      [sources, sinks, pipes]
    end
    
    def make_tap_parameter taps
      if taps.size == 1
        return taps.values.first
      else
        return taps
      end
    end
    
    def make_pipes
      if @sinks.size==1
        return children.last.tail_pipe
      else
        pipes = []
        @sinks.keys.each do |key|
          assembly = Assembly.get(key)
          if assembly
            pipes << assembly.tail_pipe
          end
        end
        return pipes.to_java(Java::CascadingPipe::Pipe)
      end
    end
    
    
  end
end