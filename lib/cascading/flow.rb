require "cascading/assembly"

module Cascading

  class FlowFactory < Cascading::BaseFactory

    def initialize(*args, &block)
      super
      @sources= {}
      @sinks = {}
      @assemblies = []
    end

    def assembly(*args, &block)
      if block.nil?
        assembly = Cascading::AssemblyFactory.get(name)
      else
        assembly = Cascading::AssemblyFactory.new(name, &block)
      end
      @assemblies << assembly  
    end

    # Create a new sink for this flow, with the specified name.
    # "tap" can be either a tap (see Cascading.tap) or a string that will 
    # reference a path.
    def sink(name, tap)
      @sinks[name] = make_tap tap 
    end

    # Create a new source for this flow, with the specified name.
    # "tap" can be either a tap (see Cascading.tap) or a string that will 
    # reference a path.
    def source(name, tap)
      @sources[name] = make_tap tap
    end

    # Builds the flow and returns it as an instance of cascading.flow.Flow
    def make
      super
      pipes = []
      @assemblies.map do |assembly|
        pipes += assembly.make
        puts pipes
      end
      puts "Making flow: #{@name} - Sources: #{@sources} - Sinks: #{@sinks} - Pipes: #{pipes}"
      Java::CascadingFlow::FlowConnector.new.connect(@name.to_s, @sources, @sinks, pipes.to_java(Java::CascadingPipe::Pipe))
    end

    # Shortcuts for FlowFactory.make.complete
    def complete
      execute
    end

    def execute
      flow = make
      flow.complete
    end

    private 
    
    def make_tap tap
      if (tap.is_a? ::String) || (tap.is_a? ::Symbol)
        tap = Cascading.tap(tap)
      end
      tap
    end
  end
end