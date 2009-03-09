require "cascading/assembly"
require "cascading/flow"
require "cascading/cascade"

module Cascading
    
  # Builder class. It provides class methods to create assemblies, flows and cascades.
  class Builder

    # Creates a new top level assembly using the specified name and specification.
    # It returns an instance of Cascading::AssemblyFactory.
    def self.assembly(name, &block)
      Cascading::AssemblyFactory.new(name, &block)
    end

    # Creates a new top level flow using the specified name and specification.
    # It returns an instance of Cascading::FlowFactory
    def self.flow(name, &block)
      Cascading::FlowFactory.new(name, &block)
    end

    # Creates a new top cascade using the specified name and specification.
    # It returns an instance of Cascading::CascadeFactory
    def self.cascade(name, &block)
      Cascading::CascadeFactory.new(name, &block)  
    end
    
  end
end