module Cascading

  class NodeFactory
    # Nothing here.
  end


  class Node
    attr_accessor :name, :parent, :children    

    # Creates a registry to the sub class:
    def self.inherited(child)
      child.send(:extend, Cascading::Registerable) 
    end

    def initialize(name, parent=nil, &block)
      @name = name
      @parent = parent
      @children = []
      self.class.add(name, self)
      if block
        create_sub_nodes(&block)
      end
    end

    def factory
      @factory ||= make_factory
      @factory
    end

    def make_factory
      factory_class = "Cascading::#{self.class.name}Factory".split("::").inject(Object) { |par, const| par.const_get(const) }
      factory_class.new
    end

    def method_missing(name, *args, &block)
      if factory.respond_to? name
        #puts "Creating node: #{name}"
        child = factory.send(name, self, *args, &block)
        # Factory may return nil! 
        if child.is_a? Cascading::Node
          @children << child
        end
        return child
      end
    end

    def create_sub_nodes(&block)
      if block
        instance_eval(&block)
      end
    end

  end


  # A module to add auto-registration capability.
  module Registerable

    def all
      @registered.nil? ? [] : @registered.values
    end

    def get(key)
      if key.is_a? self
        return key
      else
        @registered ||= {}
        return @registered[key]
      end
    end

    def reset
      @registered.clear if @registered
    end 

    def add(name, instance)
      @registered ||= {}
      @registered[name] = instance
    end  

    private

    def registered
      @registered ||= {}
      @registered
    end
  end


end