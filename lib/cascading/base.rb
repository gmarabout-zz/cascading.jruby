module Cascading
  class BaseFactory
    attr_accessor :block, :name
    
    def initialize(*args, &block)     
      @name = args[0]
      @block = block
      self.class.add(self)
    end
    
    # Creates a registry to the sub class:
    def self.inherited(child)
      child.send(:extend, Cascading::Registerable) 
    end
    
    # Creates the cascading instance.
    def make 
      instance_eval(&@block)
    end
    
  end


  # A module to add auto-registration capability.
  module Registerable
    
    def all
      @registered.nil? ? [] : @registered.values
    end
    
    def get(obj)
      if obj.is_a? self
        return obj
      else
        @registered ||= {}
        return @registered[obj]
      end
    end

    def reset
      @registered.clear if @registered
    end 
    
    def add(instance)
      @registered ||= {}
      @registered[instance.name] = instance
    end  
    
    private
    
    def registered
      @registered ||= {}
      @registered
    end
  end


end