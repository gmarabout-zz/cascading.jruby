# Copyright 2009, Grégoire Marabout. All Rights Reserved.
#
# This is free software. Please see the LICENSE and COPYING files for details.

module Cascading
  class Node
    attr_accessor :name, :parent, :children

    # Makes child Registerable
    def self.inherited(child)
      child.send(:extend, Registerable)
    end

    def initialize(name, parent, &block)
      @name = name
      @parent = parent
      @children = []
      self.class.add(name, self)
      instance_eval(&block) if block
    end

    def add_child(node)
      @children << node
      node
    end
  end

  # A module to add auto-registration capability
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
