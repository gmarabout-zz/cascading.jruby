# assembly.rb
#
# Copyright 2009, Grégoire Marabout. All Rights Reserved.
#
# This is free software. Please see the LICENSE and COPYING files for details.


require 'cascading/base'
require 'cascading/operations'
require 'cascading/helpers'
require 'cascading/ext/array'

module Cascading

  class AssemblyFactory 
    
    
    # Builds a join (CoGroup) pipe.
    def join(node, *args)
      options = args.extract_options!
      
      pipes = []
      args.each do |assembly|
        # a string instead of an Assembly variable could be used :-)
        assembly = Assembly.get(assembly)
        pipes << assembly.tail_pipe
      end
      
      group_fields_args = options.delete(:group_fields)
      group_fields = []
      if group_fields_args.is_a? ::Array
        pipes.size.times do 
          group_fields << fields(group_fields_args)
        end
      elsif group_fields_args.is_a? ::Hash
        pipes = []
        group_fields_args.each do |k, v|          
          pipes << Assembly.get(k).tail_pipe
          group_fields << fields(v)
        end
      end
         
      group_fields = group_fields.to_java(Java::CascadingTuple::Fields)
       
      declared_fields = options[:declared_fields] 
      joiner = options.delete(:joiner)

      declared_fields = fields(declared_fields)
      
      if declared_fields && joiner.nil?
        joiner = Java::CascadingPipeCogroup::InnerJoin.new
      end
  
      parameters = [pipes.to_java(Java::CascadingPipe::Pipe), group_fields, declared_fields, joiner].compact    
      node.make_pipe(Java::CascadingPipe::CoGroup, *parameters)
    end
    
    # Builds a new branch. The name of the branch is specified as first item in args array.
    def branch(node, *args, &block)
      name = args[0]
      new_node = Cascading::Assembly.new(name, node, &block)
      new_node
    end

    # Builds a new _group_by_ pipe. The fields used for grouping are specified in the args
    # array.
    def group_by(node, *args)
      # puts "Create group by pipe"
      options = args.extract_options!

      group_fields = Cascading.fields(args) 

      sort_fields = Cascading.fields(options[:sort_by] || args)
      reverse = options[:reverse]

      parameters = [node.tail_pipe, group_fields, sort_fields, reverse].compact
      node.make_pipe(Java::CascadingPipe::GroupBy, *parameters)
    end

    # Unifies several pipes sharing the same field structure.
    # This actually creates a GroupBy pipe.
    # It expects a list of assemblies as parameter. 
    def union_pipes(node, *args)
      pipes = args[0].map do |pipe|
        assembly = Assembly.get(pipe)
        assembly.tail_pipe
      end

      node.make_pipe(Java::CascadingPipe::GroupBy, pipes.to_java(Java::CascadingPipe::Pipe))
    end

    # Builds an basic _every_ pipe, and adds it to the current assembly.
    def every(node, *args)     
      # puts "Create every pipe" 
      options = args.extract_options!

      in_fields = Cascading.fields(args) 
      out_fields = Cascading.fields(options[:output])
      operation = options[:aggregator] || options[:buffer] 

      parameters = [node.tail_pipe, in_fields, operation, out_fields].compact
      node.make_pipe(Java::CascadingPipe::Every, *parameters)   
    end

    # Builds a basic _each_ pipe, and adds it to the current assembly.
    # --
    # Example:
    #     each "line", :filter=>regex_splitter(["name", "val1", "val2", "id"], 
    #                  :pattern => /[.,]*\s+/), 
    #                  :output=>["id", "name", "val1", "val2"] 
    def each(node, *args)
      # puts "Create each pipe"
      options = args.extract_options!

      in_fields = Cascading.fields(args)  
      out_fields = Cascading.fields(options[:output]) 
      operation = options[:filter] || options[:function] 

      parameters = [node.tail_pipe, in_fields, operation, out_fields].compact
      node.make_pipe(Java::CascadingPipe::Each, *parameters)
    end   

    # Restricts the current assembly to the specified fields.
    # --
    # Example:
    #     restrict_to "field1", "field2"
    def restrict_to(node, *args)
      operation = Java::CascadingOperation::Identity.new() 
      node.make_pipe(Java::CascadingPipe::Each, node.tail_pipe, Cascading.fields(args), operation)
    end

    # Renames the first list of fields to the second one.
    def rename(node, *args)
      old_names = args[0]
      new_names = args[1]
      operation = Java::CascadingOperation::Identity.new(Cascading.fields(new_names))
      
      node.make_pipe(Java::CascadingPipe::Each, node.tail_pipe, Cascading.fields(old_names), operation) 
  
    end

    def copy(node, *args)
      options = args.extract_options!
      from = args[0] || all_fields
      into = args[1] || options[:into] || all_fields
      operation = Java::CascadingOperation::Identity.new(Cascading.fields(into))
      node.make_pipe(Java::CascadingPipe::Each, node.tail_pipe, Cascading.fields(from), operation, Java::CascadingTuple::Fields::ALL)
    end

    # A pipe that does nothing.
    def pass(node, *args)
      operation = Java::CascadingOperation::Identity.new 
      node.make_pipe(Java::CascadingPipe::Each, all_fields, operation)
    end

    def assert(node, *args)
      options = args.extract_options!
      assertion = args[0]
      assertion_level = options[:level] || Java::CascadingOperation::AssertionLevel::STRICT
      node.make_pipe(Java::CascadingPipe::Each, node.tail_pipe, assertion_level, assertion)
    end
    
    
    alias co_group join

  end # class Assembly


  class Assembly < Cascading::Node

    include Cascading::Operations    
    include Cascading::PipeHelpers

    attr_accessor :tail_pipe, :head_pipe    

    def initialize(name, parent=nil, &block)
      if (parent)
        @head_pipe = Java::CascadingPipe::Pipe.new(name, parent.tail_pipe)
      else
        @head_pipe = Java::CascadingPipe::Pipe.new(name)
      end
      @tail_pipe = @head_pipe
      super
    end
    
    def make_pipe(type, *parameters) 
      @tail_pipe = type.new(*parameters)
    end

    def to_s
      "#{@name} : head pipe : #{@head_pipe} - tail pipe: #{@tail_pipe}"
    end

  end

end