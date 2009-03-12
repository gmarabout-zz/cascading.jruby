require 'cascading/base'
require 'cascading/operations'
require 'cascading/helpers'
require 'cascading/ext/array'

module Cascading

  class AssemblyFactory 
     
        
    def branch(node, *args, &block)
      name = args[0]
      new_node = Cascading::Assembly.new(name, node, &block)
      new_node
    end

    def group_by(node, *args)
      # puts "Create group by pipe"
      options = args.extract_options!
      
      group_fields = Cascading.fields(args) 
      
      sort_fields = Cascading.fields(options[:sort_by] || args)
      reverse = options[:reverse]

      parameters = [group_fields, sort_fields, reverse].compact
      node.new_pipe(Java::CascadingPipe::GroupBy, *parameters)
    end
    
    def union_pipes(node, *args)
      pipes = args[0].map do |pipe|
        #puts pipe.class
        pipe.tail_pipe
      end
      
      node.tail_pipe = Java::CascadingPipe::GroupBy.new(pipes.to_java(Java::CascadingPipe::Pipe))
    end

    def every(node, *args)     
      # puts "Create every pipe" 
      options = args.extract_options!

      in_fields = Cascading.fields(args) 
      out_fields = Cascading.fields(options[:output])
      operation = options[:aggregator] || options[:buffer] 

      parameters = [in_fields, operation, out_fields].compact
      node.new_pipe(Java::CascadingPipe::Every, *parameters)   
    end

    def each(node, *args)
      # puts "Create each pipe"
      options = args.extract_options!

      in_fields = Cascading.fields(args)  
      out_fields = Cascading.fields(options[:output]) 
      operation = options[:filter] || options[:function] 

      parameters = [in_fields, operation, out_fields].compact
      node.new_pipe(Java::CascadingPipe::Each, *parameters)
    end   

    def co_group(node, *args)
      raise "not implemented yet"
    end

    # Keeps only the specified fields in the assembly:
    def restrict_to(node, *args)
      operation = Java::CascadingOperation::Identity.new() 
      node.new_pipe(Java::CascadingPipe::Each, Cascading.fields(args), operation)
    end
    
    def rename(node, *args)
      old_names = args[0]
      new_names = args[1]
      operation = Java::CascadingOperation::Identity.new(Cascading.fields(new_names))
      node.new_pipe(Java::CascadingPipe::Each, Cascading.fields(old_names), operation, Cascading.fields(new_names))
    end

    def copy(node, *args)
      options = args.extract_options!
      from = args[0] || all_fields
      into = args[1] || options[:into] || all_fields
      operation = Java::CascadingOperation::Identity.new(Cascading.fields(into))
      node.new_pipe(Java::CascadingPipe::Each, Cascading.fields(from), operation, Java::CascadingTuple::Fields::ALL)
    end

    # A pipe that does nothing.
    def pass(node, *args)
      operation = Java::CascadingOperation::Identity.new 
      node.new_pipe(Java::CascadingPipe::Each, all_fields, operation)
    end
    
    
    def assert(node, *args)
      options = args.extract_options!
      
      assertion = args[0]
      assertion_level = options[:level] || Java::CascadingOperation::AssertionLevel::STRICT

      node.new_pipe(Java::CascadingPipe::Each, assertion_level, assertion)
    end

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
    
    def new_pipe(type, *parameters)
      #puts "Creating new pipe : #{type} with parameters: #{parameters}"
      @tail_pipe = type.new(@tail_pipe , *parameters)
    end
        
    def to_s
      "#{@name} : head pipe : #{@head_pipe} - tail pipe: #{@tail_pipe}"
    end
    
  end
  
end