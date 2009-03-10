require 'cascading/base'
require 'cascading/operations'
require 'cascading/helpers'

module Cascading

  class AssemblyFactory < Cascading::BaseFactory

    include Cascading::Operations     
    include Cascading::PipeHelpers

    attr_accessor :parent, :children, :head

    def initialize(*args, &block)
      super 
      options = args.extract_options
      @parent = options[:parent]
      @children = []
      parameters = [@name.to_s, @parent].compact
      @pipe = @head = Java::CascadingPipe::Pipe.new(*parameters)
    end

    # Make this assembly. Returns an array of pipes (instances of cascading.pipe.Pipe)
    def make()
      # puts "Making assembly #{@name}..."
      super
      pipes = [@pipe]
      @children.each do |child|
        pipes += child.make
      end
      pipes
    end

    def branch(name, &block)
      # puts "Making branch #{@name}..."
      assembly = AssemblyFactory.new(name, pipe, &block)
      assembly.parent = pipe
      @children << assembly
    end

    def group_by(*args)
      # puts "Create group by pipe"
      options = args.extract_options!

      group_fields = Cascading.fields(args) 
      sort_fields = Cascading.fields(options[:sort_by] || args)
      reverse = options[:reverse]

      parameters = [@pipe, group_fields, sort_fields, reverse].compact
      @pipe = Java::CascadingPipe::GroupBy.new(*parameters)
      @pipe
    end

    def every(*args)     
      # puts "Create every pipe" 
      options = args.extract_options!

      in_fields = Cascading.fields(args) 
      out_fields = Cascading.fields(options[:output])
      operation = options[:aggregator] || options[:buffer] 

      parameters = [@pipe, in_fields, operation, out_fields].compact
      @pipe = Java::CascadingPipe::Every.new(*parameters)   
      @pipe
    end

    def each(*args)
      # puts "Create each pipe"
      options = args.extract_options!

      in_fields = Cascading.fields(args)  
      out_fields = Cascading.fields(options[:output]) 
      operation = options[:filter] || options[:function] 

      parameters = [@pipe, in_fields, operation, out_fields].compact
      @pipe = Java::CascadingPipe::Each.new(*parameters)
      @pipe
    end   

    def co_group(*args)
      raise "not implemented yet"
    end

    def branch(name, &block)
      branch = Cascading::AssemblyFactory.new(name, :parent => @pipe, &block)
      children << branch
    end

    # Keeps only the specified fields in the assembly:
    def project(*fields)
      operation = Java::CascadingOperation::Identity.new 
      @pipe = Java::CascadingPipe::Each.new(@pipe, Cascading.fields(fields), operation)
      @pipe
    end
    
    # Deprecated. Use project instead.
    def keep_only(*fields)
      project(*fields)
    end

    def rename(old_names, new_names)
      operation =  Java::CascadingOperation::Identity.new(Cascading.fields(new_names))
      @pipe = Java::CascadingPipe::Each.new(@pipe, Cascading.fields(old_names), operation, Cascading.fields(new_names))
      @pipe
    end

    def copy(*args)
      options = args.extract_options!
      from = args[0] || all_fields
      into = args[1] || options[:into] || all_fields
      operation = Java::CascadingOperation::Identity.new(Cascading.fields(into))
      @pipe = Java::CascadingPipe::Each.new(@pipe, Cascading.fields(from), operation, Java::CascadingTuple::Fields::ALL)
      @pipe
    end

    # A pipe that does nothing.
    def pass
      operation = Java::CascadingOperation::Identity.new 
      @pipe = Java::CascadingPipe::Each.new(@pipe, all_fields, operation)
      @pipe
    end

  end # class Assembly
end