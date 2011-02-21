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
    def join(node, *args, &block)
      options = args.extract_options!
      
      pipes, incoming_scopes = [], []
      args.each do |assembly|
        # a string instead of an Assembly variable could be used :-)
        assembly_name = assembly
        assembly = Assembly.get(assembly)
        raise "Could not find assembly #{assembly_name}" unless assembly
        pipes << assembly.tail_pipe
        incoming_scopes << node.outgoing_scopes[assembly.name]
      end
      
      group_fields_args = options.delete(:on)
      if group_fields_args.is_a? ::String
        group_fields_args = [group_fields_args]
      end
      group_fields_names = group_fields_args.to_a
      group_fields = []
      if group_fields_args.is_a? ::Array
        pipes.size.times do 
          group_fields << fields(group_fields_args)
        end
      elsif group_fields_args.is_a? ::Hash
        pipes, incoming_scopes = [], []
        keys = group_fields_args.keys.sort
        keys.each do |k|          
          v = group_fields_args[k]
          assembly = Assembly.get(k)
          pipes << assembly.tail_pipe
          incoming_scopes << node.outgoing_scopes[assembly.name]
          group_fields << fields(v)
          group_fields_names = group_fields_args[keys.first].to_a
        end
      end

      group_fields = group_fields.to_java(Java::CascadingTuple::Fields)
      incoming_fields = incoming_scopes.map{ |s| s.values_fields }
      declared_fields = fields(options[:declared_fields] || dedup_fields(*incoming_fields))
      joiner = options.delete(:joiner)
      
      if declared_fields
        case joiner
        when :inner, "inner", nil
          joiner = Java::CascadingPipeCogroup::InnerJoin.new
        when :left,  "left"
          joiner = Java::CascadingPipeCogroup::LeftJoin.new
        when :right, "right"
          joiner = Java::CascadingPipeCogroup::RightJoin.new
        when :outer, "outer"
          joiner = Java::CascadingPipeCogroup::OuterJoin.new
        when Array
          joiner = joiner.map do |t|
            case t
            when true,  1, :inner then true
            when false, 0, :outer then false
            else fail "invalid mixed joiner entry: #{t}"
            end
          end
          joiner = Java::CascadingPipeCogroup::MixedJoin.new(joiner.to_java(:boolean))
        end
      end
  
      parameters = [pipes.to_java(Java::CascadingPipe::Pipe), group_fields, declared_fields, joiner].compact
      grouping_key_fields = group_fields[0] # Left key group wins
      node.make_pipe(Java::CascadingPipe::CoGroup, parameters, grouping_key_fields, incoming_scopes)
      node.do_every_block_and_rename_fields(group_fields_names, incoming_scopes, &block)
    end
    
    def inner_join(node, *args, &block)
      options = args.extract_options!
      options[:joiner] = :inner
      args << options
      join(node, *args, &block)
    end

    def left_join(node, *args, &block)
      options = args.extract_options!
      options[:joiner] = :left
      args << options
      join(node, *args, &block)
    end

    def right_join(node, *args, &block)
      options = args.extract_options!
      options[:joiner] = :right
      args << options
      join(node, *args, &block)
    end

    def outer_join(node, *args, &block)
      options = args.extract_options!
      options[:joiner] = :outer
      args << options
      join(node, *args, &block)
    end
    
    # Builds a new branch. The name of the branch is specified as first item in args array.
    def branch(node, *args, &block)
      Cascading::Assembly.new(args[0], node, node.outgoing_scopes, &block)
    end

    # Builds a new _group_by_ pipe. The fields used for grouping are specified in the args
    # array.
    def group_by(node, *args, &block)
      options = args.extract_options!

      group_fields = Cascading.fields(args) 

      sort_fields = Cascading.fields(options[:sort_by] || args)
      reverse = options[:reverse]

      parameters = [node.tail_pipe, group_fields, sort_fields, reverse].compact
      node.make_pipe(Java::CascadingPipe::GroupBy, parameters, group_fields)
      node.do_every_block_and_rename_fields(args, [node.scope], &block)
    end

    # Unifies several pipes sharing the same field structure.
    # This actually creates a GroupBy pipe.
    # It expects a list of assemblies as parameter. 
    def union_pipes(node, *args)
      pipes, incoming_scopes = [], []
      args[0].each do |pipe|
        assembly = Assembly.get(pipe)
        pipes << assembly.tail_pipe
        incoming_scopes << node.outgoing_scopes[assembly.name]
      end

      # Groups only on the 1st field (see line 186 of GroupBy.java)
      grouping_key_fields = Cascading.fields(incoming_scopes.first.values_fields.get(0))
      node.make_pipe(Java::CascadingPipe::GroupBy, [pipes.to_java(Java::CascadingPipe::Pipe)], grouping_key_fields, incoming_scopes)
      # TODO: Shouldn't union_pipes accept an every block?
      #node.do_every_block_and_rename_fields(args, incoming_scopes, &block)
    end

    # Builds an basic _every_ pipe, and adds it to the current assembly.
    def every(node, *args)
      options = args.extract_options!

      in_fields = Cascading.fields(args)
      out_fields = Cascading.fields(options[:output])
      operation = options[:aggregator] || options[:buffer]

      parameters = [node.tail_pipe, in_fields, operation, out_fields].compact
      node.make_every(Java::CascadingPipe::Every, *parameters)
    end

    # Builds a basic _each_ pipe, and adds it to the current assembly.
    # --
    # Example:
    #     each "line", :filter=>regex_splitter(["name", "val1", "val2", "id"], 
    #                  :pattern => /[.,]*\s+/), 
    #                  :output=>["id", "name", "val1", "val2"] 
    def each(node, *args)
      options = args.extract_options!

      in_fields = Cascading.fields(args)  
      out_fields = Cascading.fields(options[:output]) 
      operation = options[:filter] || options[:function]

      parameters = [node.tail_pipe, in_fields, operation, out_fields].compact
      node.make_each(Java::CascadingPipe::Each, *parameters)
    end   

    # Restricts the current assembly to the specified fields.
    # --
    # Example:
    #     project "field1", "field2"
    def project(node, *args)
      fields = Cascading.fields(args)
      operation = Java::CascadingOperation::Identity.new
      node.make_each(Java::CascadingPipe::Each, node.tail_pipe, fields, operation)
    end

    # Removes the specified fields from the current assembly.
    # --
    # Example:
    #     discard "field1", "field2"
    def discard(node, *args)
      discard_fields = Cascading.fields(args)
      keep_fields = difference_fields(node.scope.values_fields, discard_fields)
      project(node, *keep_fields.to_a)
    end

    # Assign new names to initial fields in positional order.
    # --
    # Example:
    #     bind_names "field1", "field2"
    def bind_names(node, *new_names)
      new_fields = Cascading.fields(new_names)
      operation = Java::CascadingOperation::Identity.new(new_fields)
      node.make_each(Java::CascadingPipe::Each, node.tail_pipe, all_fields, operation) 
    end

    # Renames fields according to the mapping provided.
    # --
    # Example:
    #     rename "old_name" => "new_name"
    def rename(node, name_map)
      old_names = node.scope.values_fields.to_a
      new_names = old_names.map{ |name| name_map[name] || name }
      invalid = name_map.keys.sort - old_names
      raise "invalid names: #{invalid.inspect}" unless invalid.empty?

      old_key = node.scope.primary_key_fields.to_a
      new_key = old_key.map{ |name| name_map[name] || name }

      new_fields = Cascading.fields(new_names)
      operation = Java::CascadingOperation::Identity.new(new_fields)
      node.make_each(Java::CascadingPipe::Each, node.tail_pipe, all_fields, operation) 
      node.primary(*new_key)
    end

    def cast node, type_map
      names = type_map.keys.sort
      types = JAVA_TYPE_MAP.values_at(*type_map.values_at(*names))
      fields = Cascading.fields(names)
      types = types.to_java(java.lang.Class)
      operation = Java::CascadingOperation::Identity.new(fields, types)
      node.make_each(Java::CascadingPipe::Each, node.tail_pipe, fields, operation)
    end

    def copy(node, *args)
      options = args.extract_options!
      from = args[0] || all_fields
      into = args[1] || options[:into] || all_fields
      operation = Java::CascadingOperation::Identity.new(Cascading.fields(into))
      node.make_each(Java::CascadingPipe::Each, node.tail_pipe, Cascading.fields(from), operation, Java::CascadingTuple::Fields::ALL)
    end

    # A pipe that does nothing.
    def pass(node, *args)
      operation = Java::CascadingOperation::Identity.new 
      node.make_each(Java::CascadingPipe::Each, node.tail_pipe, all_fields, operation)
    end

    def assert(node, *args)
      options = args.extract_options!
      assertion = args[0]
      assertion_level = options[:level] || Java::CascadingOperation::AssertionLevel::STRICT
      node.make_each(Java::CascadingPipe::Each, node.tail_pipe, assertion_level, assertion)
    end

    def assert_group(node, *args)
      options = args.extract_options!
      assertion = args[0]
      assertion_level = options[:level] || Java::CascadingOperation::AssertionLevel::STRICT
      node.make_every(Java::CascadingPipe::Every, node.tail_pipe, assertion_level, assertion)
    end
    
    alias co_group join

  end # class AssemblyFactory

  class Assembly < Cascading::Node

    include Cascading::Operations
    include Cascading::PipeHelpers

    attr_accessor :tail_pipe, :head_pipe, :outgoing_scopes, :scope

    def initialize(name, parent=nil, outgoing_scopes={}, &block)
      @every_applied = false
      @outgoing_scopes = outgoing_scopes
      if parent
        @head_pipe = Java::CascadingPipe::Pipe.new(name, parent.tail_pipe)
        # Copy to allow destructive update of name
        @scope = parent.scope.copy
        @scope.scope.name = name
      else
        @head_pipe = Java::CascadingPipe::Pipe.new(name)
        @scope = @outgoing_scopes[name] || Scope.empty_scope(name)
      end
      @tail_pipe = @head_pipe

      super(name, parent, &block)

      # Record outgoing scope
      @outgoing_scopes[name] = @scope
    end

    def debug_scope
      puts "Current scope for '#{name}':\n  #{@scope}\n----------\n"
    end

    def primary(*args)
      options = args.extract_options!
      if args.size > 0 && args[0] != nil
        @scope.primary_key_fields = Cascading.fields(args)
      else
        @scope.primary_key_fields = nil
      end
      @scope.grouping_primary_key_fields = @scope.primary_key_fields
    end

    def make_each(type, *parameters)
      make_pipe(type, parameters)
      @every_applied = false
    end

    def make_every(type, *parameters)
      make_pipe(type, parameters, @scope.grouping_key_fields)
      @every_applied = true
    end

    def every_applied?
      @every_applied
    end

    def do_every_block_and_rename_fields(group_fields, incoming_scopes, &block)
      return unless block

      # TODO: this should really be instance evaled on an object
      # that only allows aggregation and buffer operations.
      instance_eval &block

      # First all non-primary key fields from each pipe if its primary key is a
      # subset of the grouping primary key
      first_fields = incoming_scopes.map do |scope|
        if scope.primary_key_fields
          primary_key = scope.primary_key_fields.to_a
          grouping_primary_key = @scope.grouping_primary_key_fields.to_a
          if (primary_key & grouping_primary_key) == primary_key
            difference_fields(scope.values_fields, scope.primary_key_fields).to_a
          end
        end
      end.compact.flatten
      # assert first_fields == first_fields.uniq

      # Do no first any fields explicitly aggregated over
      first_fields = first_fields - @scope.grouping_fields.to_a
      if first_fields.size > 0
        first *first_fields
        puts "Firsting: #{first_fields.inspect} in assembly: #{@name}"
      end

      bind_names @scope.grouping_fields.to_a if every_applied?
    end

    def make_pipe(type, parameters, grouping_key_fields = [], incoming_scopes = [@scope])
      @tail_pipe = type.new(*parameters)
      @scope = Scope.outgoing_scope(@tail_pipe, incoming_scopes, grouping_key_fields, every_applied?)
    end

    def to_s
      "#{@name} : head pipe : #{@head_pipe} - tail pipe: #{@tail_pipe}"
    end
  end
end
