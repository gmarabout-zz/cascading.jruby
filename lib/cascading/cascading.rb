# Copyright 2009, Grégoire Marabout. All Rights Reserved.
#
# This is free software. Please see the LICENSE and COPYING files for details.

require 'cascading/expr_stub'

module Cascading
  JAVA_TYPE_MAP = {
    :int => java.lang.Integer.java_class, :long => java.lang.Long.java_class,
    :bool => java.lang.Boolean.java_class, :double => java.lang.Double.java_class,
    :float => java.lang.Float.java_class, :string => java.lang.String.java_class,
  }

  def cascade(name, &block)
    raise "Could not build cascade '#{name}'; block required" unless block_given?
    cascade = Cascade.new(name)
    cascade.instance_eval(&block)
    cascade
  end

  # For applications built of Flows with no Cascades
  def flow(name, &block)
    flow = Flow.new(name, nil)
    flow.instance_eval(&block)
    flow
  end

  def expr(s)
    return s if s.kind_of?(ExprStub)
    ExprStub.new(s)
  end

  # Creates a cascading.tuple.Fields instance from a string or an array of strings.
  def fields(fields)
    if fields.nil?
      return nil
    elsif fields.is_a? Java::CascadingTuple::Fields
      return fields
    elsif fields.is_a? ::Array
      if fields.size == 1
        return fields(fields[0])
      end
      raise "Fields cannot be nil: #{fields.inspect}" if fields.include?(nil)
    end
    return Java::CascadingTuple::Fields.new([fields].flatten.map{ |f| f.kind_of?(Fixnum) && JRUBY_VERSION > '1.2.0' ? f.to_java(:int) : f }.to_java(java.lang.Comparable))
  end

  def all_fields
    Java::CascadingTuple::Fields::ALL
  end

  def union_fields(*fields)
    fields(fields.inject([]){ |acc, arr| acc | arr.to_a })
  end

  def difference_fields(*fields)
    fields(fields[1..-1].inject(fields.first.to_a){ |acc, arr| acc - arr.to_a })
  end

  def copy_fields(fields)
    fields.select(all_fields)
  end

  def dedup_fields(*fields)
    raise 'Can only be applied to declarators' unless fields.all?{ |f| f.is_declarator? }
    fields(dedup_field_names(*fields.map{ |f| f.to_a }))
  end

  def dedup_field_names(*names)
    names.inject([]) do |acc, arr|
      acc + arr.map{ |e| search_field_name(acc, e) }
    end
  end

  def search_field_name(names, candidate)
    names.include?(candidate) ? search_field_name(names, "#{candidate}_") : candidate
  end

  def last_grouping_fields
    Java::CascadingTuple::Fields::VALUES
  end

  def results_fields
    Java::CascadingTuple::Fields::RESULTS
  end

  # Creates a c.s.TextLine scheme instance from the specified fields.
  def text_line_scheme(*fields)
    unless fields.empty?
      fields = fields(fields)
      return Java::CascadingScheme::TextLine.new(fields)
    else
      return Java::CascadingScheme::TextLine.new
    end
  end

  # Creates a c.s.SequenceFile scheme instance from the specified fields.
  def sequence_file_scheme(*fields)
    unless fields.empty?
      fields = fields(fields)
      return Java::CascadingScheme::SequenceFile.new(fields)
    else
      return Java::CascadingScheme::SequenceFile.new(all_fields)
    end
  end

  def multi_tap(*taps)
    Java::CascadingTap::MultiTap.new(taps.to_java("cascading.tap.Tap"))
  end

  # Generic method for creating taps.
  # It expects a ":kind" argument pointing to the type of tap to create.
  def tap(*args)
    opts = args.extract_options!
    path = args.empty? ? opts[:path] : args[0]
    scheme = opts[:scheme] || text_line_scheme
    sink_mode = opts[:sink_mode] || :keep
    sink_mode = case sink_mode
      when :keep, 'keep'       then Java::CascadingTap::SinkMode::KEEP
      when :replace, 'replace' then Java::CascadingTap::SinkMode::REPLACE
      when :append, 'append'   then Java::CascadingTap::SinkMode::APPEND
      else raise "Unrecognized sink mode '#{sink_mode}'"
    end
    fs = opts[:kind] || :hfs
    klass = case fs
      when :hfs, 'hfs' then Java::CascadingTap::Hfs
      when :dfs, 'dfs' then Java::CascadingTap::Dfs
      when :lfs, 'lfs' then Java::CascadingTap::Lfs
      else raise "Unrecognized kind of tap '#{fs}'"
    end
    parameters = [scheme, path, sink_mode]
    klass.new(*parameters)
  end
end
