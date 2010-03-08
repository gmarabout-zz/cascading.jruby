# operations.rb
#
# Copyright 2009, Grégoire Marabout. All Rights Reserved.
#
# This is free software. Please see the LICENSE and COPYING files for details.

module Cascading
  module Operations

    def identity
      Java::CascadingOperation::Identity.new
    end

    def sum_function(*args)
      options = args.extract_options!
      raise "Need to specify args" if args.empty?
      type = options[:type] || java.lang.Double.java_class
      parameters = [Cascading.fields(args),type].compact.to_java

      Java::CascadingOperationAggregator::Sum.new(*parameters)
    end

    def aggregator_function(args, aggregator_klass)
      options = args.extract_options!
      ignore_values = options[:sql] ? [nil].to_java(java.lang.Object) : nil
      parameters = [Cascading.fields(args), ignore_values].compact
      aggregator_klass.new(*parameters)
    end

    def count_function(*args)
      aggregator_function(args, Java::CascadingOperationAggregator::Count)
    end

    def average_function(*args)
      aggregator_function(args, Java::CascadingOperationAggregator::Average)
    end

    def first_function(*args)
      aggregator_function(args, Java::CascadingOperationAggregator::First)
    end

    def min_function(*args)
      aggregator_function(args, Java::CascadingOperationAggregator::Min)
    end

    def max_function(*args)
      aggregator_function(args, Java::CascadingOperationAggregator::Max)
    end

    def last_function(*args)
      aggregator_function(args, Java::CascadingOperationAggregator::Last)
    end

    def regex_parser(*args)
      options = args.extract_options!
      
      pattern = args[0].to_s
      fields = Cascading.fields(options[:fields])
      groups = options[:groups].to_java(:int) if options[:groups]
      parameters = [fields, pattern, groups].compact

      Java::CascadingOperationRegex::RegexParser.new(*parameters)
    end

    def regex_splitter(*args)
      options = args.extract_options!

      fields = Cascading.fields(args)
      pattern = options[:pattern].to_s
      parameters = [fields, pattern].compact
      Java::CascadingOperationRegex::RegexSplitter.new(*parameters)
    end

    def regex_split_generator(*args)
      options = args.extract_options!

      fields = Cascading.fields(args)
      pattern = options[:pattern].to_s
      parameters = [fields, pattern].compact
      Java::CascadingOperationRegex::RegexSplitGenerator.new(*parameters)
    end

    def expression_function(*args)
      options = args.extract_options!

      fields = Cascading.fields(args)
      expression = options[:expression].to_s
      parameters = options[:parameters]
      parameter_names = []
      parameter_types = []
      if parameters.is_a? ::Hash
        parameters.each do |name, type|
          parameter_names << name
          parameter_types << type
        end
        parameter_names = parameter_names.to_java(java.lang.String)
        parameter_types = parameter_types.to_java(java.lang.Class)

        arguments = [fields, expression, parameter_names, parameter_types].compact
      elsif !parameters.nil?
        arguments = [fields, expression, parameters.java_class].compact
      else
        arguments = [fields, expression, java.lang.String.java_class].compact
      end

      Java::CascadingOperationExpression::ExpressionFunction.new(*arguments)
    end
    
    def insert_function(*args)
      options=args.extract_options!
      fields = Cascading.fields(args)
      values = options[:values] 

      parameters = [fields, to_java_comparable_array(values)].compact
      Java::CascadingOperation::Insert.new(*parameters)
    end

    def to_java_comparable_array(arr)
      (arr.map do |v|
        case v.class
        when Fixnum
          java.lang.Integer.new(v)
        when Float
          java.lang.Double.new(v)
        else
          java.lang.String.new(v.to_s)
        end
      end).to_java(java.lang.Comparable)
    end

    def expression_filter(*args)
      options = args.extract_options!
      expression = (args[0] || options[:expression]).to_s
      parameters = options[:parameters]
      parameter_names = []
      parameter_types = []
      if parameters.is_a? ::Hash
        parameters.each do |name, type|
          parameter_names << name
          parameter_types << type
        end
        parameter_names = parameter_names.to_java(java.lang.String)
        parameter_types = parameter_types.to_java(java.lang.Class)

        arguments = [expression, parameter_names, parameter_types].compact
      elsif !parameters.nil?
        arguments = [expression, parameters.java_class].compact
      else
        arguments = [expression, java.lang.String.java_class].compact
      end

      Java::CascadingOperationExpression::ExpressionFilter.new(*arguments)
    end

    def date_parser(field, format)
      fields = fields(field)
      Java::CascadingOperationText::DateParser.new(fields, format)
    end

    def date_formatter(fields, format, timezone=nil)
      fields = fields(fields)
      timezone = Java::JavaUtil::TimeZone.get_time_zone(timezone) if timezone
      arguments = [fields, format, timezone].compact
      Java::CascadingOperationText::DateFormatter.new(*arguments)
    end

    def regex_filter(*args)
      options = args.extract_options!

      pattern = args[0]
      remove_match = options[:remove_match]
      match_each_element = options[:match_each_element] 
      parameters = [pattern.to_s, remove_match, match_each_element].compact
      Java::CascadingOperationRegex::RegexFilter.new(*parameters)
    end

    def regex_replace(*args)
      options = args.extract_options!

      fields = fields(args[0])
      pattern = args[1]
      replacement = args[2]
      replace_all = options[:replace_all]

      parameters = [fields, pattern.to_s, replacement.to_s, replace_all].compact
      Java::CascadingOperationRegex::RegexReplace.new(*parameters)
    end

  end
end
