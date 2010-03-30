# helpers.rb 
#
# Copyright 2009, Grégoire Marabout. All Rights Reserved.
#
# This is free software. Please see the LICENSE and COPYING files for details.

module Cascading
  unless defined?(JAVA_TYPE_MAP)
    JAVA_TYPE_MAP = { :int => java.lang.Integer.java_class, :long => java.lang.Long.java_class,
                      :bool => java.lang.Boolean.java_class, :double => java.lang.Double.java_class,
                      :float => java.lang.Float.java_class, :string => java.lang.String.java_class }
  end
  
  # Module PipeHelpers.
  # This module is mixed-in the class Cascading::Assembly to provide shorthands of current pipe configurations.
  # 
  # Author:: Grégoire Marabout <gmarabout@gmail.com>
  module PipeHelpers

    class ExprStub
      attr_accessor :expression, :types

      def initialize(st) 
        @expression = st.dup
        @types = {}

        # Simple regexp based parser for types

        JAVA_TYPE_MAP.each do |sym, klass|
          @expression.gsub!(/[A-Za-z0-9_]+:#{sym.to_s}/) do |match|
            name = match.split(/:/).first.gsub(/\s+/, "")
            @types[name] = klass
            match.gsub(/:#{sym.to_s}/, "")
          end
        end
      end

      def self.split_hash(h)
        keys, values = h.keys.sort, []
        keys.each do |key|
          values << h[key]
        end
        [keys, values]
      end

      def self.split_names_and_types(expr_types)
        names, types = split_hash(expr_types)
        names = names.to_java(java.lang.String)
        types = types.to_java(java.lang.Class)
        [names, types]
      end
    end

    def expr(s)
      ExprStub.new(s)
    end
    
    # Builds a debugging pipe. 
    # 
    # Without arguments, it generate a simple debug pipe, that prints all tuple to the standard
    # output. 
    #
    # The other named options are:
    # * <tt>:print_fields</tt> a boolean. If is set to true, then it prints every 10 tuples. 
    # 
    def debug(*args)
      options = args.extract_options!
      print_fields = options[:print_fields] || true
      parameters = [print_fields].compact
      debug = Java::CascadingOperation::Debug.new(*parameters)
      debug.print_tuple_every = options[:tuple_interval] || 1
      debug.print_fields_every = options[:fields_interval] || 10
      each(all_fields, :filter => debug)
    end
    
    # Builds a pipe that assert the size of the tuple is the size specified in parameter.
    #
    # The method accept an unique uname argument : a number indicating the size expected.
    def assert_size_equals(*args)
      options = args.extract_options!
      assertion = Java::CascadingOperationAssertion::AssertSizeEquals.new(args[0])      
      assert(assertion, options)
    end

    # Builds a pipe that assert the none of the fields in the tuple are null.
    def assert_not_null(*args)
      options = args.extract_options!
      assertion = Java::CascadingOperationAssertion::AssertNotNull.new()      
      assert(assertion, options)
    end

    def assert_group_size_equals(*args)
      options = args.extract_options!
      assertion = Java::CascadingOperationAssertion::AssertGroupSizeEquals.new(args[0])
      assert_group(assertion, options)
    end

    # Builds a series of every pipes for aggregation.
    # 
    # Args can either be a list of fields to aggregate and an options hash or
    # a hash that maps input field name to output field name (similar to
    # insert) and an options hash.
    #
    # Options include:
    #   * <tt>:sql</tt> a boolean indicating whether the operation should act like the SQL equivalent
    #
    # <tt>function</tt> is a symbol that is the method to call to construct the Cascading Aggregator.
    def composite_aggregator(args, function)
      if !args.empty? && args.first.kind_of?(Hash)
        field_map = args.shift.sort
        options = args.extract_options!
      else
        options = args.extract_options!
        field_map = args.zip(args)
      end
      field_map.each do |in_field, out_field|
        agg = self.send(function, out_field, options)
        every(in_field, :aggregator => agg, :output => all_fields)
      end
      puts "WARNING: composite aggregator '#{function.to_s.gsub('_function', '')}' invoked on 0 fields; will be ignored" if field_map.empty?
    end

    def min(*args); composite_aggregator(args, :min_function); end
    def max(*args); composite_aggregator(args, :max_function); end
    def first(*args); composite_aggregator(args, :first_function); end
    def last(*args); composite_aggregator(args, :last_function); end
    def average(*args); composite_aggregator(args, :average_function); end

    # Counts elements of a group.  First unnamed parameter is the name of the
    # output count field (defaults to 'count' if it is not provided).
    def count(*args)
      options = args.extract_options!
      name = args[0] || 'count'
      every(last_grouping_fields, :aggregator => count_function(name, options), :output => all_fields)
    end

    # Fields to be summed may either be provided as an array, in which case
    # they will be aggregated into the same field in the given order, or as a
    # hash, in which case they will be aggregated from the field named by the
    # key into the field named by the value after being sorted.
    def sum(*args)
      options = args.extract_options!
      type = JAVA_TYPE_MAP[options[:type]]
      raise "No type specified for sum" unless type

      mapping = options[:mapping] ? options[:mapping].sort : args.zip(args)
      mapping.each do |in_field, out_field|
        every(in_field, :aggregator => sum_function(out_field, :type => type), :output => all_fields)
      end
    end

    # Builds a _parse_ pipe. This pipe will parse the fields specified in input (first unamed arguments),
    # using a specified regex pattern.
    #
    # If provided, the unamed arguments must be the fields to be parsed. If not provided, then all incoming
    # fields are used.
    #
    # The named options are:
    # * <tt>:pattern</tt> a string or regex. Specifies the regular expression used for parsing the argument fields. 
    # * <tt>:output</tt> a string or array of strings. Specifies the outgoing fields (all fields will be output by default)
    def parse(*args)
        options = args.extract_options!
        fields = args || all_fields
        pattern = options[:pattern]
        output = options[:output] || all_fields
        each(fields, :filter => regex_parser(pattern, options), :output => output)
    end

    # Builds a pipe that splits a field into other fields, using a specified regular expression. 
    #
    # The first unamed argument is the field to be splitted. 
    # The second unamed argument is an array of strings indicating the fields receiving the result of the split. 
    #
    # The named options are:
    # * <tt>:pattern</tt> a string or regex. Specifies the regular expression used for splitting the argument fields. 
    # * <tt>:output</tt> a string or array of strings. Specifies the outgoing fields (all fields will be output by default)
    def split(*args)
      options = args.extract_options!
      fields = options[:into] || args[1]
      pattern = options[:pattern] || /[.,]*\s+/
      output = options[:output] || all_fields
      each(args[0], :filter => regex_splitter(fields, :pattern => pattern), :output=>output)
    end
    
    # Builds a pipe that parses the specified field as a date using hte provided format string.
    # The unamed argument specifies the field to format.
    #
    # The named options are:
    # * <tt>:into</tt> a string. It specifies the receiving field. By default, it will be named after
    # the input argument.
    # * <tt>:pattern</tt> a string. Specifies the date format.
    # * <tt>:output</tt> a string or array of strings. Specifies the outgoing fields (all fields will be output by default)
    def parse_date(*args)
      options = args.extract_options!
      field = options[:into] || "#{args[0]}_parsed"
      output = options[:output] || all_fields
      pattern = options[:pattern] || "yyyy/MM/dd"

      each args[0], :function => date_parser(field, pattern), :output => output
    end

    # Builds a pipe that format a date using a specified format pattern.
    #
    # The unamed argument specifies the field to format.
    #
    # The named options are:
    # * <tt>:into</tt> a string. It specifies the receiving field. By default, it will be named after
    # the input argument.
    # * <tt>:pattern</tt> a string. Specifies the date format.
    # * <tt>:timezone</tt> a string.  Specifies the timezone (defaults to UTC).
    # * <tt>:output</tt> a string or array of strings. Specifies the outgoing fields (all fields will be output by default)
    def format_date(*args)
      options = args.extract_options!
      field = options[:into] || "#{args[0]}_formatted"
      pattern = options[:pattern] || "yyyy/MM/dd"
      output = options[:output] || all_fields

      each args[0], :function => date_formatter(field, pattern, options[:timezone]), :output => output
    end

    # Builds a pipe that perform a query/replace based on a regular expression.
    #
    # The first unamed argument specifies the input field.
    # 
    # The named options are:
    # * <tt>:pattern</tt> a string or regex. Specifies the pattern to look for in the input field. This non-optional argument
    # can also be specified as a second _unamed_ argument.
    # * <tt>:replacement</tt> a string. Specifies the replacement.
    # * <tt>:output</tt> a string or array of strings. Specifies the outgoing fields (all fields will be output by default)
    def replace(*args)
      options = args.extract_options!

      pattern = options[:pattern] || args[1]
      replacement = options[:replacement] || args[2]
      into = options[:into] || "#{args[0]}_replaced"
      output = options[:output] || all_fields

      each args[0], :function => regex_replace(into, pattern, replacement), :output => output
    end

    # Builds a pipe that inserts values into the current tuple.
    #
    # The method takes a hash as parameter. This hash contains as keys the names of the fields to insert
    # and as values, the values they must contain. For example:
    #     
    #       insert {"who" => "Grégoire", "when" => Time.now.strftime("%Y-%m-%d") }
    #
    # will insert two new fields: a field _who_ containing the string "Grégoire", and a field _when_ containing 
    # the formatted current date.
    # The methods outputs all fields.
    # The named options are:
    def insert(args)
      args.keys.sort.each do |field_name|
        value = args[field_name]

        if value.kind_of?(ExprStub)
          each all_fields,
            :function => expression_function(field_name, :expression => value.expression,
                           :parameters => value.types), :output => all_fields
        else
          each all_fields, :function => insert_function([field_name], :values => [value]), :output => all_fields
        end
      end
    end

    # Builds a pipe that filters the tuples based on an expression or a pattern (but not both !).
    #
    # The first unamed argument, if provided, is a filtering expression (using the Janino syntax).
    #
    # The named options are:
    # * <tt>:pattern</tt> a string. Specifies a regular expression pattern used to filter the tuples. If this
    # option is provided, then the filter is regular expression-based. This is incompatible with the _expression_ option.
    # * <tt>:expression</tt> a string. Specifies a Janino expression used to filter the tuples. This option has the 
    # same effect than providing it as first unamed argument. If this option is provided, then the filter is Janino 
    # expression-based. This is incompatible with the _pattern_ option. 
    def filter(*args)
      options = args.extract_options!
      from = options.delete(:from) || all_fields
      expression = options.delete(:expression) || args.shift
      regex = options.delete(:pattern)
      if expression
        stub = ExprStub.new(expression)
        types, expression = stub.types, stub.expression

        each from, :filter => expression_filter(
          :parameters => types,
          :expression => expression
        )
      elsif regex
        each from, :filter => regex_filter(regex, options)
      end
    end

    # Builds a pipe that rejects the tuples based on an expression.
    #
    # The first unamed argument, if provided, is a filtering expression (using the Janino syntax).
    #
    # The named options are:
    # * <tt>:expression</tt> a string. Specifies a Janino expression used to filter the tuples. This option has the 
    # same effect than providing it as first unamed argument. If this option is provided, then the filter is Janino 
    # expression-based. 
    def reject(*args)
      options = args.extract_options
      raise "Regex not allowed" if options && options[:pattern]

      filter(*args)
    end

    # Builds a pipe that includes just the tuples matching an expression.
    #
    # The first unamed argument, if provided, is a filtering expression (using the Janino syntax).
    #
    # The named options are:
    # * <tt>:expression</tt> a string. Specifies a Janino expression used to select the tuples. This option has the 
    # same effect than providing it as first unamed argument. If this option is provided, then the filter is Janino 
    # expression-based. 
    def where(*args)
      options = args.extract_options
      raise "Regex not allowed" if options && options[:pattern]

      if options[:expression]
        options[:expression] = "!(#{options[:expression]})" 
      elsif args[0]
        args[0] = "!(#{args[0]})" 
      end

      filter(*args)
    end

    # Builds a pipe that evaluates the specified Janino expression and insert it in a new field in the tuple.
    #
    # The named options are:
    # * <tt>:from</tt> a string or array of strings. Specifies the input fields.
    # * <tt>:express</tt> a string. The janino expression.
    # * <tt>:into</tt> a string. Specified the name of the field to insert with the result of the evaluation.
    # * <tt>:parameters</tt> a hash. Specifies the type mapping for the parameters. See Cascading::Operations.expression_function.
    def eval_expression(*args)
      options = args.extract_options!

      into = options.delete(:into)
      from = options.delete(:from) || all_fields
      output = options.delete(:output) || all_fields
      options[:expression] ||= args.shift
      options[:parameters] ||= args.shift

      each from, :function => expression_function(into, options), :output=>output
    end
    
    # Builds a pipe that returns distinct tuples based on the provided fields.
    #
    # The method accepts optional unamed argument specifying the fields to base the distinct on
    # (all fields, by default).
    def distinct(*args)
      raise "Distinct is badly broken"
      fields = args[0] || all_fields
      group_by *fields
      pass
    end
      
    # Builds a pipe that will unify (merge) pipes. The method accepts the list of pipes as argument.
    # Tuples unified must share the same fields.   
    def union(*args)
      options = args.extract_options!
      pipes = args
      
      union_pipes pipes
    end
  end # module PipeHelpers
  
end # module Cascading
