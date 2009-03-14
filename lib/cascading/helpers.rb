module Cascading
  
  # Module PipeHelpers.
  # This module is mixed-in the class Cascading::Assembly to provide shorthands of current pipe configurations.
  # 
  # Author:: Grégoire Marabout <gmarabout@gmail.com>
  module PipeHelpers
    
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
      each(all_fields, :filter => Java::CascadingOperation::Debug.new(*parameters))
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

    # Builds a pipe that computes a count.
    #
    # If provided, the unamed arguments must be the fields to be used for count. If not provided, 
    # the latest grouped fields are used.
    # 
    # The other named options are:
    # * <tt>:into</tt> a string or array of strings. Specifies the field name receiving the result value ("count" fill be used by default).
    # * <tt>:output</tt> a string or array of strings. Specifies the outgoing fields (all fields will be output by default)
    def count(*args)
      options = args.extract_options!
      fields = *args || last_grouping_fields
      into = options[:into] || "count"     
      output = options[:output] || all_fields
      every(fields, :aggregator=>count_function(into), :output => output)
    end
    
    # Builds a pipe that computes an average.
    # 
    # If provided, the unamed arguments must be the fields to be used for the average. If not provided, 
    # the latest grouped fields are used.
    #
    # The other named options are:
    # * <tt>:into</tt> a string or array of strings. Specifies the field name receiving the result value ("count" fill be used by default).
    # * <tt>:output</tt> a string or array of strings. Specifies the outgoing fields (all fields will be output by default)
    def average(*args)
      options = args.extract_options!
      fields = args[0] || last_grouping_fields
      into = options[:into] 
      output = options[:output] || all_fields
      every(fields, :aggregator=>average_function(into), :output => output)
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
    # * <tt>:patter</tt> a string. Specifies the date format.
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
    # * <tt>:patter</tt> a string. Specifies the date format.
    # * <tt>:output</tt> a string or array of strings. Specifies the outgoing fields (all fields will be output by default)
    def format_date(*args)
      options = args.extract_options!
      field = options[:into] || "#{args[0]}_formatted"
      output = options[:output] || all_fields
      pattern = options[:pattern] || "yyyy/MM/dd"

      each args[0], :function => date_formatter(field, pattern), :output => output
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
      keys = []
      values = []
      args.each do |k, v|
        keys << k
        values << v
      end
      
      each all_fields, :function => insert_function(keys, :values => values), :output => all_fields
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
      expression = options.delete(:expression) || args[0]
      regex = options.delete(:pattern)
      if expression
        each from, :filter => expression_filter(:expression => expression)
      elsif regex
        each from, :filter => regex_filter(regex, options)
      end
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
      from = options.delete(:from)|| all_fields
      output = options.delete(:output) || all_fields
      options[:expression] = args[0]

      each from, :function => expression_function(into, options), :output=>output
    end
    
    # Builds a pipe that returns distinct tuples based on the provided fields.
    #
    # The method accepts optional unamed argument specifying the fields to base the distinct on
    # (all fields, by default).
    def distinct(*fields)
      #group_by fields || all_fields
      every fields, :aggregator=>Java::CascadingOperationAggregator::First.new, :output=>results_fields
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