module Cascading
  module PipeHelpers
    def cut
      raise 'Not implemented yet'
    end 

    def tokenize
      raise 'Not implemented yet'
    end

    def debug(*args)
      options = args.extract_options!
      print_fields = options[:print_fields] || true
      parameters = [print_fields].compact
      each(all_fields, :filter => Java::CascadingOperation::Debug.new(*parameters))
    end

    def count(*args)
      options = args.extract_options!
      fields = args[0] || last_grouping_fields
      into = options[:into]      
      output = options[:output] || all_fields
      every(fields, :aggregator=>count_function(into), :output => output)
    end
    
    def average(*args)
      options = args.extract_options!
      fields = args[0] || last_grouping_fields
      into = options[:into] 
      output = options[:output] || all_fields
      every(fields, :aggregator=>average_function(into), :output => output)
    end
    
    def parse(*args)
        options = args.extract_options!
        fields = args || all_fields
        pattern = options[:pattern]
        output = options[:output] || all_fields
        each(fields, :filter => regex_parser(pattern, options), :output => output)
    end

    def split(*args)
      options = args.extract_options!
      fields = options[:into] || args[1]
      pattern = options[:pattern] || /[.,]*\s+/
      output = options[:output] || all_fields
      each(args[0], :filter => regex_splitter(fields, :pattern => pattern), :output=>output)
    end


    def format_date(*args)
      options = args.extract_options!
      field = options[:into] || "#{args[0]}_formatted"
      output = options[:output] || all_fields
      pattern = options[:pattern] || "yyyy/MM/dd"

      each args, :function => date_formatter(field, pattern), :output => output
    end

    def replace(*args)
      options = args.extract_options!

      pattern = options[:pattern] || args[1]
      replacement = options[:replacement] || args[2]
      into = options[:into] || "#{args[0]}_replaced"
      output = options[:output] || all_fields

      each args[0], :function => regex_replace(into, pattern, replacement), :output => output
    end

    def insert(*args)
      options = args.extract_options!
      keys = []
      values = []
      options.each do |k, v|
        keys << k
        values << v
      end
      each all_fields, :function => insert_function(keys, :values => values), :output => all_fields
    end

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

    def eval_expression(*args)
      options = args.extract_options!

      into = options.delete(:into)
      from = options.delete(:from)|| all_fields
      output = options.delete(:output) || all_fields
      options[:expression] = args[0]

      each from, :function => expression_function(into, options), :output=>output
    end
    
    def distinct(*fields)
      group_by(fields || all_fields)
      every all_fields, :aggregator=>Java::CascadingOperationAggregator::First.new, :output=>results_fields
    end
      
      
    def merge(*args)
      options = args.extract_options!
      pipes = args
      
      merge_pipes pipes
    end
  end # module PipeHelpers
  
end # module Cascading