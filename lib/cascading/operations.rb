module Cascading
  module Operations

    def identity
      Java::CascadingOperation::Identity.new
    end

    def count_function(*args)
      options = args.extract_options!
      if args.empty?
        # By default, output field is "count"
        args = ["count"]
      end
      parameters = [Cascading.fields(args)].compact
      Java::CascadingOperationAggregator::Count.new(*parameters)
    end

    def sum_function(*args)
      options = args.extract_options!
      if args.empty?
        # By default, output field is "sum"
        args = ["sum"]
      end
      parameters = [Cascading.fields(args)].compact
      Java::CascadingOperationAggregator::Sum.new(*parameters)
    end

    def average_function(*args)
      options = args.extract_options!
      if args.empty?
        # By default, output field is "average"
        args = ["average"]
      end
      parameters = [Cascading.fields(args)].compact
      Java::CascadingOperationAggregator::Average.new(*parameters)
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

    def expression_filter(options)
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

        arguments = [expression, parameter_names, parameter_types].compact
      elsif !parameters.nil?
        arguments = [expression, parameters.java_class].compact
      else
        arguments = [expression, java.lang.String.java_class].compact
      end

      Java::CascadingOperationExpression::ExpressionFilter.new(*arguments)
    end

    def json_generator(names)
      fields = []
      paths = []
      names.each do |k,v|
        fields << k
        paths << v
      end
      fields = Cascading.fields(fields)
    
      parameters = [fields, paths.to_java(java.lang.String)].compact
      Java::OrgCascadingJson::JSONGenerator.new(*parameters)
    end
    
    def date_formatter(fields, format)
      fields = fields(fields)
      Java::CascadingOperationText::DateFormatter.new(fields, format)      
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