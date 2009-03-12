module Cascading

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
    end
    return Java::CascadingTuple::Fields.new([fields].flatten.to_java(java.lang.Comparable))
  end

  def all_fields
    Java::CascadingTuple::Fields::ALL
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
      return Java::CascadingScheme::TextLine.new()
    end
  end

  # Creates a c.s.SequenceFile scheme instance from the specified fields.
  def sequence_file_scheme(*fields)
    unless fields.empty?
      fields = fields(fields) 
      return Java::CascadingScheme::SequenceFile.new(fields)
    else
      return Java::CascadingScheme::SequenceFile.new()
    end
  end


  # Creates a c.t.Hfs tap instance.
  def hfs_tap(*args)
    opts = args.extract_options!
    path = args[0] || opts[:path]
    scheme = opts[:scheme] || text_line_scheme("line")
    replace = opts[:replace]
    parameters = [scheme, path, replace].compact
    Java::CascadingTap::Hfs.new(*parameters)
  end

  # Creates a c.t.Dfs tap instance.
  def dfs_tap(*args)
    opts = args.extract_options!
    path = args.empty? ? opts[:path] : args[0]
    scheme = opts[:scheme] || text_line_scheme("line")
    replace = opts[:replace]
    parameters = [scheme, path, replace].compact
    Java::CascadingTap::Dfs.new(*parameters)
  end

  # Creates a c.t.Lfs tap instance.
  def lfs_tap(*args)
    opts = args.extract_options!
    path = args.empty? ? opts[:path] : args[0]
    scheme = opts[:scheme] || text_line_scheme("line")
    replace = opts[:replace]
    parameters = [scheme, path, replace].compact
    Java::CascadingTap::Lfs.new(*parameters)
  end

  # Generic method for creating taps.
  # It expects a ":kind" argument pointing to the type of tap to create. 
  def tap(*args)
    opts = args.extract_options
    fs = opts[:kind] || "hfs"   
    send("#{fs}_tap", *args) 
  end

end
