require 'cascading'

module Cascading
  HBASE_HOME = ENV['HBASE_HOME']
  CASCADING_HBASE_HOME = ENV['CASCADING_HBASE_HOME']
  
  Cascading.require_all_jars(HBASE_HOME) if Cascading::HBASE_HOME
  Cascading.require_all_jars(CASCADING_HBASE_HOME) if Cascading::CASCADING_HBASE_HOME
  
  def hbase_scheme(keys, families, values)
    parameters = [fields(keys), [families].compact.to_java(java.lang.String), [fields(values)].to_java(Java::CascadingTuple.Fields)].compact 
    Java::CascadingHbase::HBaseScheme.new(*parameters)
  end
  
  def hbase_tap(*args)
    opts = args.extract_options!
    table_name = args[0]
    key = opts[:key]
    families = opts[:families]
    values = opts[:values]
    sink_mode = opts[:sink]
    scheme = hbase_scheme(key, families, values)
    parameters = [table_name, scheme, sink_mode].compact
    Java::CascadingHbase::HBaseTap.new(*parameters)
  end
end
