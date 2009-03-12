module Cascading

  CASCADING_JDBC_HOME = ENV["CASCADING_JDBC_HOME"]
  Cascading.require_all_jars(CASCADING_JDBC_HOME)

  def jdbc_scheme(columns, order_by=nil)
    parameters = [columns.to_java(java.lang.String), order_by].compact
    Java::CascadingJdbc::JDBCScheme.new(*parameters)
  end


  def jdbc_tap(*args)
    opts = args.extract_options!

    connection_url =  args[0]
    driver_class_name = opts[:driver_class_name]
    column_names = opts[:column_names]
    table_name = opts[:table_name]
    primary_key = opts[:primary_key]
    column_defs = opts[:column_defs]
    sink_mode = opts[:sink]

    parameters = [table_name, [column_names].flatten.to_java(java.lang.String)].compact

    table_desc = Java::CascadingJdbc::TableDesc.new(*parameters)
    order_by = opts[:order_by]

    jdbc_scheme =  jdbc_scheme(column_names, order_by)
    parameters = [connection_url, driver_class_name, table_desc, jdbc_scheme, sink_mode].compact
    Java::CascadingJdbc::JDBCTap.new(*parameters)
  end

end
