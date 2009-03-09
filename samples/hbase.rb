require "cascading"
require 'digest/md5'

input = 'output/fetched/fetch.txt'
output = 'output/splitted'

# 
# class MyFilter < Java::OrgCascadingOperation::BaseFunction
#     
#   def operate(flow_process, function_call)
#       
#     value = functionCall.getArguments().getString( 0 )
#     
#     # key = Digest::MD5.hexdigest(value)
#     key = value
#     output = Java::CascadingTuple::Tuple.new
#     
#     output.add(key)
#     output.add(value)
#     
#     functionCall.getOutputCollector().add(output)
#   end  
#   
#   def getNumArgs
#     1
#   end
# end


# my_function = Java::OrgCascadingOperationJruby::BaseFunction.new(fields(["key", "value"])) do |fp, fc|
#   value = fc.getArguments().getString( 0 )
#   key = Digest::MD5.hexdigest(value)
#   output = Java::CascadingTuple::Tuple.new
#   output.add(key)    
#   output.add(value)
#   fc.getOutputCollector().add(output)
# end

assembly "copy" do  
  each "line", :function => identity
 
  
end

# In HBase we have a 'test' table with a single family 'data' containing a column 'data:value'.

hbase_tap = hbase_tap('test', :key=>'key', :families=>"data", :values=>["value"])

begin
  flow("extract", :source => hfs_tap(input), :sink => hfs_tap(output), :assembly => "copy").complete
rescue NativeException => e
  e.cause.printStackTrace()

end