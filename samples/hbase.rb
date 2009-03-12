require "cascading"
require 'digest/md5'

input = 'output/fetched/fetch.txt'
output = 'output/splitted'


Assembly.new "copy" do  
  each "line", :function => identity
  
end

# In HBase we have a 'test' table with a single family 'data' containing a column 'data:value'.

hbase_tap = hbase_tap('test', :key=>'key', :families=>"data", :values=>["value"])

begin
  flow("extract", :source => hfs_tap(input), :sink => hfs_tap(output), :assembly => "copy").complete
rescue NativeException => e
  e.cause.printStackTrace()

end