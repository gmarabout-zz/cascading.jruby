require 'test/unit'
require 'cascading'
require 'cascading-ext/hbase'

class TC_Cascading < Test::Unit::TestCase
  def test_hbase_tap
    tap = Cascading.hbase_tap("my_table", :key=>["key"], :families=>["data", "metaData"], :values=>["val1", "val2"])
    assert tap.get_scheme().is_a? Java::CascadingHbase::HBaseScheme
    assert tap.is_a? Java::CascadingHbase::HBaseTap
  end 
end