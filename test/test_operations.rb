#! /usr/bin/env jruby

require 'test/unit'
require 'cascading'

class TC_Operations < Test::Unit::TestCase

  include Cascading::Operations

  def test_each_regex_splitter
    # assembly = assembly "assembly1" do
    #      each "field1", :filter => regex_splitter(["field1", "field2", "field3"])
    #    end
    # 
    #    assert assembly.is_a? Java::CascadingPipe::Each
  end

  def test_each_regex_splitter_with_pattern
    # assembly = Cascading.assembly "assembly1" do
    #   each "field1", :filter=>regex_splitter(:fields => ["field1", "field2", "field3"], :pattern=>"**")
    # end
    # 
    # assert assembly.is_a? Java::CascadingPipe::Each
    # assert_not_nil Cascading::AssemblyFactory.get("assembly1")
    # assert_equal assembly, Cascading::AssemblyFactory.get("assembly1")
  end

end