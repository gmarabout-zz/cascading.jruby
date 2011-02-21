require 'test/unit'
require 'cascading'

class TC_Cascading < Test::Unit::TestCase
  def test_fields_field
    result = Cascading.fields(Cascading.all_fields)
    assert result == Cascading.all_fields
  end

  def test_fields_single
    declared = "Field1"

    result = Cascading.fields( declared )

    assert result.size == 1

    assert_equal declared, result.get(0) 
  end

  def test_fields_multiple
    declared = ["Field1", "Field2", "Field3"]

    result = Cascading.fields( declared )

    assert result.size == 3

    assert_equal declared[0], result.get(0)
    assert_equal declared[1], result.get(1)
    assert_equal declared[2], result.get(2) 
  end

  def test_tap
    tap = tap('/temp')
    assert_equal '/temp', tap.getPath().toString()
    assert tap.is_a? Java::CascadingTap::Hfs  

    tap = tap('/temp', :kind => :dfs)
    assert_equal '/temp', tap.getPath().toString()
    assert tap.is_a? Java::CascadingTap::Dfs

    tap = tap('/temp', :kind => :lfs)
    assert_equal '/temp', tap.getPath().toString()
    assert tap.is_a? Java::CascadingTap::Lfs

    tap = tap('/temp', :kind => :hfs)
    assert_equal '/temp', tap.getPath().toString()
    assert tap.is_a? Java::CascadingTap::Hfs
  end
end
