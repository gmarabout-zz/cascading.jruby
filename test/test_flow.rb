require 'test/unit'
require 'cascading'

class TC_Flow < Test::Unit::TestCase
  def test_assembly
    flow = flow 'My Flow1' do
      assembly "Test1" do
      end
    end

    assert_equal 1, flow.children.size
    assert_equal flow.children["Test1"], flow.find_child("Test1")
    assert_equal flow.last_child, flow.find_child("Test1")
  end
end
