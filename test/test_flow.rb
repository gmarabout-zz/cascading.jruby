require 'test/unit'
require 'cascading'

class TC_Flow < Test::Unit::TestCase

  def test_assembly
    flow = Cascading::Flow.new("My Flow1") do 
      assembly "Test1" do
      end
    end
    
    assert_equal 1, flow.children.size
    assert_equal flow.children[0], Cascading::Assembly.get("Test1")
    
  end
  
end
