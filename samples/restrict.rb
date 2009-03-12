require "cascading"

input = 'samples/data/data2.txt'

output = "output/restrict"

flow = Cascading::Flow.new("copy_to_mysql") do
  source tap(input)
  sink tap(output, :replace=>true)
  
  assembly "extract" do

    split "line", :pattern => /[.,]*\s+/, :into=>["name", "score1", "score2", "id"], :output => ["name", "score1", "score2", "id"]

    assert Java::CascadingOperationAssertion::AssertSizeEquals.new(4)

    #debug :print_fields=>true

    restrict_to "name", "score1", "score2"
   
    assert Java::CascadingOperationAssertion::AssertSizeEquals.new(3)
   
    #debug :print_fields=>true
   
    restrict_to "name", "score2"
 
    assert Java::CascadingOperationAssertion::AssertSizeEquals.new(2)
 
    #debug :print_fields=>true
  end
end 

flow.complete
