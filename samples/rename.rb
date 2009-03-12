require "cascading"

input = 'samples/data/data2.txt'

output = "output/rename"

flow = Cascading::Flow.new("rename") do
  source tap(input)
  sink tap(output, :replace=>true)
  
  assembly "extract" do

    split "line", :pattern => /[.,]*\s+/, :into=>["name", "score1", "score2", "id"], :output => ["name", "score1", "score2", "id"]

    assert Java::CascadingOperationAssertion::AssertSizeEquals.new(4)

    rename ["name"], ["coucou"]
   
    assert Java::CascadingOperationAssertion::AssertSizeEquals.new(4)
   
    debug :print_fields=>true
  end
end 

flow.complete