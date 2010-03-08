require "cascading"

input1 = "samples/data/data_join1.txt"
input2 = "samples/data/data_join2.txt"
input3 = "samples/data/data_join3.txt"

output = "output/joined"

flow = Cascading::Flow.new("Join sample") do
  source "extract1", tap(input1)
  source "extract2", tap(input2)
  source "extract3", tap(input3)

  sink "join", tap(output, :replace=>true)

  assembly "extract1" do
    split "line", :pattern => /[.,]*\s+/, :into=> ["id", "name"], :output => ["id", "name"]
  end

  assembly "extract2" do
    split "line", :pattern => /[.,]*\s+/, :into=> ["id", "age"], :output => ["id", "age"]
  end

  assembly "extract3" do
    split "line", :pattern => /[.,]*\s+/, :into=> ["id", "city"], :output => ["id", "city"]
  end
  
  assembly "join" do
    join "extract1", "extract2", "extract3", :on => ["id"], :declared_fields => ["id", "name", "id2", "age", "id3", "city"]
    restrict_to "id", "name", "age", "city"
  end
end 

flow.complete