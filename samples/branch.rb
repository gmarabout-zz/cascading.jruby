require "cascading"

input = 'samples/data/data2.txt'

output1 = "output/branch1"
output2 = "output/branch2"
output3 = "output/branch3"

flow = Cascading::Flow.new("copy_to_mysql") do
  source "extract", tap(input)
  sink "branch1", tap(output1, :replace=>true)
  sink "branch2", tap(output2, :replace=>true)

  assembly "extract" do

    split "line", :pattern => /[.,]*\s+/, :into=>["name", "score1", "score2", "id"], :output => ["name", "score1", "score2", "id"]


    branch "branch1" do
      group_by "score1"
      count
      project "score1","count"
      #insert "value" => "Hello", :output => ["name", "score1", "value"]
    end

    branch "branch2" do
      group_by "score2"
      count
      project "score2","count"
      # insert "value" => "World",  :output => ["name", "score2", "value"]
    end

  end
end 

flow.complete
