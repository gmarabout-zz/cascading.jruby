require "cascading"

input = 'output/fetched/to_be_branched.txt'
dataUrl = "http://www.census.gov/genealogy/names/dist.all.last"
unless File.exist? input
  system "curl --create-dirs -o #{input} #{dataUrl}"
end

output = "output/merged"
output1 = "output/merged1"
output2 = "output/merged2"

flow = Cascading::Flow.new("copy_to_mysql") do
  source  "extract", tap(input)
  sink tap(output)
  #sink  "branch1", tap(output1, :replace=>true)
  #sink  "branch2", tap(output2, :replace=>true)
  
  assembly "extract" do
    
    split "line", :pattern => /[.,]*\s+/, :into=>["name", "score1", "score2", "id"], :output => ["name", "score1", "score2", "id"]
  
    b1 = branch "branch1" do
      group_by "score1","name"
      count      
      rename ["score1"], ["score"]
    end
    
     b2 = branch "branch2" do
      group_by "score2","name"
      count      
      rename ["score2"],  ["score"]      
    end
    
    
    merge b1, b2
    
  end
end 

flow.complete
