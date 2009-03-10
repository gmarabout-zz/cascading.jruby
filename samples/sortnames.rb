#! /usr/bin/jruby      

require 'cascading'

source1 = tap("http://www.census.gov/genealogy/names/dist.all.last", :scheme => text_line_scheme)
sink1 = tap('output/sorted', :scheme => text_line_scheme, :replace=>true)

expr = "val2 < 40 ? val1 : val2"

flow1 = Cascading::Builder.flow("fetch") do
  source "fetch", source1

  sink "fetch", sink1

  assembly "fetch" do
    each "line", :filter=>regex_splitter(["name", "val1", "val2", "id"], :pattern => /[.,]*\s+/), :output=>["id", "name", "val1", "val2"] 

    each ["val1", "val2"], 
      :function => expression_function("val3", :expression => expr, :parameters => {"val1"=>java.lang.Double, "val2"=>java.lang.Double}), 
      :output => ["name", "val3", "id"]      
  end
end

flow1.complete

