#!/usr/bin/jruby

require "cascading"
input = 'samples/data/data.json'

assembly "copy" do

    # Split "line" using a JSONSplitter
    each "line", :filter => Java::OrgCascadingOperation::JSONSplitter.new(fields(["name", "age", "address"])), :output => ["name", "age", "address"]

end

flow = flow("copy") do
  source "copy", tap(input)

  sink "copy", tap('output/splitted', :replace=>true)
  
  assembly "copy"
end

flow.execute



