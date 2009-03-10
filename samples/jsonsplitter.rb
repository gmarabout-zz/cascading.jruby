#!/usr/bin/jruby

require "cascading"
input = 'samples/data/data.json'

Cascading::Builder.assembly "copy" do

    # Split "line" using a JSONSplitter
    each "line", :filter => Java::OrgCascadingOperation::JSONSplitter.new(fields(["name", "age", "address"])), :output => ["name", "age", "address"]
    debug :print_fields=>true
end

flow = Cascading::Builder.flow("copy") do
  source "copy", tap(input)

  sink "copy", tap('output/splitted', :replace=>true)
  
  assembly "copy"
end

flow.execute



