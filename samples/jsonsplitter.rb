#!/usr/bin/jruby

require "cascading"
input = 'samples/data/data.json'

Cascading::Assembly.new "copy" do

    # Split "line" using a JSONSplitter
    each "line", :filter => Java::OrgCascadingOperation::JSONSplitter.new(fields(["name", "age", "address"])), :output => ["name", "age", "address"]
    debug :print_fields=>true
end

flow = Cascading::Flow.new("copy") do
  source "copy", tap(input)

  sink "copy", tap('output/json_splitted', :replace=>true)
  
  assembly "copy"
end

flow.complete



