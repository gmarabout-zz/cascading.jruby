#!/usr/bin/jruby

require "cascading"
input = 'samples/data/data2.txt'

flow = Cascading::Flow.new("copy") do
  source "copy", tap(input)

  sink "copy", tap('output/splitted', :replace=>true)
  
  assembly "copy" do

      split "line", :pattern => /[.,]*\s+/, :into=>["name", "score1", "score2", "id"], :output => ["name", "score1", "score2", "id"]

      group_by "score1"

      count

      project "score1", "count"
  end
end

flow.complete



