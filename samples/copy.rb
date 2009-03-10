#!/usr/bin/jruby

require "cascading"
input = 'output/fetched/to_be_copied.txt'
dataUrl = "http://www.census.gov/genealogy/names/dist.all.last"
unless File.exist? input
  system "curl --create-dirs -o #{input} #{dataUrl}"
end

Cascading::Builder.assembly "copy" do
  # Let's rename the column name
  #rename "line", "value"
  
  # Keep only value containing an "R"
  #each "value", :filter => Java::CascadingOperationExpression::ExpressionFilter.new ("value.indexOf(\"R\")==-1", Java::JavaLang::String.java_class)
  
end

flow = Cascading::Builder.flow("copy") do
  source tap(input)
  sink tap('output/copied', :replace=>true)
  assembly "copy"
end

flow.complete
