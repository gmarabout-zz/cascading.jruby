#!/usr/bin/jruby

require "cascading"
input = 'output/fetched/fetch.txt'
dataUrl = 'http://www.gutenberg.org/files/20417/20417-8.txt'
unless File.exist? input
  system "curl --create-dirs -o #{input} #{dataUrl}"
end



flow = Cascading::Builder.flow("logwordcount") do
  
    source "logwordcount", input
  
    assembly ("logwordcount") do
      each "line", :output =>"word", :filter => regex_split_generator("word", :pattern => /[.,]*\s+/)
      
      group_by "word"

      every "word", :aggregator => count_function, :output => %w{ count word }
    
      group_by "count", :reverse => true
    end
    
    sink "logwordcount", tap('output/imported', :scheme => text_line_scheme, :replace=>true)

end


flow.execute
