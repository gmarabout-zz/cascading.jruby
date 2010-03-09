# TODO: update and fix this sample

require "cascading"
require "cascading-ext/jdbc"

input = 'output/fetched/to_be_copied_to_db.txt'
dataUrl = "http://www.census.gov/genealogy/names/dist.all.last"
unless File.exist? input
  system "curl --create-dirs -o #{input} #{dataUrl}"
end

connection_props = {  
  :driver_class_name => "com.mysql.jdbc.Driver",
  :table_name => "user",
  :column_names => ["id", "name", "score1", "score2"],
  :primary_key => "id"
}

flow = Cascading::Flow.new("copy_to_mysql") do
  source "extract", tap(input)
  sink "extract", jdbc_tap('jdbc:mysql://localhost/test?user=root&password=', connection_props)
  #sink tap("output/fake-jdbc", :replace => true)

  assembly "extract" do
    split "line", :pattern => /[.,]*\s+/, :into=>["name", "score1", "score2", "id"]


  end
end 

flow.complete
