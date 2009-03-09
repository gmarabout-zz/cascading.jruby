require "cascading"

input = 'output/fetched/to_be_copied.txt'
dataUrl = "http://www.census.gov/genealogy/names/dist.all.last"
unless File.exist? input
  system "curl --create-dirs -o #{input} #{dataUrl}"
end

connection_props = {  
  :driver_class_name => "com.mysql.jdbc.Driver",
  :table_name => "test",
  :column_names => ["id", "values"],
}


assembly "copy" do
  each "line", :filter => identity
end


flow("copy_to_mysql", 
  :source => tap(input), 
  :sink => jdbc_tap('jdbc:mysql://localhost/test?user=root&password=', connection_props), 
  :assembly => "copy").complete