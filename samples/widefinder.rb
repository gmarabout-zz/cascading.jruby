require 'cascading'

dataUrl = 'http://files.cascading.org/apache.200.txt.gz'

String logs = 'output/logs/'
String output = 'output/results'

APACHE_COMMON_REGEX = /^([^ ]*) +[^ ]* +[^ ]* +\[([^]]*)\] +\"([^ ]*) ([^ ]*) [^ ]*\" ([^ ]*) ([^ ]*) \"([^ ]*)\".*$/
APACHE_COMMON_GROUPS = [1, 2, 3, 4, 5, 6]
APACHE_COMMON_FIELDS = ["ip", "time", "method", "url", "status", "size"]

URL_PATTERN = /^\/archives\/.*$/

cascade = Cascading::Builder.cascade("widefinder") do
  flow "fetcher" do
    source dataUrl
    assembly "copy" do
      pass 
    end
    sink tap(logs, :replace=>true)
  end
  
    flow "counter" do
      source logs
    
      assembly "count" do
        # parse apache log, given regex groups are matched with respective field names
        parse("line", :pattern => APACHE_COMMON_REGEX, :groups => APACHE_COMMON_GROUPS, :declared => APACHE_COMMON_FIELDS)
    
        debug :print_fields=>true
        # throw away tuples that don't match
        #filter("url", :pattern => URL_PATTERN)
        
        #    
        # throw away unused fields
        #keep_only "url"
        #    
        #        group_by "url"
        #    
        #        # creates 'count' field, by default
        #        count
        #    
        #        # group/sort on 'count', reverse the sort order
        #        group_by "count", :reverse => true
    
      end
    
    
      sink tap(output, :replace=>true)
    end

end


cascade.complete