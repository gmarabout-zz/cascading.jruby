#! /usr/bin/env jruby

# Look in the tasks/setup.rb file for the various options that can be
# configured in this Rakefile. The .rake files in the tasks directory
# are where the options are used.

begin
  require 'bones'
  Bones.setup
rescue LoadError
  begin
    load 'tasks/setup.rb'
  rescue LoadError
    raise RuntimeError, '### please install the "bones" gem ###'
  end
end

ensure_in_path 'lib'

require 'cascading'

task :default => 'test:run'

task :run_samples do
  ensure_in_path "samples"
  require "logwordcount"
  #require "copy"
  #require "jdbc"
  #require "hbase"
  #require "jsonsplitter"
end

task :run do 
  # ensure_in_path "samples"
  # require "samples/#{ARGS[0]}"
  puts ARGS[0]
end


PROJ.name = 'cascading.jruby'
PROJ.authors = 'Grégoire Marabout'
PROJ.email = 'gmarabout@gmail.com'
PROJ.url = ''
PROJ.version = Cascading::VERSION
PROJ.rubyforge.name = 'cascading.jruby'

PROJ.spec.opts << '--color'

# EOF
