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

task :run do
  # ensure_in_path "samples"
  puts "Running #{ARGS[0]}"
  require "samples/#{ARGS[0]}"
end

desc 'Remove gem and Java build files'
task :clean => ['ant:clean', 'gem:clean'] do
  puts 'Build files removed'
end

PROJ.name = 'cascading.jruby'
PROJ.authors = ['Matt Walker', 'Grégoire Marabout']
PROJ.email = 'mwalker@etsy.com'
PROJ.url = 'http://github.com/etsy/cascading.jruby'
PROJ.version = Cascading::VERSION
PROJ.summary = 'A JRuby DSL for Cascading'
PROJ.description = 'cascading.jruby is a small DSL above Cascading, written in JRuby'
PROJ.rubyforge.name = 'cascading.jruby'
PROJ.spec.opts << '--color'

# EOF
