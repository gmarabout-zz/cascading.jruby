#! /usr/bin/env jruby
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'cascading'
require 'samples/cascading'

input = 'output/fetched/to_be_copied.txt'
dataUrl = 'http://www.census.gov/genealogy/names/dist.all.last'
system "curl --create-dirs -o #{input} #{dataUrl}" unless File.exists?(input)

Cascading::Flow.new('copy') do
  source 'copy', tap(input)
  assembly 'copy' do
    rename 'line' => 'value'
    reject 'value:string.indexOf("R") == -1'
  end
  sink 'copy', tap('output/copied', :sink_mode => :replace)
end.complete(sample_properties)
