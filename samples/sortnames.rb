#! /usr/bin/env jruby
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

# Question: does this script actually sort the names?

require 'cascading'
require 'samples/cascading'

Cascading::Flow.new('fetch') do
  # You don't have to curl and cache inputs: tap can fetch via HTTP
  source 'fetch', tap('http://www.census.gov/genealogy/names/dist.all.last')

  assembly 'fetch' do
    split 'line', ['name', 'val1', 'val2', 'id']
    insert 'val3' => expr('val2:double < 40.0 ? val1:double : val2:double')
    project 'name', 'val3', 'id'
  end

  sink 'fetch', tap('output/sorted', :sink_mode => :replace)
end.complete(sample_properties)
